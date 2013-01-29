/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.messaging.handling;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.NotificationContext.MapKey;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.messaging.handling.MessageHandler.ErrorCode;
import com.linkedin.helix.messaging.handling.MessageHandler.ErrorType;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.Attributes;
import com.linkedin.helix.model.Message.MessageType;
import com.linkedin.helix.monitoring.StateTransitionContext;
import com.linkedin.helix.monitoring.StateTransitionDataPoint;
import com.linkedin.helix.util.StatusUpdateUtil;

public class HelixTask implements MessageTask
{
  private static Logger             LOG     = Logger.getLogger(HelixTask.class);
  private final Message             _message;
  private final MessageHandler      _handler;
  private final NotificationContext _notificationContext;
  private final HelixManager        _manager;
  final StatusUpdateUtil                  _statusUpdateUtil;
  final HelixTaskExecutor                 _executor;
  volatile boolean                  _isTimeout = false;
//  final MessageTimeoutTask _timeoutTask;
  

  public HelixTask(Message message,
                   NotificationContext notificationContext,
                   MessageHandler handler,
                   HelixTaskExecutor executor)
  {
    _notificationContext = notificationContext;
    _message = message;
    _handler = handler;
    _manager = notificationContext.getManager();
    _statusUpdateUtil = new StatusUpdateUtil();
    _executor = executor;
    
  }

  @Override
  public HelixTaskResult call()
  {
    HelixTaskResult taskResult = null;
    
    // internal means application error, framework means helix error
    ErrorType type = null;
    ErrorCode code = null;

    long start = System.currentTimeMillis();
    LOG.info("Handling task:" + getTaskId() + " begins");
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    _statusUpdateUtil.logInfo(_message,
                              HelixTask.class,
                              "Message handling task begin execute",
                              accessor);
    _message.setExecuteStartTimeStamp(new Date().getTime());

    // add a concurrent map to hold currentStateUpdates for sub-messages of a batch-message
    // partitionName -> csUpdate
    if (_message.getBatchMessageMode() == true) {
  	  _notificationContext.add(MapKey.CURRENT_STATE_UPDATE.toString(), 
  			  new ConcurrentHashMap<String, CurrentStateUpdate>());
    }

    try
    {
      
      taskResult = _handler.handleMessage();
    }
    catch (InterruptedException e)
    {
      taskResult = new HelixTaskResult();
      taskResult.setException(e);
      taskResult.setInterrupted(true);
      
      _statusUpdateUtil.logError(_message,
                                 HelixTask.class,
                                 e,
                                 "State transition interrupted, timeout:" + _isTimeout,
                                 accessor);
      LOG.info("Message " + _message.getMsgId() + " is interrupted");
    }
    catch (Exception e)
    {
      taskResult = new HelixTaskResult();
      taskResult.setException(e);
      taskResult.setMessage(e.getMessage());

      String errorMessage =
          "Exception while executing a message. " + e + " msgId: " + _message.getMsgId()
              + " type: " + _message.getMsgType();
      LOG.error(errorMessage, e);
      _statusUpdateUtil.logError(_message, HelixTask.class, e, errorMessage, accessor);
    }

    // cancel timeout task
    _executor.cancelTimeoutTask(this);

    Exception exception = null;
    try
    {
        if (taskResult.isSucess())
        {
          _statusUpdateUtil.logInfo(_message,
                                    _handler.getClass(),
                                    "Message handling task completed successfully",
                                    accessor);
          LOG.info("Message " + _message.getMsgId() + " completed.");
        }
        else {
              type = ErrorType.INTERNAL;

            // TODO: interrupt can also be thrown by sth other than cancel/timeout?
        	if (taskResult.isInterrupted()) 
            {
              LOG.info("Message " + _message.getMsgId() + " is interrupted");
              code = _isTimeout ? ErrorCode.TIMEOUT : ErrorCode.CANCEL;
              if (_isTimeout)
              {
                int retryCount = _message.getRetryCount();
                LOG.info("Message timeout, retry count: " + retryCount + " MSGID:"
                    + _message.getMsgId());
                _statusUpdateUtil.logInfo(_message,
                                          _handler.getClass(),
                                          "Message handling task timeout, retryCount:"
                                              + retryCount,
                                          accessor);
                // Notify the handler that timeout happens, and the number of retries left
                // In case timeout happens (time out and also interrupted)
                // we should retry the execution of the message by re-schedule it in
                if (retryCount > 0)
                {
                  _message.setRetryCount(retryCount - 1);
                  
                  HelixTask task = new HelixTask(_message, _notificationContext, _handler, _executor);
                  // _executor.scheduleTask(_message, _handler, _notificationContext);
                  _executor.scheduleTask(task);
                  return taskResult;
                }
              }
            }
            else
            // logging for errors
            {
              code = ErrorCode.ERROR;

              String errorMsg =
                  "Message execution failed. msgId: " + _message.getMsgId()
                      + taskResult.getMessage();
              _statusUpdateUtil.logError(_message, _handler.getClass(), errorMsg, accessor);
            }
        }
        
        // Post-processing for the finished task
      	// remove msg
    	if (_message.getAttribute(Attributes.PARENT_MSG_ID) == null) {
            removeMessageFromZk(accessor, _message);
            reportMessageStat(_manager, _message, taskResult);
            sendReply(accessor, _message, taskResult);
            _executor.finishTask(this);
    	}
    }
    catch (Exception e)
    {
        exception = e;
        type = ErrorType.FRAMEWORK;
        code = ErrorCode.ERROR;

      String errorMessage =
          "Exception after executing a message, msgId: " + _message.getMsgId() + e;
      LOG.error(errorMessage, e);
      _statusUpdateUtil.logError(_message, HelixTask.class, errorMessage, accessor);
    }
    finally
    {
      long end = System.currentTimeMillis();
      LOG.info("msg:" + _message.getMsgId() + " handling task completed, results:"
          + taskResult.isSucess() + ", at: " + end + ", took:" + (end - start));

      // Notify the handler about any error happened in the handling procedure, so that
      // the handler have chance to finally cleanup
      if (type == ErrorType.INTERNAL)
      {
        _handler.onError(taskResult.getException(), code, type);
      } else if (type == ErrorType.FRAMEWORK) {
    	  _handler.onError(exception, code, type);
      }
    }
    return taskResult;
  }

  private void removeMessageFromZk(HelixDataAccessor accessor, Message message)
  {
    Builder keyBuilder = accessor.keyBuilder();
    if (message.getTgtName().equalsIgnoreCase("controller"))
    {
      // TODO: removeProperty returns boolean
      accessor.removeProperty(keyBuilder.controllerMessage(message.getMsgId()));
    }
    else
    {
      accessor.removeProperty(keyBuilder.message(_manager.getInstanceName(),
                                                 message.getMsgId()));
    }
  }

  private void sendReply(HelixDataAccessor accessor,
                         Message message,
                         HelixTaskResult taskResult)
  {
    if (_message.getCorrelationId() != null
        && !message.getMsgType().equals(MessageType.TASK_REPLY.toString()))
    {
      LOG.info("Sending reply for message " + message.getCorrelationId());
      _statusUpdateUtil.logInfo(message, HelixTask.class, "Sending reply", accessor);

      taskResult.getTaskResultMap().put("SUCCESS", "" + taskResult.isSucess());
      taskResult.getTaskResultMap().put("INTERRUPTED", "" + taskResult.isInterrupted());
      if (!taskResult.isSucess())
      {
        taskResult.getTaskResultMap().put("ERRORINFO", taskResult.getMessage());
      }
      Message replyMessage =
          Message.createReplyMessage(_message,
                                     _manager.getInstanceName(),
                                     taskResult.getTaskResultMap());
      replyMessage.setSrcInstanceType(_manager.getInstanceType());

      if (message.getSrcInstanceType() == InstanceType.PARTICIPANT)
      {
        Builder keyBuilder = accessor.keyBuilder();
        accessor.setProperty(keyBuilder.message(message.getMsgSrc(),
                                                replyMessage.getMsgId()),
                             replyMessage);
      }
      else if (message.getSrcInstanceType() == InstanceType.CONTROLLER)
      {
        Builder keyBuilder = accessor.keyBuilder();
        accessor.setProperty(keyBuilder.controllerMessage(replyMessage.getMsgId()),
                             replyMessage);
      }
      _statusUpdateUtil.logInfo(message, HelixTask.class, "1 msg replied to "
          + replyMessage.getTgtName(), accessor);
    }
  }

  private void reportMessageStat(HelixManager manager,
                                 Message message,
                                 HelixTaskResult taskResult)
  {
    // report stat
    if (!message.getMsgType().equals(MessageType.STATE_TRANSITION.toString()))
    {
      return;
    }
    long now = new Date().getTime();
    long msgReadTime = message.getReadTimeStamp();
    long msgExecutionStartTime = message.getExecuteStartTimeStamp();
    if (msgReadTime != 0 && msgExecutionStartTime != 0)
    {
      long totalDelay = now - msgReadTime;
      long executionDelay = now - msgExecutionStartTime;
      if (totalDelay > 0 && executionDelay > 0)
      {
        String fromState = message.getFromState();
        String toState = message.getToState();
        String transition = fromState + "--" + toState;

        StateTransitionContext cxt =
            new StateTransitionContext(manager.getClusterName(),
                                       manager.getInstanceName(),
                                       message.getResourceName(),
                                       transition);

        StateTransitionDataPoint data =
            new StateTransitionDataPoint(totalDelay,
                                         executionDelay,
                                         taskResult.isSucess());
        _executor.getParticipantMonitor().reportTransitionStat(cxt, data);
      }
    }
    else
    {
      LOG.warn("message read time and start execution time not recorded.");
    }
  }
  
  @Override
  public String getTaskId()
  {
	  return _message.getId();
  }
  
	@Override
	public Message getMessage() {
		return _message;
	}

	@Override
	public 	NotificationContext getNotificationContext()
	{
		return _notificationContext;
	}
	
	@Override
	public void onTimeout() {
		_isTimeout = true;
		_handler.onTimeout();
	}
};
