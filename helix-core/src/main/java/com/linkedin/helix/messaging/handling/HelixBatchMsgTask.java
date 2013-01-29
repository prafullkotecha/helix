package com.linkedin.helix.messaging.handling;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.messaging.handling.MessageHandler.ErrorCode;
import com.linkedin.helix.messaging.handling.MessageHandler.ErrorType;
import com.linkedin.helix.model.Message;

public class HelixBatchMsgTask implements MessageTask {
	private static Logger LOG = Logger.getLogger(HelixBatchMsgTask.class);

	final NotificationContext _context;
	final Message _batchMsg;
	final List<Message> _msgs;
	final List<MessageHandler> _handlers;

	public HelixBatchMsgTask(Message batchMsg, List<Message> msgs, List<MessageHandler> handlers,
	        NotificationContext context) {
		_batchMsg = batchMsg;
		_context = context;
		_msgs = msgs;
		_handlers = handlers;
	}

	@Override
	public HelixTaskResult call() throws Exception {
	    HelixTaskResult taskResult = new HelixTaskResult();
	    
	    Exception exception = null;
	    ErrorType type = ErrorType.INTERNAL;
	    ErrorCode code = ErrorCode.ERROR;

	    long start = System.currentTimeMillis();
	    LOG.info("taskId:" + getTaskId() + " handling task begin, at: " + start);

	    try
	    {

    		for (MessageHandler handler : _handlers) {
    			if (handler != null) {
    				taskResult = handler.handleMessage();
    				// if any fails, skip the remaining handlers and return fail
    				if (!taskResult.isSucess()) {
    					// return result;
    					break;
    				}
    			}
    		}
	    }
	    catch (InterruptedException e)
	    {
	      LOG.info("taskId: " + getTaskId() + " is interrupted");
	      taskResult.setInterrupted(true);
	      taskResult.setException(e);
	      exception = e;
	    }
	    catch (Exception e)
	    {
	      String errorMessage =
	          "Exception while executing a task. " + e + " taskId: " + getTaskId();
	      LOG.error(errorMessage, e);
	      taskResult.setSuccess(false);
	      taskResult.setException(e);
	      taskResult.setMessage(e.getMessage());
	      exception = e;
	    }

	    if (taskResult.isSucess())
	    {
	      LOG.info("task: " + getTaskId() + " completed.");
	    }
	    else if (taskResult.isInterrupted())
	    {
	      LOG.info("task: " + getTaskId() + " is interrupted");
	    }
	    
        LOG.info("task:" + getTaskId() + " handling task completed, results:" + taskResult.isSucess());

        taskResult.setErrcode(code);
        taskResult.setErrType(type);
		return taskResult;
	}

	@Override
	public String getTaskId() {
		StringBuilder sb = new StringBuilder();
		sb.append(_batchMsg.getId());
		sb.append("/");
		List<String> msgIdList = new ArrayList<String>();
		if (_msgs != null) {
			for (Message msg : _msgs) {
				msgIdList.add(msg.getId());
			}
		}
		sb.append(msgIdList);
		return sb.toString();
	}

	@Override
	public Message getMessage() {
		return _batchMsg;
	}

	@Override
	public NotificationContext getNotificationContext() {
		return _context;
	}

	@Override
	public void onTimeout() {
		for (MessageHandler handler : _handlers) {
			if (handler != null) {
				handler.onTimeout();
			}
		}
	}
}
