package com.linkedin.helix.messaging.handling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixException;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.NotificationContext.MapKey;
import com.linkedin.helix.PropertyKey;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.Attributes;
import com.linkedin.helix.participant.HelixStateMachineEngine;

public class BatchMsgHandler extends MessageHandler {
	private static Logger LOG = Logger.getLogger(BatchMsgHandler.class);

	final MessageHandlerFactory _msgHandlerFty;
	final BatchMsgWrapper _batchMsgWrapper;
	final TaskExecutor _executor;
	final List<Message> _subMessages;
	final List<MessageHandler> _subMessageHandlers;

	public BatchMsgHandler(Message msg, NotificationContext context, MessageHandlerFactory fty,
	        BatchMsgWrapper batchMsgWrapper, TaskExecutor executor) {
		super(msg, context);
		
		if (fty == null || executor == null) {
			throw new HelixException("MessageHandlerFactory | TaskExecutor can't be null");
		}
		
		_msgHandlerFty = fty;
		_batchMsgWrapper = batchMsgWrapper;
		_executor = executor;

		// create sub-messages
		_subMessages = new ArrayList<Message>();
		List<String> partitionKeys = _message.getPartitionNames();
		for (String partitionKey : partitionKeys) {
			// assign a new message id, put batch-msg-id to parent-id field
			Message subMsg = new Message(_message.getRecord(), UUID.randomUUID().toString());
			subMsg.setPartitionName(partitionKey);
			subMsg.setAttribute(Attributes.PARENT_MSG_ID, _message.getId());
			subMsg.setGroupMessageMode(false);

			_subMessages.add(subMsg);
		}

		// create sub-message handlers
		_subMessageHandlers = createMsgHandlers(_subMessages, context);
	}
	
	List<MessageHandler> createMsgHandlers(List<Message> msgs, NotificationContext context) {
		
		List<MessageHandler> handlers = new ArrayList<MessageHandler>();
		for (Message msg : msgs) {
			 MessageHandler handler = _msgHandlerFty.createHandler(msg, context);
			 handlers.add(handler);
		}
		return handlers;
	}


	public void preHandleMessage() {
		if (_message.getBatchMessageMode() == true && _batchMsgWrapper != null) {
			_batchMsgWrapper.start(_message, _notificationContext);
		}

	}

	public void postHandleMessage() {
		if (_message.getBatchMessageMode() == true && _batchMsgWrapper != null) {
			_batchMsgWrapper.end(_message, _notificationContext);
		}

		// update currentState
		HelixManager manager = _notificationContext.getManager();
		HelixDataAccessor accessor = manager.getHelixDataAccessor();
		ConcurrentHashMap<String, CurrentStateUpdate> csUpdateMap = (ConcurrentHashMap<String, CurrentStateUpdate>) _notificationContext
		        .get(MapKey.CURRENT_STATE_UPDATE.toString());
		
		if (csUpdateMap != null) {
    		Map<PropertyKey, CurrentState> csUpdate = mergeCurStateUpdate(csUpdateMap);
    
    		// TODO: change to use asyncSet
    		for (PropertyKey key : csUpdate.keySet()) {
    			// logger.info("updateCS: " + key);
    			// System.out.println("\tupdateCS: " + key.getPath() + ", " +
    			// curStateMap.get(key));
    			accessor.updateProperty(key, csUpdate.get(key));
    		}
		}
	}

	// will not return until all sub-message executions are done
	@Override
	public HelixTaskResult handleMessage() {
		synchronized (_batchMsgWrapper) {
			preHandleMessage();


			// System.err.println("create subMsgs: " + subMsgs);

			int exeBatchSize = 1; // TODO: getExeBatchSize from msg
			List<MessageTask> batchTasks = new ArrayList<MessageTask>();
			List<String> partitionKeys = _message.getPartitionNames();
			for (int i = 0; i < partitionKeys.size(); i += exeBatchSize) {
				if (i + exeBatchSize <= partitionKeys.size()) {
					List<Message> msgs = _subMessages.subList(i, i + exeBatchSize);
					List<MessageHandler> handlers = _subMessageHandlers.subList(i, i + exeBatchSize);
					HelixBatchMsgTask batchTask = new HelixBatchMsgTask(_message, msgs, handlers, _notificationContext);
					batchTasks.add(batchTask);

				} else {
					List<Message> msgs = _subMessages.subList(i, i + partitionKeys.size());
					List<MessageHandler> handlers = _subMessageHandlers.subList(i, i + partitionKeys.size());

					HelixBatchMsgTask batchTask = new HelixBatchMsgTask(_message, msgs, handlers, _notificationContext);
					batchTasks.add(batchTask);
				}
			}

			HelixTaskResult result = new HelixTaskResult();
			try {
				// invokeAll() is blocking call
				List<Future<HelixTaskResult>> futures = _executor.invokeAllTasks(batchTasks);
				for (Future<HelixTaskResult> future : futures) {
					HelixTaskResult taskResult = future.get();

					// if any subMsg execution fails, skip postHandling() and
					// return
					if (!taskResult.isSucess()) {
						onError(taskResult.getException(), taskResult.getErrCode(), taskResult.getErrType());
						return taskResult;
					}

				}
			} catch (Exception e) {
				LOG.error("fail to execute batchMsg: " + _message.getId(), e);
				result.setException(e);
				return result;
			}

			postHandleMessage();

			// TODO: fill result
			result.setSuccess(true);
			return result;
		}
	}

	@Override
	public void onError(Exception e, ErrorCode code, ErrorType type) {
		// if one sub-message execution fails, call onError on all sub-message handlers
		for (MessageHandler handler : _subMessageHandlers) {
			handler.onError(e, code, type);
		}
	}

	// TODO: optimize this based on the fact that each cs update is for a
	// distinct partition
	private Map<PropertyKey, CurrentState> mergeCurStateUpdate(
	        ConcurrentHashMap<String, CurrentStateUpdate> csUpdateMap) {
		Map<String, CurrentStateUpdate> curStateUpdateMap = new HashMap<String, CurrentStateUpdate>();
		for (CurrentStateUpdate update : csUpdateMap.values()) {
			String path = update._key.getPath(); // TODO: this is time
			                                     // consuming, optimize it
			if (!curStateUpdateMap.containsKey(path)) {
				curStateUpdateMap.put(path, update);
			} else {
				// long start = System.currentTimeMillis();
				curStateUpdateMap.get(path).merge(update._delta);
				// long end = System.currentTimeMillis();
				// LOG.info("each merge took: " + (end - start));
			}
		}

		Map<PropertyKey, CurrentState> ret = new HashMap<PropertyKey, CurrentState>();
		for (CurrentStateUpdate update : curStateUpdateMap.values()) {
			ret.put(update._key, update._delta);
		}

		return ret;
	}

}
