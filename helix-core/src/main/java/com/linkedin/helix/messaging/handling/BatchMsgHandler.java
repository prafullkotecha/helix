package com.linkedin.helix.messaging.handling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyKey;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.Attributes;

public class BatchMsgHandler extends MessageHandler {
	final MessageHandlerFactory _msgHandlerFty;
	final BatchMsgModel _batchMsgModel;
	final ExecutorService _executorSvc;

	public BatchMsgHandler(Message msg, NotificationContext context, MessageHandlerFactory fty,
	        BatchMsgModel batchMsgModel, ExecutorService exeSvc) {
		super(msg, context);
		_msgHandlerFty = fty;
		_batchMsgModel = batchMsgModel;
		_executorSvc = exeSvc;
	}

	public void preHandleMessage() {
		if (_message.getGroupMessageMode() == true && _batchMsgModel != null) {
			_batchMsgModel.start(_message, _notificationContext);
		}

	}

	public void postHandleMessage() {
		if (_message.getGroupMessageMode() == true && _batchMsgModel != null) {
			_batchMsgModel.end(_message, _notificationContext);
		}

		// update currentState
		HelixManager manager = _notificationContext.getManager();
		HelixDataAccessor accessor = manager.getHelixDataAccessor();
		ConcurrentHashMap<String, CurrentStateUpdate> csUpdateMap = (ConcurrentHashMap<String, CurrentStateUpdate>) _notificationContext
		        .get("HELIX_CURRENT_STATE_UPDATE");
		Map<PropertyKey, CurrentState> csUpdate = merge(csUpdateMap);
		// TODO: change to use asyncSet
		for (PropertyKey key : csUpdate.keySet()) {
			// logger.info("updateCS: " + key);
			// System.out.println("\tupdateCS: " + key.getPath() + ", " +
			// curStateMap.get(key));
			accessor.updateProperty(key, csUpdate.get(key));
		}
	}

	// will not return until all sub-messages are done
	@Override
	public HelixTaskResult handleMessage() {
		preHandleMessage();

		List<Message> subMsgs = new ArrayList<Message>();
		List<String> partitionKeys = _message.getPartitionNames();
		for (String partitionKey : partitionKeys) {
			Message subMsg = new Message(_message.getRecord());
			subMsg.setPartitionName(partitionKey);
			subMsg.setAttribute(Attributes.PARENT_MSG_ID, _message.getId());
			subMsg.setGroupMessageMode(false);

			subMsgs.add(subMsg);
		}

		int exeBatchSize = 1; // TODO: getExeBatchSize from msg
		List<HelixBatchMsgTask> batchTasks = new ArrayList<HelixBatchMsgTask>();
		for (int i = 0; i < partitionKeys.size(); i += exeBatchSize) {
			HelixBatchMsgTask batchTask = null;
			if (i + exeBatchSize <= partitionKeys.size()) {
				batchTask = new HelixBatchMsgTask(subMsgs.subList(i, i + exeBatchSize),
				        _notificationContext, _msgHandlerFty);
			} else {
				batchTask = new HelixBatchMsgTask(subMsgs.subList(i, i + partitionKeys.size()),
				        _notificationContext, _msgHandlerFty);
			}
			if (batchTask != null) {
				batchTasks.add(batchTask);
			}
		}
		try {
			// invokeAll() is blocking call
			_executorSvc.invokeAll(batchTasks);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		postHandleMessage();
		return null;
	}

	@Override
	public void onError(Exception e, ErrorCode code, ErrorType type) {
		// TODO: call onError on each subMsg handler

	}

	// TODO: optimize this based on the fact that each cs update is for one
	// partition
	public Map<PropertyKey, CurrentState> merge(
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
