package com.linkedin.helix.messaging.handling;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyKey;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.participant.statemachine.StateModel;
import com.linkedin.helix.participant.statemachine.StateModelParser;
import com.linkedin.helix.participant.statemachine.StateTransitionError;
import com.linkedin.helix.util.StatusUpdateUtil;

public class HelixBatchMessageHandler extends MessageHandler {
	private static Logger LOG = Logger.getLogger(HelixBatchMessageHandler.class);

	private final List<StateModel> _stateModels;
	StatusUpdateUtil _statusUpdateUtil;
	private final StateModelParser _transitionMethodFinder;
	private final CurrentState _currentStateDelta;
	volatile boolean _isTimeout = false;
	private final HelixTaskExecutor _executor;
	final List<MessageHandler> _subHandlers;
	final List<Message> _subMsgs;

	// wait for all sub-tasks to complete
	final CountDownLatch _countDown;

	// hold cs update from sub-tasks
	final ConcurrentLinkedQueue<CurrentStateUpdate> _curStateUpdateList;

	public HelixBatchMessageHandler(List<StateModel> stateModels, Message message,
	        NotificationContext context, CurrentState currentStateDelta,
	        HelixTaskExecutor executor, List<MessageHandler> subHandlers, List<Message> subMsgs) {
		super(message, context);
		_stateModels = stateModels;
		_statusUpdateUtil = new StatusUpdateUtil();
		_transitionMethodFinder = new StateModelParser();
		_currentStateDelta = currentStateDelta;
		_executor = executor;
		_subHandlers = subHandlers;
		_subMsgs = subMsgs;

		// count-down
		List<String> partitionNames = message.getPartitionNames();
		int partitionNb = partitionNames.size();
		int exeBatchSize = message.getExeBatchSize();
		_countDown = new CountDownLatch((partitionNb + exeBatchSize - 1) / exeBatchSize); // round
		                                                                                  // up
		//
		_curStateUpdateList = new ConcurrentLinkedQueue<CurrentStateUpdate>();
	}

	@Override
	public HelixTaskResult handleMessage() throws InterruptedException {

		System.err.println("handling batch message: " + _message);

		HelixTaskResult taskResult = new HelixTaskResult();
		HelixManager manager = _notificationContext.getManager();
		HelixDataAccessor accessor = manager.getHelixDataAccessor();

		_statusUpdateUtil.logInfo(_message, HelixStateTransitionHandler.class,
		        "Message handling task begin execute", accessor);
		_message.setExecuteStartTimeStamp(new Date().getTime());

		Exception exception = null;
		try {
			// prepareMessageExecution(manager, _message);
			invoke(accessor, _notificationContext, taskResult, _message);
			// } catch (HelixStateMismatchException e) {
			// // Simply log error and return from here if State mismatch.
			// // The current state of the state model is intact.
			// taskResult.setSuccess(false);
			// taskResult.setMessage(e.toString());
			// taskResult.setException(e);
			// exception = e;
			// // return taskResult;
		} catch (Exception e) {
			String errorMessage = "Exception while executing a state transition task "
			        + _message.getPartitionName();
			// logger.error(errorMessage, e);
			if (e.getCause() != null && e.getCause() instanceof InterruptedException) {
				e = (InterruptedException) e.getCause();
			}
			_statusUpdateUtil.logError(_message, HelixStateTransitionHandler.class, e,
			        errorMessage, accessor);
			taskResult.setSuccess(false);
			taskResult.setMessage(e.toString());
			taskResult.setException(e);
			taskResult.setInterrupted(e instanceof InterruptedException);
			exception = e;
		}
		postExecutionMessage(manager, _message, _notificationContext, taskResult, exception);

		return taskResult;
	}

	@Override
	public void onError(Exception e, ErrorCode code, ErrorType type) {
		// TODO Auto-generated method stub

	}

	private void invoke(HelixDataAccessor accessor, NotificationContext context,
	        HelixTaskResult taskResult, Message message) throws IllegalAccessException,
	        InvocationTargetException, InterruptedException {
		_statusUpdateUtil.logInfo(message, HelixStateTransitionHandler.class,
		        "Message handling invoking", accessor);

		// by default, we invoke state transition function in state model
		Method methodToInvoke = null;
		// String fromState = message.getFromState();
		// String toState = message.getToState();
		// methodToInvoke =
		// _transitionMethodFinder.getMethodForTransition(_stateModels.get(0)
		// .getClass(), fromState, toState, new Class[] { Message.class,
		// NotificationContext.class });

		// invoke begin method
		methodToInvoke = _transitionMethodFinder.getMethodByConvention(_stateModels.get(0)
		        .getClass(), "begin", new Class[] { Message.class, NotificationContext.class });

		if (methodToInvoke != null) {
			methodToInvoke.invoke(_stateModels.get(0), new Object[] { message, context });

			// submit helix tasks for each sub message
			context.add("BATCH_HANDLER", this);
//			_executor.scheduleTaskAll(_subMsgs, _subHandlers, context);
			for (int i = 0; i < _subHandlers.size(); i++) {
				MessageHandler handler = _subHandlers.get(i);
				Message subMsg = _subMsgs.get(i);

				// pass batch message handler through context
//				System.err.println("schedule task: " + handler);
				_executor.scheduleTask(subMsg, handler, context);
			}

			// need to wait for every task to finish
			_countDown.await();

			// invoke end method
			methodToInvoke = _transitionMethodFinder.getMethodByConvention(_stateModels.get(0)
			        .getClass(), "end", new Class[] { Message.class, NotificationContext.class });

			if (methodToInvoke != null) {
				methodToInvoke.invoke(_stateModels.get(0), new Object[] { message, context });

				// set result successful
				taskResult.setSuccess(true);
			} else {
				System.err.println("error in finding method for end");
			}

		} else {
			String errorMessage = "Unable to find method for begin in "
			        + _stateModels.get(0).getClass();
			// logger.error(errorMessage);
			taskResult.setSuccess(false);

			_statusUpdateUtil.logError(message, HelixStateTransitionHandler.class, errorMessage,
			        accessor);
		}
		
		// tell HelixTask to remove msg and send reply msg
		context.add("NEED_REMOVE_BATCH_MSG", new Boolean(true));
	}

	void postExecutionMessage(HelixManager manager, Message message, NotificationContext context,
	        HelixTaskResult taskResult, Exception exception) {
//		List<String> partitionKeys = message.getExePartitionNames();
//
//		String resource = message.getResourceName();
//		String sessionId = message.getTgtSessionId();
//		String instanceName = manager.getInstanceName();

		HelixDataAccessor accessor = manager.getHelixDataAccessor();
//		Builder keyBuilder = accessor.keyBuilder();

//		int bucketSize = message.getBucketSize();
//		ZNRecordBucketizer bucketizer = new ZNRecordBucketizer(bucketSize);

		// Lock the helix manager so that the session id will not change when we
		// update
		// the state model state. for zk current state it is OK as we have the
		// per-session
		// current state node
		// synchronized (manager)
		{
			if (!message.getTgtSessionId().equals(manager.getSessionId())) {
				LOG.warn("Session id has changed during message execution. Skip postExecutionMessage. Old session "
				        + message.getExecutionSessionId()
				        + " , new session : "
				        + manager.getSessionId());
				return;
			}

		}

		try {
			long start = System.currentTimeMillis();
			Map<PropertyKey, CurrentState> curStateMap = merge();
			long end = System.currentTimeMillis();
			LOG.info("merge currentState took: " + (end - start));
			for (PropertyKey key : curStateMap.keySet()) {
				// logger.info("updateCS: " + key);
				// System.out.println("\tupdateCS: " + key.getPath() + ", " +
				// curStateMap.get(key));
				accessor.updateProperty(key, curStateMap.get(key));
			}

		} catch (Exception e) {
			LOG.error("Error when updating the current state ", e);
			StateTransitionError error = new StateTransitionError(ErrorType.FRAMEWORK,
			        ErrorCode.ERROR, e);
			for (StateModel stateModel : _stateModels) {
				stateModel.rollbackOnError(message, context, error);
			}
			_statusUpdateUtil.logError(message, HelixStateTransitionHandler.class, e,
			        "Error when update the current state ", accessor);
		}
	}

	void addCurStateUpdate(Message subMessage, PropertyKey key, CurrentState delta) {
		// System.out.println("\tadd curState update: " + key + ", " + delta);
		_curStateUpdateList.add(new CurrentStateUpdate(key, delta));
		_countDown.countDown();
	}

	public Map<PropertyKey, CurrentState> merge() {
		Map<String, CurrentStateUpdate> curStateUpdateMap = new HashMap<String, CurrentStateUpdate>();
		for (CurrentStateUpdate update : _curStateUpdateList) {
			String path = update._key.getPath();
			if (!curStateUpdateMap.containsKey(path)) {
				curStateUpdateMap.put(path, update);
			} else {
//				long start = System.currentTimeMillis();
				curStateUpdateMap.get(path).merge(update._curStateDelta);
//				long end = System.currentTimeMillis();
//				LOG.info("each merge took: " + (end - start));
			}
		}

		Map<PropertyKey, CurrentState> ret = new HashMap<PropertyKey, CurrentState>();
		for (CurrentStateUpdate update : curStateUpdateMap.values()) {
			ret.put(update._key, update._curStateDelta);
		}

		return ret;
	}
}
