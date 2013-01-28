package com.linkedin.helix.messaging.handling;

import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;

public class MessageTimeoutTask extends TimerTask {
	private static Logger LOG = Logger.getLogger(MessageTimeoutTask.class);

	final HelixTaskExecutor _executor;
	// Message _message;
	// NotificationContext _context;
	final MessageTask _task;

	public MessageTimeoutTask(HelixTaskExecutor executor, MessageTask task)
	// Message message,
	// NotificationContext context)
	{
		_executor = executor;
		_task = task;
		// _message = message;
		// _context = context;
	}

	@Override
	public void run() {
		// _isTimeout = true;
		Message message = _task.getMessage();
		// NotificationContext context = _task.getNotificationContext();
		System.out.println("msg: " + message.getMsgId() + " timeouot.");
		LOG.warn("Message time out, canceling. id:" + message.getMsgId() + " timeout : "
		        + message.getExecutionTimeout());
		_task.onTimeout();
		// _executor.cancelTask(_message, _context);
		boolean success = _executor.cancelTask(_task);

//		if (success) {
//			// check retry
//			int retryCount = message.getRetryCount();
//			System.out.println("retry msg: " + message.getId() + ", retryCount: " + retryCount);
//			LOG.info("Message timeout, retry count: " + retryCount + " MSGID:" + _task.getTaskId());
//
//			// TODO: add back log
//			// _statusUpdateUtil.logInfo(_message,
//			// _handler.getClass(),
//			// "Message handling task timeout, retryCount:"
//			// + retryCount,
//			// accessor);
//
//			// Notify the handler that timeout happens, and the number of
//			// retries left
//			// In case timeout happens (time out and also interrupted)
//			// we should retry the execution of the message by re-schedule it in
//			if (retryCount > 0) {
//				message.setRetryCount(retryCount - 1);
//
//				// HelixTask task = new HelixTask(message, context, _handler,
//				// _executor);
//				// _executor.scheduleTask(_message, _handler,
//				// _notificationContext);
//				_executor.scheduleTask(_task.clone());
//			}
//
//		}
	}

}