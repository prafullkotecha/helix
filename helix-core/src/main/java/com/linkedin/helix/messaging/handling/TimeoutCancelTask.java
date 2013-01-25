package com.linkedin.helix.messaging.handling;

import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.linkedin.helix.model.Message;

public class TimeoutCancelTask extends TimerTask {
	private static Logger LOG = Logger.getLogger(TimeoutCancelTask.class);

	final HelixTaskExecutor _executor;
	// Message _message;
	// NotificationContext _context;
	final MessageTask _task;

	public TimeoutCancelTask(HelixTaskExecutor executor, MessageTask task)
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
//		_isTimeout = true;
		Message message = _task.getMessage();
		LOG.warn("Message time out, canceling. id:" + message.getMsgId() + " timeout : "
		        + message.getExecutionTimeout());
		_task.onTimeout();
		// _executor.cancelTask(_message, _context);
		_executor.cancelTask(_task);
	}

}