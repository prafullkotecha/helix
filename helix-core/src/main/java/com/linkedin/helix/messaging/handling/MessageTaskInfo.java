package com.linkedin.helix.messaging.handling;

import java.util.TimerTask;
import java.util.concurrent.Future;

public class MessageTaskInfo {
	final MessageTask _task;
	final Future<HelixTaskResult> _future;
	final TimerTask _timerTask;
	
	public MessageTaskInfo(MessageTask task, Future<HelixTaskResult> future, TimerTask timerTask)
	{
		_task = task;
		_future = future;
		_timerTask = timerTask;
	}

	public Future<HelixTaskResult> getFuture() {
		return _future;
	}
	
}
