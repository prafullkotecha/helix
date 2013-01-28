package com.linkedin.helix.messaging.handling;

import java.util.concurrent.Callable;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;

// clone is needed when MessageTask times out and a clone task needs to be scheduled
public interface MessageTask extends Callable<HelixTaskResult> {
	String getTaskId();
	
	Message getMessage();
	
	NotificationContext getNotificationContext();
	
	void onTimeout();

//	MessageTask clone();
}
