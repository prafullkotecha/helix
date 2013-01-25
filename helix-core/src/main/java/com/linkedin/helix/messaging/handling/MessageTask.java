package com.linkedin.helix.messaging.handling;

import java.util.concurrent.Callable;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;

public interface MessageTask extends Callable<HelixTaskResult>{
	String getTaskId();
	
	Message getMessage();
	
	NotificationContext getNotificationContext();
}
