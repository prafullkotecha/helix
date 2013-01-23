package com.linkedin.helix.messaging.handling;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;

/**
 * Default implementation of handling start()/end() for batch messages 
 *
 */
public class BatchMsgWrapper 
{
	public void start(Message batchMsg, NotificationContext context) 
	{	
		System.out.println("default batchMsg.start() invoked");
	}
	
	public void end(Message batchMsg, NotificationContext context) 
	{	
		System.out.println("default batchMsg.end() invoked");
	}
}
