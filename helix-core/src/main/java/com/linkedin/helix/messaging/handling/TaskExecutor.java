package com.linkedin.helix.messaging.handling;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;

public interface TaskExecutor {
	  public static final int DEFAULT_PARALLEL_TASKS = 40;

	  /**
	   * register message handler factory this executor can handle
	   * @param type
	   * @param factory
	   */
	  public void registerMessageHandlerFactory(String type, MessageHandlerFactory factory);
	  
	  /**
	   * register message handler factory this executor can handle with specified thread-pool size
	   * 
	   * @param type
	   * @param factory
	   * @param threadpoolSize
	   */
	  public void registerMessageHandlerFactory(String type, MessageHandlerFactory factory,
            int threadPoolSize);
	  
	  /**
	   * schedule a message execution
	   * @param message
	   * @param handler
	   * @param context
	   */
	  public void scheduleTask(Message message, MessageHandler handler,
              NotificationContext context);

	  /**
	   * cancel a message execution
	   * @param message
	   * @param context
	   */
	  public void cancelTask(Message message, NotificationContext context);
	  
	  /**
	   * finish a message execution
	   * @param message
	   */
	  public void finishTask(Message message);
	  
	  /**
	   * shutdown executor
	   */
	  public void shutdown();
}
