package com.linkedin.helix.messaging.handling;

import java.util.List;
import java.util.concurrent.Future;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;

public interface TaskExecutor {
	public static final int DEFAULT_PARALLEL_TASKS = 40;

	/**
	 * register message handler factory this executor can handle
	 * 
	 * @param type
	 * @param factory
	 */
	public void registerMessageHandlerFactory(String type, MessageHandlerFactory factory);

	/**
	 * register message handler factory this executor can handle with specified
	 * thread-pool size
	 * 
	 * @param type
	 * @param factory
	 * @param threadpoolSize
	 */
	public void registerMessageHandlerFactory(String type, MessageHandlerFactory factory,
	        int threadPoolSize);

	/**
	 * schedule a message execution
	 * 
	 * @param message
	 * @param handler
	 * @param context
	 */
	public boolean scheduleTask(MessageTask task);

	/**
	 * blocking on scheduling all tasks
	 * 
	 * @param tasks
	 */
	public List<Future<HelixTaskResult>> invokeAllTasks(List<MessageTask> tasks)
	        throws InterruptedException;

	/**
	 * cancel a message execution
	 * 
	 * @param message
	 * @param context
	 */
	public boolean cancelTask(MessageTask task);

	/**
	 * cancel the timeout for the given task
	 * 
	 * @param task
	 * @return
	 */
	public boolean cancelTimeoutTask(MessageTask task);
	
	/**
	 * finish a message execution
	 * 
	 * @param message
	 */
	public void finishTask(MessageTask task);

	/**
	 * shutdown executor
	 */
	public void shutdown();
}
