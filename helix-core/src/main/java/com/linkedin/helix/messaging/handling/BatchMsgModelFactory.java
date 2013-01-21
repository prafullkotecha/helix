package com.linkedin.helix.messaging.handling;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


// default impl of BatchMsgModelFactory
public class BatchMsgModelFactory<T extends BatchMsgModel>
{
	// resourceName -> batchMsgModel
	private final ConcurrentMap<String, T> _batchMsgModelMap = new ConcurrentHashMap<String, T>();

	public BatchMsgModel createBatchMsgModel(String resourceName)
	{
		// TODO: put batchMsgModel to map
		return new BatchMsgModel();
	}
}
