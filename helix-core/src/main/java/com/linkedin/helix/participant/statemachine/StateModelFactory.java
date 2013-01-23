/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.participant.statemachine;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.linkedin.helix.messaging.handling.BatchMsgWrapper;

public abstract class StateModelFactory<T extends StateModel>
{
  // partitionName -> StateModel
  private ConcurrentMap<String, T> _stateModelMap = new ConcurrentHashMap<String, T>();
  
  // resourceName -> BatchMsgWrapper
  private final ConcurrentMap<String, BatchMsgWrapper> _batchMsgWrapperMap 
      = new ConcurrentHashMap<String, BatchMsgWrapper>();

  /**
   * This method will be invoked only once per partitionName per session
   * 
   * @param partitionName
   * @return
   */
  public abstract T createNewStateModel(String partitionName);

  /**
   * Add a state model for a partition
   * Doesn't seem to be used by Helix, stateModel should be generated in this factory
   * 
   * @param partitionName
   * @return
   */
  @Deprecated
  public void addStateModel(String partitionName, T stateModel)
  {
    _stateModelMap.put(partitionName, stateModel);
  }
  
  /**
   * Create a state model for a partition and add it to map
   * 
   * @param partitionName
   */
  public T createAndAddStateModel(String partitionName)
  {
	T stateModel = createNewStateModel(partitionName);
    _stateModelMap.put(partitionName, stateModel);
    return stateModel;
  }

  /**
   * Get the state model for a partition
   * 
   * @param partitionName
   * @return
   */
  public T getStateModel(String partitionName)
  {
    return _stateModelMap.get(partitionName);
  }

  /**
   * Get the state model map
   * 
   * @return
   */
  public Map<String, T> getStateModelMap()
  {
    return _stateModelMap;
  }
  
  /**
   * Create batch message wrapper for a resource
   * 
   * @param resourceName
   * @return
   */
  public BatchMsgWrapper createBatchMsgWrapper(String resourceName)
  {
	  return new BatchMsgWrapper();
  }
  
  /**
   * create batch message wrapper for a resource and put it into map
   * 
   * @param partitionName
   * @return
   */
  public BatchMsgWrapper createAndAddBatchMsgWrapper(String resourceName)
  {
	BatchMsgWrapper wrapper = createBatchMsgWrapper(resourceName);
	_batchMsgWrapperMap.put(resourceName, wrapper);
    return wrapper;
  }

  /**
   * get batch message wrapper
   * 
   * @param resourceName
   * @return
   */
  public BatchMsgWrapper getBatchMsgWrapper(String resourceName)
  {
    return _batchMsgWrapperMap.get(resourceName);
  }


}
