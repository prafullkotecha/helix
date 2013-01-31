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
package com.linkedin.helix.manager.zk;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.linkedin.helix.BaseDataAccessor;
import com.linkedin.helix.ConfigChangeListener;
import com.linkedin.helix.ControllerChangeListener;
import com.linkedin.helix.CurrentStateChangeListener;
import com.linkedin.helix.ExternalViewChangeListener;
import com.linkedin.helix.HealthStateChangeListener;
import com.linkedin.helix.HelixConstants.ChangeType;
import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixProperty;
import com.linkedin.helix.IdealStateChangeListener;
import com.linkedin.helix.InstanceConfigChangeListener;
import com.linkedin.helix.LiveInstanceChangeListener;
import com.linkedin.helix.MessageListener;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyKey;
import com.linkedin.helix.PropertyPathConfig;
import com.linkedin.helix.ScopedConfigChangeListener;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.HealthStat;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.InstanceConfig;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message;

public class CallbackHandler implements IZkChildListener, IZkDataListener
{
  private static Logger logger = Logger.getLogger(CallbackHandler.class);

  private final String _path;
  private final Object _listener;
  
  // TODO: eventType is not used
  private final EventType[] _eventTypes;
  private final HelixDataAccessor _accessor;
  private final ChangeType _changeType;
  private final ZkClient _zkClient;
  private final AtomicLong lastNotificationTimeStamp;
  private final HelixManager _manager;

  private final PropertyKey _propertyKey;
  private final BaseDataAccessor<ZNRecord> _baseAccessor;
  
  public CallbackHandler(HelixManager manager, ZkClient client, PropertyKey propertyKey,
                         Object listener, EventType[] eventTypes, ChangeType changeType)
  {
    this._manager = manager;
    this._accessor = manager.getHelixDataAccessor();
    this._zkClient = client;
    this._propertyKey = propertyKey;
    this._path = propertyKey.getPath();
    this._listener = listener;
    this._eventTypes = eventTypes;
    this._changeType = changeType;
    _baseAccessor = new ZkBaseDataAccessor<ZNRecord>(client);
    
    lastNotificationTimeStamp = new AtomicLong(System.nanoTime());
    init();
  }

  public Object getListener()
  {
    return _listener;
  }

  public String getPath()
  {
    return _path;
  }

  public void invoke(NotificationContext changeContext) throws Exception
  {
    // This allows the listener to work with one change at a time
    synchronized (_manager)
    {
      long start = System.currentTimeMillis();
      if (logger.isInfoEnabled())
      {
        logger.info(Thread.currentThread().getId() + " START:INVOKE "
            + _path + " listener:" + _listener.getClass().getCanonicalName());
      }

      switch(_changeType)
      {
      case IDEAL_STATE:
      {
        IdealStateChangeListener idealStateChangeListener =
            (IdealStateChangeListener) _listener;
        subscribeForChanges(changeContext, _path, true, true);
        List<IdealState> idealStates = _accessor.getChildValues(_propertyKey);

        idealStateChangeListener.onIdealStateChange(idealStates, changeContext);
        break;
      }
      case INSTANCE_CONFIG:
      {
        subscribeForChanges(changeContext, _path, true, true);
    	if (_listener instanceof ConfigChangeListener)
    	{
    		ConfigChangeListener configChangeListener = (ConfigChangeListener) _listener;
    		List<InstanceConfig> configs = _accessor.getChildValues(_propertyKey);
    		configChangeListener.onConfigChange(configs, changeContext);
    	} else if (_listener instanceof InstanceConfigChangeListener)
    	{
    		InstanceConfigChangeListener listener = (InstanceConfigChangeListener) _listener;
    		List<InstanceConfig> configs = _accessor.getChildValues(_propertyKey);
    		listener.onInstanceConfigChange(configs, changeContext);    		
    	}
    	break;
      }
      case CONFIG: 
      {
        subscribeForChanges(changeContext, _path, true, true);
		ScopedConfigChangeListener listener = (ScopedConfigChangeListener) _listener;
		List<HelixProperty> configs = _accessor.getChildValues(_propertyKey);
		listener.onConfigChange(configs, changeContext);
		break;
      }
      case LIVE_INSTANCE:
      {
        LiveInstanceChangeListener liveInstanceChangeListener =
            (LiveInstanceChangeListener) _listener;
        subscribeForChanges(changeContext, _path, true, true);
        List<LiveInstance> liveInstances =
            _accessor.getChildValues(_propertyKey);
        liveInstanceChangeListener.onLiveInstanceChange(liveInstances, changeContext);
        break;
      }
      case CURRENT_STATE:
      {
        CurrentStateChangeListener currentStateChangeListener;
        currentStateChangeListener = (CurrentStateChangeListener) _listener;
        subscribeForChanges(changeContext, _path, true, true);
        String instanceName = PropertyPathConfig.getInstanceNameFromPath(_path);
        List<CurrentState> currentStates = _accessor.getChildValues(_propertyKey);

        currentStateChangeListener.onStateChange(instanceName,
                                                 currentStates,
                                                 changeContext);
        break;
      }
      case MESSAGE:
      {
        MessageListener messageListener = (MessageListener) _listener;
        subscribeForChanges(changeContext, _path, true, false);
        String instanceName = PropertyPathConfig.getInstanceNameFromPath(_path);
        List<Message> messages =
            _accessor.getChildValues(_propertyKey);

        messageListener.onMessage(instanceName, messages, changeContext);
        break;
      }
      case MESSAGES_CONTROLLER:
      {
        MessageListener messageListener = (MessageListener) _listener;
        subscribeForChanges(changeContext, _path, true, false);
        List<Message> messages =
            _accessor.getChildValues(_propertyKey);

        messageListener.onMessage(_manager.getInstanceName(), messages, changeContext);
        break;
      }
      case EXTERNAL_VIEW:
      {
        ExternalViewChangeListener externalViewListener =
            (ExternalViewChangeListener) _listener;
        subscribeForChanges(changeContext, _path, true, true);
        List<ExternalView> externalViewList =
            _accessor.getChildValues(_propertyKey);

        externalViewListener.onExternalViewChange(externalViewList, changeContext);
        break;
      }
      case CONTROLLER:
      {
        ControllerChangeListener controllerChangelistener =
            (ControllerChangeListener) _listener;
        subscribeForChanges(changeContext, _path, true, false);
        controllerChangelistener.onControllerChange(changeContext);
        break;
      }
      case HEALTH:
      {
        HealthStateChangeListener healthStateChangeListener =
            (HealthStateChangeListener) _listener;
        subscribeForChanges(changeContext, _path, true, true); // TODO: figure out
        // settings here
        String instanceName = PropertyPathConfig.getInstanceNameFromPath(_path);

        List<HealthStat> healthReportList =
            _accessor.getChildValues(_propertyKey);

        healthStateChangeListener.onHealthChange(instanceName,
                                                 healthReportList,
                                                 changeContext);
        break;
      }
      default:
    	break;
      }
      
      long end = System.currentTimeMillis();
      if (logger.isInfoEnabled())
      {
        logger.info(Thread.currentThread().getId() + " END:INVOKE " + _path
            + " listener:" + _listener.getClass().getCanonicalName() + " Took: "
            + (end - start));
      }
    }
  }

  private void subscribeChildChange(String path, NotificationContext context)
  {
	  NotificationContext.Type type = context.getType();
      if (type == NotificationContext.Type.INIT || type == NotificationContext.Type.CALLBACK)
      {
        logger.info(_manager.getInstanceName() + " subscribe child change@" + path);
        _zkClient.subscribeChildChanges(path, this);
      }
      else if (type == NotificationContext.Type.FINALIZE)
      {
        logger.info(_manager.getInstanceName() + " UNsubscribe child change@" + path);
        _zkClient.unsubscribeChildChanges(path, this);
      }
  }
  
  private void subscribeDataChange(String path, NotificationContext context)
  {
    	NotificationContext.Type type = context.getType();
        if (type == NotificationContext.Type.INIT
            || type == NotificationContext.Type.CALLBACK)
        {
          if (logger.isDebugEnabled())
          {
            logger.debug(_manager.getInstanceName() + " subscribe data change@" + path);
          }
          _zkClient.subscribeDataChanges(path, this);

        }
        else if (type == NotificationContext.Type.FINALIZE)
        {
          logger.info(_manager.getInstanceName() + " UNsubscribe data change@" + path);
          _zkClient.unsubscribeDataChanges(path, this);
        }
  }
  
  private void subscribeForChanges(NotificationContext context,
                                   String path,
                                   boolean watchParent,
                                   boolean watchChild)
  {
    if (watchParent)
    {
    	subscribeChildChange(path, context);
    }

    if (watchChild)
    {
      try
      {
    	switch(_changeType)
    	{
        case CURRENT_STATE:
        case IDEAL_STATE:
        case EXTERNAL_VIEW:
        {
            // check if bucketized
        	List<ZNRecord> records = _baseAccessor.getChildren(path, null, 0);
        	for (ZNRecord record : records)
        	{
                HelixProperty property = new HelixProperty(record);
            	String childPath = path + "/" + record.getId();

                int bucketSize = property.getBucketSize();
                if (bucketSize > 0)
                {
                  // subscribe both data-change and child-change on bucketized parent
                  // data-change gives a delete-callback which is used to remove watch
                  subscribeChildChange(childPath, context);
                  subscribeDataChange(childPath, context);
                  
                  // subscribe data-change on bucketized child
                  List<String> bucketizedChildNames = _zkClient.getChildren(childPath);
                  if (bucketizedChildNames != null) 
                  {
                    for (String bucketizedChildName : bucketizedChildNames)
                    {
                       String bucketizedChildPath = childPath + "/" + bucketizedChildName;
                       subscribeDataChange(bucketizedChildPath, context);
                    }  
                  }
                } else
                {
                    subscribeDataChange(childPath, context);
                }
        	}
        	break;
        }
        default:
        {
            List<String> childNames = _zkClient.getChildren(path);
            if (childNames != null) 
            {
              for (String childName : childNames)
              {
                 String childPath = path + "/" + childName;
                 subscribeDataChange(childPath, context);
              }  
            }
        	break;
        }
    	}
      }
      catch (ZkNoNodeException e)
      {
        logger.warn("fail to subscribe child/data change@" + path, e);
      }
    }

  }

  public EventType[] getEventTypes()
  {
    return _eventTypes;
  }

  /**
   * Invoke the listener so that it sets up the initial values from the zookeeper if any
   * exists
   * 
   */
  public void init()
  {
    updateNotificationTime(System.nanoTime());
    try
    {
      NotificationContext changeContext = new NotificationContext(_manager);
      changeContext.setType(NotificationContext.Type.INIT);
      invoke(changeContext);
    }
    catch (Exception e)
    {
      ZKExceptionHandler.getInstance().handle(e);
    }
  }

  @Override
  public void handleDataChange(String dataPath, Object data)
  {
    try
    {
      updateNotificationTime(System.nanoTime());
      if (dataPath != null && dataPath.startsWith(_path))
      {
        NotificationContext changeContext = new NotificationContext(_manager);
        changeContext.setType(NotificationContext.Type.CALLBACK);
        invoke(changeContext);
      }
    }
    catch (Exception e)
    {
      ZKExceptionHandler.getInstance().handle(e);
    }
  }

  @Override
  public void handleDataDeleted(String dataPath)
  {
    try
    {
      updateNotificationTime(System.nanoTime());
      if (dataPath != null && dataPath.startsWith(_path))
      {
          logger.info(_manager.getInstanceName() + " UNsubscribe data change@" + dataPath);
          _zkClient.unsubscribeDataChanges(dataPath, this);

          // only for bucketized parent. OK if we don't have child-change watch on the path
          logger.info(_manager.getInstanceName() + " UNsubscribe child change@" + dataPath);
          _zkClient.unsubscribeChildChanges(dataPath, this);

          // No need to invoke() since this event will handled by child-change on parent-node
//        NotificationContext changeContext = new NotificationContext(_manager);
//        changeContext.setType(NotificationContext.Type.CALLBACK);
// 		  invoke(changeContext);
      }
    }
    catch (Exception e)
    {
      ZKExceptionHandler.getInstance().handle(e);
    }
  }

  @Override
  public void handleChildChange(String parentPath, List<String> currentChilds)
  {
    try
    {
      updateNotificationTime(System.nanoTime());
      if (parentPath != null && parentPath.startsWith(_path))
      {
        NotificationContext changeContext = new NotificationContext(_manager);
        changeContext.setType(NotificationContext.Type.CALLBACK);
        invoke(changeContext);
      }
    }
    catch (Exception e)
    {
      ZKExceptionHandler.getInstance().handle(e);
    }
  }

  /**
   * Invoke the listener for the last time so that the listener could clean up resources
   * 
   */
  public void reset()
  {
    try
    {
      NotificationContext changeContext = new NotificationContext(_manager);
      changeContext.setType(NotificationContext.Type.FINALIZE);
      invoke(changeContext);
    }
    catch (Exception e)
    {
      ZKExceptionHandler.getInstance().handle(e);
    }
  }

  private void updateNotificationTime(long nanoTime)
  {
    long l = lastNotificationTimeStamp.get();
    while (nanoTime > l)
    {
      boolean b = lastNotificationTimeStamp.compareAndSet(l, nanoTime);
      if (b)
      {
        break;
      }
      else
      {
        l = lastNotificationTimeStamp.get();
      }
    }
  }

}
