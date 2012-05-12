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
package com.linkedin.helix.store.zk;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.log4j.Logger;

import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.store.PropertyChangeListener;

class ZkCallbackHandler<T> implements IZkChildListener, IZkDataListener
{
  private static Logger LOG = Logger.getLogger(ZkCallbackHandler.class);

  private final ZkClient _zkClient;
  private final ZKPropertyStore<T> _store;

  // listen on prefix and all its childs
  private final String _prefix;
//  private final PropertyChangeListener<T> _listener;

  final Set<PropertyChangeListener<T>> _listeners = new CopyOnWriteArraySet<PropertyChangeListener<T>>();
  
  public ZkCallbackHandler(ZkClient client, ZKPropertyStore<T> store, String prefix)
//                           PropertyChangeListener<T> listener)
  {
    _zkClient = client;
    _store = store;
    _prefix = prefix;
//    _listener = listener;
  }

  void fireListeners(String key)
  {
    for (PropertyChangeListener<T> listener : _listeners) 
    {
      listener.onPropertyChange(key);
    }
  }
  
  public void addListener(PropertyChangeListener<T> listener)
  {
    _listeners.add(listener);
  }
  
  @Override
  public void handleDataChange(String path, Object data) throws Exception
  {
    LOG.debug("dataChanged@ " + path + ", newData: " + data);
    String key = _store.getRelativePath(path);
//    _listener.onPropertyChange(key);
    fireListeners(key);
  }

  @Override
  public void handleDataDeleted(String path) throws Exception
  {
    LOG.debug("dataDeleted@ " + path);
    
    _zkClient.unsubscribeDataChanges(path, this);
    _zkClient.unsubscribeChildChanges(path, this);

    String key = _store.getRelativePath(path);
    fireListeners(key);
  }

  @Override
  public void handleChildChange(String path, List<String> currentChilds) throws Exception
  {
    LOG.debug("childChange@ " + path + ", curChilds: " + currentChilds);
    // System.out.println("childs changed @ " + path + " to " + currentChilds);


    if (currentChilds == null)
    {
      /**
       * When a node with a child change watcher is deleted
       * a child change is triggered on the deleted node
       * and in this case, the currentChilds is null
       */
//      return;
//    } else if (currentChilds.size() == 0)
//    {
//      String key = _store.getRelativePath(path);
//      _listener.onPropertyChange(key);
    }
    else
    {
      _zkClient.subscribeDataChanges(path, this);
      _zkClient.subscribeChildChanges(path, this);

      String key = _store.getRelativePath(path);
//      _listener.onPropertyChange(key);
      fireListeners(key);

      for (String child : currentChilds)
      {
        String childPath = path.endsWith("/") ? path + child : path + "/" + child;
//        _zkClient.subscribeDataChanges(childPath, this);
//        _zkClient.subscribeChildChanges(childPath, this);

        // recursive call
        // TODO: change _zkClient.getChildren() to _store.getChildren()
        handleChildChange(childPath, _zkClient.getChildren(childPath));
      }
    }
  }
}
