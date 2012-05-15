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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.exception.ZkBadVersionException;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.store.PropertyListener;
import com.linkedin.helix.store.PropertySerializer;
import com.linkedin.helix.store.PropertyStat;
import com.linkedin.helix.store.PropertyStore;
import com.linkedin.helix.store.PropertyStoreException;


public class ZKPropertyStore<T> implements PropertyStore<T>, ZkListener

{
  private static Logger                           LOG                =
                                                                         Logger.getLogger(ZKPropertyStore.class);

//  private volatile boolean                        _isConnected       = false;
//  private volatile boolean                        _hasSessionExpired = false;

  protected final ZkClient                        _zkClient;
  protected PropertySerializer<T>                 _serializer;
  protected final String                          _root;

  // zookeeperPath -> zkCallback
//  private final Map<String, ZkCallbackHandler<T>> _callbackMap       =
//                                                                         new ConcurrentHashMap<String, ZkCallbackHandler<T>>();
  private final Map<String, Set<PropertyListener<T>>> _callbackMap       =
      new ConcurrentHashMap<String, Set<PropertyListener<T>>>();

  
  final ZkCache                                   _cache;

  public ZKPropertyStore(ZkClient zkClient,
                         final PropertySerializer<T> serializer,
                         String root)
  {
    if (zkClient == null || serializer == null || root == null)
    {
      throw new IllegalArgumentException("zkClient|serializer|root can't be null");
    }

    _root = normalizeKey(root);
    _zkClient = zkClient;
    _zkClient.setZkSerializer(new ByteArraySerializer());

    setPropertySerializer(serializer);

    _zkClient.createPersistent(_root, true);
//    _zkClient.subscribeStateChanges(this);

    _cache = new ZkCache(_root, _zkClient, this);
  }

  // key is normalized if it has exactly 1 leading slash
  private String normalizeKey(String key)
  {
    if (key == null)
    {
      LOG.error("Key can't be null");
      throw new IllegalArgumentException("Key can't be null");
    }

    // strip off leading slash
    while (key.startsWith("/"))
    {
      key = key.substring(1);
    }

    return "/" + key;
  }

  private String getAbsolutePath(String key)
  {
    key = normalizeKey(key);
    if (key.equals("/"))
    {
      return _root;
    }
    else
    {
      return _root + key;
    }
  }

  // return a normalized key
  String getRelativePath(String path)
  {
    if (!path.startsWith(_root))
    {
      String errMsg = path + "does NOT start with property store's root: " + _root;
      LOG.error(errMsg);
      throw new IllegalArgumentException(errMsg);
    }

    if (path.equals(_root))
    {
      return "/";
    }
    else
    {
      return path.substring(_root.length());
    }
  }

  @Override
  public void createPropertyNamespace(String prefix) throws PropertyStoreException
  {
    String path = getAbsolutePath(prefix);
    if (isSubscribed(path))
    {
//      _cache.set(path, null);
    }
    else
    {
      _zkClient.createPersistent(path, true);
    }
  }

  @Override
  public void setProperty(String key, final T value) throws PropertyStoreException
  {
    String path = getAbsolutePath(key);

    // serializer should handle value == null
    byte[] valueBytes = _serializer.serialize(value);
    if (isSubscribed(path))
    {
//      _cache.set(path, valueBytes);
    }
    else
    {
      try
      {
        _zkClient.createPersistent(path, valueBytes);
      }
      catch (ZkNodeExistsException e)
      {
        _zkClient.writeData(path, valueBytes);
      }
    }

  }

  @Override
  public T getProperty(String key) throws PropertyStoreException
  {
    return getProperty(key, null);
  }

  /**
   * Start from key and go up to property store root return true if any parent is
   * subscribed
   * 
   * @param path
   *          : path always starts with _root
   * @return
   */
  boolean isSubscribed(String path)
  {
    while (path.length() >= _root.length())
    {
      if (_callbackMap.containsKey(path))
      {
        return true;
      }
      path = path.substring(0, path.lastIndexOf('/'));
    }
    return false;
  }

  @Override
  public T getProperty(String key, PropertyStat propertyStat) throws PropertyStoreException
  {
    String normalizedKey = normalizeKey(key);
    String path = getAbsolutePath(normalizedKey);
    Stat stat = new Stat();

    byte[] bytes;
    if (isSubscribed(path))
    {
      bytes = (byte[]) _cache.get(path, stat);
    }
    else
    {
      bytes = _zkClient.readDataAndStat(path, stat, true);
    }

    T value = _serializer.deserialize(bytes);

    if (propertyStat != null)
    {
      PropertyStat.copyStat(stat, propertyStat);
    }

    return value;
  }

  @Override
  public void removeProperty(String key) throws PropertyStoreException
  {
    String path = getAbsolutePath(normalizeKey(key));

    try
    {
      if (isSubscribed(path))
      {
//        _cache.remove(path);
      }
      else
      {
        _zkClient.delete(path);
      }
    }
    catch (ZkNoNodeException e)
    {
      // OK
    }
  }

  @Override
  public String getPropertyRootNamespace()
  {
    return _root;
  }

  @Override
  public void removeNamespace(String prefixKey) throws PropertyStoreException
  {
    String path = getAbsolutePath(prefixKey);

    try
    {
      if (isSubscribed(path))
      {
//        _cache.remove(path);
      }
      else
      {
        _zkClient.deleteRecursive(path);
      }
    }
    catch (ZkNoNodeException e)
    {
      // OK
    }
  }

  void getPropertyNamesRecursive(String key, List<String> keys) throws PropertyStoreException
  {
    String path = getAbsolutePath(key);

    try
    {
      List<String> childs = _zkClient.getChildren(path);
      if (childs != null)
      {
        keys.add(key);
        for (String child : childs)
        {
          String childPath = path.equals("/") ? path + child : path + "/" + child;
          getPropertyNamesRecursive(childPath, keys);
        }
      }
    }
    catch (ZkNoNodeException e)
    {
      // OK
    }
  }

  @Override
  public List<String> getPropertyNames(String keyPrefix) throws PropertyStoreException
  {
    String prefixPath = getAbsolutePath(normalizeKey(keyPrefix));
    List<String> keys = new ArrayList<String>();

    if (isSubscribed(prefixPath))
    {
      List<String> paths = null;    // _cache.getKeys(prefixPath);

      // strip off property store root
      for (String path : paths)
      {
        keys.add(getRelativePath(path));
      }
    }
    else
    {
      getPropertyNamesRecursive(keyPrefix, keys);
    }

    // sort it to ensure deterministic order
    if (keys.size() > 1)
    {
      Collections.sort(keys);
    }

    return keys;
  }

  @Override
  public void setPropertyDelimiter(String delimiter) throws PropertyStoreException
  {
    throw new PropertyStoreException("setPropertyDelimiter() not implemented for ZKPropertyStore");
  }

//  void subscribeRecursive(String path, ZkCallbackHandler<T> zkCallback)
//  {
//    // TODO: replace with _store.getChildren()
//    List<String> childs = _zkClient.getChildren(path);
//    if (childs != null)
//    {
//      _zkClient.subscribeDataChanges(path, zkCallback);
//      _zkClient.subscribeChildChanges(path, zkCallback);
//
//      for (String child : childs)
//      {
//        String childPath = path.endsWith("/") ? path + child : path + "/" + child;
//        subscribeRecursive(childPath, zkCallback);
//      }
//    }
//  }

  // put data/child listeners on prefix and all children
  @Override
  public void subscribeForPropertyChange(String prefix,
                                         final PropertyListener<T> listener) throws PropertyStoreException
  {
    if (listener == null)
    {
      throw new IllegalArgumentException("listener can't be null. Prefix: " + prefix);
    }

    String path = getAbsolutePath(prefix);

//    Set<PropertyListener<T>> listenerSet = null;
//    boolean needDoSubscribe = false;
    synchronized (_callbackMap)
    {
      if (!isSubscribed(path))
      {
        LOG.debug("Subscribed changes for prefix: " + path);
        _cache.updateCache(path);
      }
      
      if (!_callbackMap.containsKey(path))
      {
        _callbackMap.put(path, new CopyOnWriteArraySet<PropertyListener<T>>());
//        needDoSubscribe = true;
      }
      Set<PropertyListener<T>> listenerSet = _callbackMap.get(path);
      listenerSet.add(listener);

//      if (needDoSubscribe)
//      {
////        subscribeRecursive(path, callback);
//        LOG.debug("Subscribed changes for prefix: " + path);
//
//        _cache.updateCache(path);
//      }
    }
  }

//  void unsubscribeRecursive(String path, ZkCallbackHandler<T> zkCallback)
//  {
//    // TODO: replace with _store.getChildren()
//    List<String> childs = _zkClient.getChildren(path);
//    if (childs != null)
//    {
//      _zkClient.unsubscribeDataChanges(path, zkCallback);
//      _zkClient.unsubscribeChildChanges(path, zkCallback);
//
//      for (String child : childs)
//      {
//        String childPath = path.endsWith("/") ? path + child : path + "/" + child;
//        unsubscribeRecursive(childPath, zkCallback);
//      }
//    }
//  }

  @Override
  public void unsubscribeForPropertyChange(String prefix,
                                           PropertyListener<T> listener) throws PropertyStoreException
  {
    if (listener == null)
    {
      throw new IllegalArgumentException("listener can't be null. Prefix: " + prefix);
    }

    String path = getAbsolutePath(prefix);
//    Set<PropertyListener<T>> callback = null;
//    boolean needDoUnsubscribe = false;

    synchronized (_callbackMap)
    {
      Set<PropertyListener<T>> listenerSet = _callbackMap.get(path);
      if (listenerSet != null)
      {
//        final Set<PropertyListener<T>> listeners = callback._listeners;
        listenerSet.remove(listener);
        if (listenerSet.isEmpty())
        {
          _callbackMap.remove(path);
//          needDoUnsubscribe = true;
        }

      }

      if (!isSubscribed(path))
      {
//        unsubscribeRecursive(path, callback);
        LOG.debug("Unsubscribed changes for prefix: " + path);

        _cache.purgeCache(path);
      }
    }
  }

  @Override
  public boolean canParentStoreData()
  {
    return false;
  }

  @Override
  public void setPropertySerializer(final PropertySerializer<T> serializer)
  {
    if (serializer == null)
    {
      throw new IllegalArgumentException("serializer can't be null");
    }

    _serializer = serializer;
  }

  @Override
  public void updatePropertyUntilSucceed(String key, DataUpdater<T> updater) throws PropertyStoreException
  {
    updatePropertyUntilSucceed(key, updater, true);
  }

  @Override
  public void updatePropertyUntilSucceed(String key,
                                         DataUpdater<T> updater,
                                         boolean createIfAbsent) throws PropertyStoreException
  {
    String path = getAbsolutePath(key);
    ByteArrayUpdater bytesUpdater = new ByteArrayUpdater(updater, _serializer);

    if (isSubscribed(path))
    {
      // TODO fix it
//      _cache.updateSerialized(path, bytesUpdater, createIfAbsent);
    }
    else
    {
      Stat stat = new Stat();
      boolean retry;
      do
      {
        retry = false;
        try
        {
          byte[] oldData = _zkClient.readData(path, stat);
          byte[] newData = bytesUpdater.update(oldData);
          _zkClient.writeData(path, newData, stat.getVersion());
        }
        catch (ZkBadVersionException e)
        {
          retry = true;
        }
        catch (ZkNoNodeException e)
        {
          if (createIfAbsent)
          {
            retry = true;
            _zkClient.createPersistent(path, true);
          }
          else
          {
            throw e;
          }
        }
      }
      while (retry);
    }
  }

  @Override
  public boolean exists(String key)
  {
    String path = getAbsolutePath(key);
    return _zkClient.exists(path);
  }

//  @Override
//  public void handleStateChanged(KeeperState state) throws Exception
//  {
//    LOG.info("KeeperState:" + state);
//    switch (state)
//    {
//    case SyncConnected:
//      _isConnected = true;
//      break;
//    case Disconnected:
//      _isConnected = false;
//      break;
//    case Expired:
//      _isConnected = false;
//      _hasSessionExpired = true;
//      break;
//    }
//  }
//
//  // TODO move this to ZkCache
//  @Override
//  public void handleNewSession() throws Exception
//  {
//    ZkConnection connection = ((ZkConnection) _zkClient.getConnection());
//    ZooKeeper zookeeper = connection.getZookeeper();
//    LOG.info("handleNewSession: " + zookeeper.getSessionId());
//
//    synchronized (_callbackMap)
//    {
//      for (String path : _callbackMap.keySet())
//      {
//        Set<PropertyListener<T>> listenerSet = _callbackMap.get(path);
//        if (listenerSet == null)
//        {
//          LOG.error("Get a null callback for path: " + path + ". Remove it");
//          _callbackMap.remove(path);
//          continue;
//        }
//
////        subscribeRecursive(path, callback);
//
//        // TODO do initial invocation
//        // callback.handleChildChange(path, _zkClient.getChildren(path));
//        // }
//      }
//    }
//  }

  @Override
  public boolean start()
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean stop()
  {
    // TODO Auto-generated method stub
    return false;
  }

  public static void main(String[] args) throws Exception
  {
    final PropertySerializer<String> serializer = new PropertySerializer<String>()
    {

      @Override
      public byte[] serialize(String data) throws PropertyStoreException
      {
        // TODO Auto-generated method stub
        String str = data;
        if (str == null)
        {
          return null;
        }

        return str.getBytes();
      }

      @Override
      public String deserialize(byte[] bytes) throws PropertyStoreException
      {
        // TODO Auto-generated method stub
        if (bytes == null)
        {
          return null;
        }
        return new String(bytes);
      }
    };
    String root = "/testNewZkPropertyStore";
    ZkClient client = new ZkClient("localhost:2191");
    client.setZkSerializer(new ZkSerializer()
    {

      @Override
      public byte[] serialize(Object data) throws ZkMarshallingError
      {
        // TODO Auto-generated method stub
        String str = (String) data;
        if (str == null)
        {
          return null;
        }

        return str.getBytes();
      }

      @Override
      public Object deserialize(byte[] bytes) throws ZkMarshallingError
      {
        // TODO Auto-generated method stub
        if (bytes == null)
        {
          return null;
        }
        return new String(bytes);
      }
    });

    client.deleteRecursive(root);

    ZKPropertyStore<String> store =
        new ZKPropertyStore<String>(new ZkClient("localhost:2191"), serializer, root);
    store.subscribeForPropertyChange("/", new PropertyListener<String>()
    {
      @Override
      public void onPropertyChange(String key)
      {
        // TODO Auto-generated method stub
        // System.out.println("onPropertyChange: " + key);
      }

      @Override
      public void onPropertyCreate(String key)
      {
        // TODO Auto-generated method stub
        
      }

      @Override
      public void onPropertyDelete(String key)
      {
        // TODO Auto-generated method stub
        
      }
    });

    randomCreateByZkClient(100, root, client);

    // wait for cache being updated by zk callbacks
    Thread.sleep(5000);

    verify(store, client, root);

    randomOpByStore(100, client, store);
    Thread.sleep(5000);

    verify(store, client, root);

    System.out.println("END");
  }

  static ConcurrentLinkedQueue<String> changes = new ConcurrentLinkedQueue<String>();

  static void randomCreateByZkClient(int number, String root, ZkClient client)
  {
    int count = 0;
    int maxDepth = 10;
    String delim = "/";
    while (count < number)
    {
      int depth = ((int) (Math.random() * 10000)) % maxDepth + 1;
      StringBuilder sb = new StringBuilder(root);
      for (int i = 0; i < depth; i++)
      {
        int childId = ((int) (Math.random() * 10000)) % 5;
        sb.append(delim).append("child-" + childId);
      }
      String key = sb.toString();
      String val = key;

      String keyToCreate = key;
      while (keyToCreate.startsWith(root))
      {
        if (client.exists(keyToCreate))
        {
          break;
        }
        changes.add(keyToCreate + "-" + "create" + "-" + System.currentTimeMillis());
        keyToCreate = keyToCreate.substring(0, keyToCreate.lastIndexOf('/'));
      }

      client.createPersistent(key, true);

      System.out.println("Writing key: " + key);
      client.writeData(key, val);
      count = count + 1;
      changes.add(key + "-" + "write" + "-" + System.currentTimeMillis());
    }
  }

  static void randomOpByStore(int number, ZkClient client, ZKPropertyStore<String> store) throws Exception
  {
    int count = 0;
    int newWrites = 0;
    int updates = 0;
    int deletion = 0;
    int maxDepth = 10;
    String delim = "/";
    while (count < number)
    {
      int depth = ((int) (Math.random() * 10000)) % maxDepth + 1;
      StringBuilder sb = new StringBuilder(store.getPropertyRootNamespace());
      for (int i = 0; i < depth; i++)
      {
        int childId = ((int) (Math.random() * 10000)) % 5;
        sb.append(delim).append("child-" + childId);
      }
      String key = sb.toString();
      String storeKey = store.getRelativePath(key);
      String val = key;
      if (!client.exists(key))
      {
        store.setProperty(storeKey, val);
        newWrites++;
      }
      else
      {
        int op = ((int) (Math.random() * 10000)) % 2;
        if (op == 0)
        {
          System.out.println("Deleting key:" + key);

          // Map<String, String> toDelete = new HashMap<String, String>();
          // read(toDelete, client, key);
          // for (String child : toDelete.keySet()) {
          // changes.add(child + "-" + "delete" + "-"
          // + System.currentTimeMillis());
          // }

          store.removeProperty(storeKey);
          deletion++;
        }
        else
        {
          System.out.println("Updating key:" + key);
          Object data = client.readData(key);

          String object = (data != null ? data.toString() : key) + "-updated";
          store.setProperty(storeKey, object);
          // changes.add(key + "-" + "write" + "-"
          // + System.currentTimeMillis());
          updates++;
        }
      }
      count++;
    }
    System.out.println("newWrites:" + newWrites + " updates:" + updates + " deletions:"
        + deletion);
  }

  static void verify(ZKPropertyStore<String> store, ZkClient client, String root) throws Exception
  {
    Map<String, ZNode> zkMap = new HashMap<String, ZNode>();
    ZkCache cache = store._cache;

    read(zkMap, client, root);
    System.out.println("actual size: " + zkMap.size() + ", cached size: "
        + cache._map.size());

    if (cache._map.size() != zkMap.size())
    {
      throw new Exception("size not same. actual: " + zkMap.size() + ", cache: "
          + cache._map.size());
    }

    // verify cache
    for (String key : zkMap.keySet())
    {
      String actual = (String) (zkMap.get(key)._data);
      Stat actualStat = zkMap.get(key).getStat();

      Stat cachedStat = new Stat();
      byte[] byteArray = (byte[]) cache.get(key, cachedStat);
      String cached = byteArray == null ? null : new String(byteArray);

      // verify value
      if (actual == null)
      {
        if (cached != null)
        {
          throw new Exception(key + " not equal value. actual: " + actual + ", cached: "
              + cached);
        }
      }
      else
      {
        if (!actual.equals(cached))
        {
          throw new Exception(key + " not equal value. actual: " + actual + ", cached: "
              + cached);
        }
      }

      // verify stat
      if (!actualStat.equals(cachedStat))
      {
        // TODO: there is a bug of not updating root's stat
        printChangesFor(key);
        throw new Exception(key + " not equal stat. actual: " + actualStat + ", cached: "
            + cachedStat);
      }

      // verify childs
      Set<String> actualChilds = zkMap.get(key)._childSet;
      Set<String> cachedChilds = cache._map.get(key)._childSet;
      if (!actualChilds.equals(cachedChilds))
      {
        printChangesFor(key);

        throw new Exception(key + " childs not equal. actualChilds: " + actualChilds
            + ", cachedChilds: " + cachedChilds);
      }
    }

    // verify store.get()
    for (String key : zkMap.keySet())
    {
      String actual = (String) (zkMap.get(key)._data);
      String storeKey = store.getRelativePath(key);
      String storeGet = store.getProperty(storeKey);

      // verify value
      if (actual == null)
      {
        if (storeGet != null)
        {
          throw new Exception(key + " not equal. actual: " + actual + ", storeGet: "
              + storeGet);
        }
      }
      else
      {
        if (!actual.equals(storeGet))
        {
          throw new Exception(key + " not equal. actual: " + actual + ", storeGet: "
              + storeGet);
        }
      }

      // verify store getPropertyNames()
      List<String> actualKeys = getKeys(zkMap, key);
      if (actualKeys.size() > 1)
      {
        Collections.sort(actualKeys);
      }

      String storeKeyPrefix = store.getRelativePath(key);
      List<String> storeKeys = store.getPropertyNames(storeKeyPrefix);
      List<String> storeKeys2 = new ArrayList<String>();
      // prepend with root for comparing
      for (String storeKey2 : storeKeys)
      {
        if (storeKey2.equals("/"))
        {
          storeKeys2.add(store.getPropertyRootNamespace());
        }
        else
        {
          storeKeys2.add(store.getPropertyRootNamespace() + storeKey2);
        }
      }
      if (!actualKeys.equals(storeKeys2))
      {
        printChangesFor(key);

        throw new Exception(key + " keys not equal. actualKeys: " + actualKeys
            + ", storeKeys: " + storeKeys);
      }

    }
  }

  static List<String> getKeys(Map<String, ZNode> zkMap, String pathPrefix)
  {
    List<String> paths = new ArrayList<String>();
    for (String path : zkMap.keySet())
    {
      if (path.startsWith(pathPrefix))
      {
        paths.add(path);
      }
    }
    return paths;
  }

  static void read(Map<String, ZNode> map, ZkClient client, String root)
  {
    List<String> childs = client.getChildren(root);
    if (childs != null)
    {
      Stat stat = new Stat();
      String value = client.readData(root, stat);
      ZNode node = new ZNode(root, value, stat);
      node._childSet.addAll(childs);
      map.put(root, node);

      for (String child : childs)
      {
        String childPath = root + "/" + child;
        read(map, client, childPath);
      }
    }

  }

  private static void printChangesFor(String child)
  {
    System.out.println("START:Changes detected for child:" + child);
    int id = 0;
    for (String entry : changes)
    {
      if (entry.startsWith(child + "-"))
      {
        System.out.println(id + ": " + entry);
      }
      id++;
    }

    System.out.println("END:Changes detected for child:" + child);
  }

  @Override
  public void handleDataChange(String path)
  {
    while (path.length() >= _root.length())
    {
      if (_callbackMap.containsKey(path))
      {
        Set<PropertyListener<T>> listenerSet = _callbackMap.get(path);
        String key = getRelativePath(path);
        for (PropertyListener<T> listener : listenerSet)
        {
          listener.onPropertyChange(key);
        }
      }
      path = path.substring(0, path.lastIndexOf('/'));
    }
  }

  @Override
  public void handleNodeCreate(String path)
  {
    while (path.length() >= _root.length())
    {
      if (_callbackMap.containsKey(path))
      {
        Set<PropertyListener<T>> listenerSet = _callbackMap.get(path);
        String key = getRelativePath(path);
        for (PropertyListener<T> listener : listenerSet)
        {
          listener.onPropertyCreate(key);
        }
      }
      path = path.substring(0, path.lastIndexOf('/'));
    }
  }

  @Override
  public void handleNodeDelete(String path)
  {
    while (path.length() >= _root.length())
    {
      if (_callbackMap.containsKey(path))
      {
        Set<PropertyListener<T>> listenerSet = _callbackMap.get(path);
        String key = getRelativePath(path);
        for (PropertyListener<T> listener : listenerSet)
        {
          listener.onPropertyDelete(key);
        }
      }
      path = path.substring(0, path.lastIndexOf('/'));
    }
  }
}
