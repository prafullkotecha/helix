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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkBadVersionException;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.store.PropertyChangeListener;
import com.linkedin.helix.store.PropertySerializer;
import com.linkedin.helix.store.PropertyStat;
import com.linkedin.helix.store.PropertyStore;
import com.linkedin.helix.store.PropertyStoreException;

public class ZKPropertyStore<T> implements PropertyStore<T>, IZkStateListener // IZkDataListener,
// IZkChildListener,
{
  private static Logger LOG = Logger.getLogger(ZKPropertyStore.class);

  static class ByteArraySerializer implements ZkSerializer
  {
    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError
    {
//      if (data == null)
//      {
//        return null;
//      }
//      else
//      {
//        return ((ByteArray) data)._bytes;
//      }
      return (byte[]) data;
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError
    {
//      if (bytes == null)
//      {
//        return null;
//      }
//      else
//      {
//        return new ByteArray(bytes);
//      }
      return bytes;
    }

  };

  class ByteArrayUpdater implements DataUpdater<byte[]>
  {
    final DataUpdater<T>        _updater;
    final PropertySerializer<T> _serializer;

    ByteArrayUpdater(DataUpdater<T> updater, PropertySerializer<T> serializer)
    {
      _updater = updater;
      _serializer = serializer;
    }

    @Override
    public byte[] update(byte[] current)
    {
      T currentValue = null;
      try
      {
        if (current != null)
        {
          currentValue = _serializer.deserialize(current);
        }
        T updateValue = _updater.update(currentValue);
        return _serializer.serialize(updateValue);
      }
      catch (PropertyStoreException e)
      {
        LOG.error("Exception in update " + currentValue + ". Updater: " + _updater, e);
      }
      return null;
    }
  }

  private volatile boolean                        _isConnected       = false;
  private volatile boolean                        _hasSessionExpired = false;

  protected final ZkClient                        _zkClient;
  protected PropertySerializer<T>                 _serializer;
  protected final String                          _root;

  // zookeeperPath -> zkCallback
  private final Map<String, ZkCallbackHandler<T>> _callbackMap       =
                                                                         new ConcurrentHashMap<String, ZkCallbackHandler<T>>();
  // private final Map<String, Map<PropertyChangeListener<T>, ZkCallbackHandler<T>>>
  // _callbackMap
  // = new HashMap<String, Map<PropertyChangeListener<T>, ZkCallbackHandler<T>>>();

  // TODO cache capacity should be bounded
  // private final Map<String, PropertyItem> _cache = new ConcurrentHashMap<String,
  // PropertyItem>();
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
    _zkClient.subscribeStateChanges(this);

    _cache = new ZkCache(_root, _zkClient);
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

  // always a return normalized key
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
    try
    {
      // if (!_zkClient.exists(path))
      // {
      // _zkClient.createPersistent(path, true);
      // }
      if (isSubscribed(path))
      {
        _cache.set(path, null);
      }
      else
      {
        _zkClient.createPersistent(path, true);
      }
    }
    catch (Exception e)
    {
      LOG.error("Exception in createPropertyNamespace(" + prefix + ")", e);
      throw new PropertyStoreException(e.toString());
    }
  }

  @Override
  public void setProperty(String key, final T value) throws PropertyStoreException
  {
    String path = getAbsolutePath(key);

    try
    {
      // if (!_zkClient.exists(path))
      // {
      // _zkClient.createPersistent(path, true);
      // }

      // serializer should handle value == null
      byte[] valueBytes = _serializer.serialize(value);
      // _zkClient.writeData(path, new ByteArray(valueBytes));
      if (isSubscribed(path))
      {
        _cache.set(path, valueBytes);
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

      // update cache
      // getProperty(key);

    }
    catch (Exception e)
    {
      LOG.error("Exception when setProperty(" + key + ", " + value + ")", e);
      throw new PropertyStoreException(e.toString());
    }
  }

  @Override
  public T getProperty(String key) throws PropertyStoreException
  {
    return getProperty(key, null);
  }

  // bytes and stat are not null
  private T getValueAndStat(byte[] bytes, Stat stat, PropertyStat propertyStat) throws PropertyStoreException
  {
    T value = _serializer.deserialize(bytes);

    if (propertyStat != null)
    {
      propertyStat.setLastModifiedTime(stat.getMtime());
      propertyStat.setVersion(stat.getVersion());
    }
    return value;
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
    // String path = getAbsolutePath(key);
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

    T value = null;
    try
    {
      byte[] bytes;
      if (isSubscribed(path))
      {
        bytes = (byte[]) _cache.get(path);

      }
      else
      {
        bytes = _zkClient.readDataAndStat(path, stat, true);
      }

      if (bytes != null)
      {
        value = getValueAndStat(bytes, stat, propertyStat);
      }

      // if (_cache.containsKey(normalizedKey))
      // {
      // // cache hit
      // stat = _zkClient.getStat(path);
      // if (stat != null)
      // {
      // PropertyItem item = _cache.get(normalizedKey);
      // if (item.getVersion() < stat.getVersion())
      // {
      // // stale data in cache
      // ByteArray bytes = _zkClient.readDataAndStat(path, stat, true);
      // if (bytes != null)
      // {
      // value = getValueAndStat(bytes.getBytes(), stat, propertyStat);
      // _cache.put(normalizedKey, new PropertyItem(bytes.getBytes(), stat));
      // } else
      // {
      // _cache.remove(normalizedKey);
      // }
      //
      // } else
      // {
      // // valid data in cache
      // // item.getBytes() should not be null
      // value = getValueAndStat(item.getBytes(), stat, propertyStat);
      // }
      // } else
      // {
      // // stat == null means the znode doesn't exist
      // _cache.remove(normalizedKey);
      // }
      // } else
      // {
      // // cache miss
      // ByteArray bytes = _zkClient.readDataAndStat(path, stat, true);
      // if (bytes != null)
      // {
      // value = getValueAndStat(bytes.getBytes(), stat, propertyStat);
      // _cache.put(normalizedKey, new PropertyItem(bytes.getBytes(), stat));
      // }
      // }

      return value;
    }
    catch (Exception e)
    {
      LOG.error("Exception in getProperty(" + key + ")", e);
      throw (new PropertyStoreException(e.toString()));
    }
  }

  @Override
  public void removeProperty(String key) throws PropertyStoreException
  {
    String normalizedKey = normalizeKey(key);
    String path = getAbsolutePath(normalizedKey);

    try
    {
      // if (_zkClient.exists(path))
      // {
      // _zkClient.delete(path);
      // }
      // _cache.remove(normalizedKey);
      if (isSubscribed(path))
      {
        _cache.remove(path);
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
    catch (Exception e)
    {
      LOG.error("Exception in removeProperty(" + key + ")", e);
      throw (new PropertyStoreException(e.toString()));
    }
  }

  @Override
  public String getPropertyRootNamespace()
  {
    return _root;
  }

  @Override
  public void removeNamespace(String prefix) throws PropertyStoreException
  {
    String path = getAbsolutePath(prefix);

    try
    {
      // if (_zkClient.exists(path))
      // {
      // _zkClient.deleteRecursive(path);
      // }
      //
      // // update cache
      // // childs are all normalized keys
      // List<String> childs = getPropertyNames(prefix);
      // for (String child : childs)
      // {
      // _cache.remove(child);
      // }
      if (isSubscribed(path))
      {
        _cache.remove(path);
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
    catch (Exception e)
    {
      LOG.error("Exception in removeProperty(" + prefix + ")", e);
      throw (new PropertyStoreException(e.toString()));
    }
  }

  // prefix is always normalized
  void getPropertyNamesRecursive(String key, List<String> keys) throws PropertyStoreException
  {
    String path = getAbsolutePath(key);

    // if (!_zkClient.exists(path))
    // {
    // return;
    // }

    try
    {
      List<String> childs = _zkClient.getChildren(path);
      // if (childs == null)
      // {
      // return;
      // }

      // if (childs.size() == 0)
      // {
      // // add leaf node to cache
      // // getProperty(prefix);
      // leafNodes.add(prefix);
      // return;
      // }

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
    keyPrefix = normalizeKey(keyPrefix);
    String prefixPath = getAbsolutePath(keyPrefix);
    List<String> keys = new ArrayList<String>();
    
    if (isSubscribed(prefixPath))
    {
      List<String> paths = _cache.getKeys(prefixPath);
      
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

    // sort it to get deterministic order
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

  void subscribeRecursive(String path, ZkCallbackHandler<T> zkCallback)
  {
    // TODO: replace with _store.getChildren()
    List<String> childs = _zkClient.getChildren(path);
    if (childs != null)
    {
      _zkClient.subscribeDataChanges(path, zkCallback);
      _zkClient.subscribeChildChanges(path, zkCallback);

      for (String child : childs)
      {
        String childPath = path.endsWith("/") ? path + child : path + "/" + child;
        subscribeRecursive(childPath, zkCallback);
      }
    }
  }

  // put data/child listeners on prefix and all childs
  @Override
  public void subscribeForPropertyChange(String prefix,
                                         final PropertyChangeListener<T> listener) throws PropertyStoreException
  {
    if (listener == null)
    {
      throw new IllegalArgumentException("listener can't be null. Prefix: " + prefix);
    }

    String path = getAbsolutePath(prefix);

    ZkCallbackHandler<T> callback = null;
    boolean needDoSubscribe = false;
    synchronized (_callbackMap)
    {
      // Map<PropertyChangeListener<T>, ZkCallbackHandler<T>> callbacks;
      if (!_callbackMap.containsKey(path))
      {
        _callbackMap.put(path, new ZkCallbackHandler<T>(_zkClient, this, prefix));
        needDoSubscribe = true;
      }
      callback = _callbackMap.get(path);
      callback.addListener(listener);
      // if (!callbacks.containsKey(listener))
      // {
      // callback = new ZkCallbackHandler<T>(_zkClient, this, prefix, listener);
      // callbacks.put(listener, callback);
      // }

      if (needDoSubscribe)
      {
        subscribeRecursive(path, callback);
        LOG.debug("Subscribed changes for prefix: " + path);

        _cache.updateCache(path);
      }
    }

    // try
    // {
    // if (needDoSubscribe)
    // {
    // // a newly added callback
    // // _zkClient.subscribeDataChanges(path, callback);
    // // _zkClient.subscribeChildChanges(path, callback);
    // subscribeRecursive(path, callback);
    //
    // // TODO: do we need to fire initial invocations?
    // // callback.handleChildChange(path, _zkClient.getChildren(path));
    //
    // LOG.debug("Subscribed changes for prefix: " + path);
    // }
    // } catch (Exception e)
    // {
    // LOG.error("Exception in subscribeForPropertyChange(" + prefix + ")", e);
    // throw (new PropertyStoreException(e.toString()));
    // }
  }

  // // prefix is always a normalized key
  // void unsubscribeRecursive(String prefix, ZkCallbackHandler<T> callback)
  // {
  // String path = getAbsolutePath(prefix);
  //
  // _zkClient.unsubscribeDataChanges(path, callback);
  // _zkClient.unsubscribeChildChanges(path, callback);
  //
  // List<String> childs = _zkClient.getChildren(path);
  // if (childs == null || childs.size() == 0)
  // {
  // return;
  // }
  //
  // for (String child : childs)
  // {
  // doUnsubscribeForPropertyChange(prefix + "/" + child, callback);
  // }
  // }

  void unsubscribeRecursive(String path, ZkCallbackHandler<T> zkCallback)
  {
    // TODO: replace with _store.getChildren()
    List<String> childs = _zkClient.getChildren(path);
    if (childs != null)
    {
      _zkClient.unsubscribeDataChanges(path, zkCallback);
      _zkClient.unsubscribeChildChanges(path, zkCallback);

      for (String child : childs)
      {
        String childPath = path.endsWith("/") ? path + child : path + "/" + child;
        unsubscribeRecursive(childPath, zkCallback);
      }
    }
  }

  @Override
  public void unsubscribeForPropertyChange(String prefix,
                                           PropertyChangeListener<T> listener) throws PropertyStoreException
  {
    if (listener == null)
    {
      throw new IllegalArgumentException("listener can't be null. Prefix: " + prefix);
    }

    String path = getAbsolutePath(prefix);
    ZkCallbackHandler<T> callback = null;
    boolean needDoUnsubscribe = false;

    synchronized (_callbackMap)
    {
      callback = _callbackMap.get(path);
      if (callback != null)
      {
        final Set<PropertyChangeListener<T>> listeners = callback._listeners;
        listeners.remove(listener);
        if (listeners.isEmpty())
        {
          _callbackMap.remove(path);
          needDoUnsubscribe = true;
        }

      }
      // if (_callbackMap.containsKey(path))
      // {
      // Map<PropertyChangeListener<T>, ZkCallbackHandler<T>> callbacks =
      // _callbackMap.get(path);
      // callback = callbacks.remove(listener);
      //
      // if (callbacks == null || callbacks.isEmpty())
      // {
      // _callbackMap.remove(path);
      // }
      // }

      if (needDoUnsubscribe)
      {
        unsubscribeRecursive(path, callback);
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
    try
    {
      if (!_zkClient.exists(path))
      {
        if (!createIfAbsent)
        {
          throw new PropertyStoreException("Can't update " + key
              + " since no node exists");
        }
        else
        {
          _zkClient.createPersistent(path, true);
        }
      }

      _zkClient.updateDataSerialized(path, new ByteArrayUpdater(updater, _serializer));
    }
    catch (Exception e)
    {
      LOG.error("Exception in updatePropertyUntilSucceed(" + key + ", " + createIfAbsent
          + ")", e);
      throw (new PropertyStoreException(e.toString()));
    }

    // update cache
    // getProperty(key);
  }

  @Override
  public boolean compareAndSet(String key, T expected, T update, Comparator<T> comparator)
  {
    return compareAndSet(key, expected, update, comparator, true);
  }

  @Override
  public boolean compareAndSet(String key,
                               T expected,
                               T update,
                               Comparator<T> comparator,
                               boolean createIfAbsent)
  {
    String path = getAbsolutePath(key);

    // if two threads call with createIfAbsent=true
    // one thread creates the node, the other just goes through
    // when wirteData() one thread writes the other gets ZkBadVersionException
    if (!_zkClient.exists(path))
    {
      if (createIfAbsent)
      {
        _zkClient.createPersistent(path, true);
      }
      else
      {
        return false;
      }
    }

    try
    {
      Stat stat = new Stat();
      byte[] currentBytes = _zkClient.readDataAndStat(path, stat, true);
      T current = null;
      if (currentBytes != null)
      {
        current = _serializer.deserialize(currentBytes);
      }

      if (comparator.compare(current, expected) == 0)
      {
        byte[] valueBytes = _serializer.serialize(update);
        _zkClient.writeData(path, valueBytes, stat.getVersion());

        // update cache
        // getProperty(key);

        return true;
      }
    }
    catch (ZkBadVersionException e)
    {
      LOG.warn("Get BadVersion when writing to zookeeper. Mostly Ignorable due to contention");
    }
    catch (Exception e)
    {
      LOG.error("Exception when compareAndSet(" + key + ")", e);
    }

    return false;
  }

  @Override
  public boolean exists(String key)
  {
    String path = getAbsolutePath(key);
    return _zkClient.exists(path);
  }

  @Override
  public void handleStateChanged(KeeperState state) throws Exception
  {
    LOG.info("KeeperState:" + state);
    switch (state)
    {
    case SyncConnected:
      _isConnected = true;
      break;
    case Disconnected:
      _isConnected = false;
      break;
    case Expired:
      _isConnected = false;
      _hasSessionExpired = true;
      break;
    }
  }

  @Override
  public void handleNewSession() throws Exception
  {
    ZkConnection connection = ((ZkConnection) _zkClient.getConnection());
    ZooKeeper zookeeper = connection.getZookeeper();
    LOG.info("handleNewSession: " + zookeeper.getSessionId());

    synchronized (_callbackMap)
    {
      for (String path : _callbackMap.keySet())
      {
        // Map<PropertyChangeListener<T>, ZkCallbackHandler<T>> callbacks =
        // _callbackMap.get(path);
        ZkCallbackHandler<T> callback = _callbackMap.get(path);
        if (callback == null)
        {
          LOG.error("Get a null callback for path: " + path + ". Remove it");
          _callbackMap.remove(path);
          continue;
        }

        subscribeRecursive(path, callback);

        // for (PropertyChangeListener<T> listener : callbacks.keySet())
        // {
        // ZkCallbackHandler<T> callback = callbacks.get(listener);
        //
        // if (callback == null)
        // {
        // LOG.error("Get a null callback. Remove it. Path: " + path + ", listener: " +
        // listener);
        // callbacks.remove(listener);
        // continue;
        // }
        // _zkClient.subscribeDataChanges(path, callback);
        // _zkClient.subscribeChildChanges(path, callback);

        // do initial invocation
        // callback.handleChildChange(path, _zkClient.getChildren(path));
        // }
      }
    }
  }

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
    store.subscribeForPropertyChange("/", new PropertyChangeListener<String>()
    {
      @Override
      public void onPropertyChange(String key)
      {
        // TODO Auto-generated method stub
        // System.out.println("onPropertyChange: " + key);
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
      byte[] byteArray = (byte[]) cache.get(key);
      String cached = byteArray == null ? null : new String(byteArray);

      // verify value
      if (actual == null)
      {
        if (cached != null)
        {
          throw new Exception(key + " not equal. actual: " + actual + ", cached: "
              + cached);
        }
      }
      else
      {
        if (!actual.equals(cached))
        {
          throw new Exception(key + " not equal. actual: " + actual + ", cached: "
              + cached);
        }
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
      
      // verify childs
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
        } else
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
      String value = client.readData(root);
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
}
