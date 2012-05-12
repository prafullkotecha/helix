package com.linkedin.helix.store.zk;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkServer;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.util.ZKClientPool;

public class ZkCache implements IZkChildListener, IZkDataListener
{
  String                                     _rootPath;
  ZkClient                                   _client;
  ConcurrentMap<String, ZNode>               _map    =
                                                         new ConcurrentHashMap<String, ZNode>();
  ReadWriteLock                              _lock   = new ReentrantReadWriteLock();
  // final static List<String> changes = new ArrayList<String>();
  final static ConcurrentLinkedQueue<String> changes =
                                                         new ConcurrentLinkedQueue<String>();

  public ZkCache(String path, ZkClient client)
  {
    super();
    _rootPath = path;
    _client = client;
    // init();
  }

  private void init()
  {
    updateCache(_rootPath);
  }

  boolean purgeCache(String path)
  {
    try
    {
      _lock.writeLock().lock();
      changes.add(path + "-" + "cachepurge" + "-" + System.currentTimeMillis());
      List<String> children;
      try
      {
        children = _client.getChildren(path);
      }
      catch (ZkNoNodeException e)
      {
        return true;
      }

      for (String subPath : children)
      {
        if (!purgeCache(path + "/" + subPath))
        {
          return false;
        }
      }

      // boolean succeed = _client.delete(key);
      _map.remove(path);
      String parent = path.substring(0, path.lastIndexOf('/'));

      ZNode zNode = _map.get(parent);
      if (zNode != null)
      {
        String name = path.substring(path.lastIndexOf('/') + 1);
        System.out.println("Removing child:" + name + " from child set of parent:"
            + parent);
        zNode._childSet.remove(name);
      }

      return true;
    }
    finally
    {
      _lock.writeLock().unlock();
    }

  }

  void updateCache(String parent)
  {
    // System.out.println("Updating cache:" + parent);
    try
    {
      Stat stat = new Stat();
      _lock.writeLock().lock();
      changes.add(parent + "-" + "cacheupdate" + "-" + System.currentTimeMillis());
      _client.subscribeChildChanges(parent, this);
      _client.subscribeDataChanges(parent, this);
      Object readData = _client.readData(parent, stat);
      _map.putIfAbsent(parent, new ZNode(parent, readData, stat));
      ZNode parentNode = _map.get(parent);
      List<String> children = _client.getChildren(parent);
      for (String child : children)
      {
        String childPath = parent + "/" + child;
        if (!parentNode.hasChild(child))
        {
          parentNode.addChild(child);
          updateCache(childPath);
        }
        // updateCache(childPath);
      }
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  @Override
  public void handleDataChange(String dataPath, Object data) throws Exception
  {
    System.out.println("Handle handleDataChange:" + dataPath);
    // TODO: sync on path
    try
    {
      _lock.writeLock().lock();
      changes.add(dataPath + "-" + "datachange" + "-" + data + "-"
          + System.currentTimeMillis());
//      if (dataPath.equals(_rootPath))
//      {
//        System.out.println("here: " + data);
//      }

      ZNode zNode = _map.get(dataPath);
      if (zNode != null)
      {
        // TODO: optimize it
        Stat stat = new Stat();
        Object readData = _client.readData(dataPath, stat);
        zNode.setData(readData);
        zNode._stat = stat;
      }
      else
      {
        // debug
        // this is because we see dataChange on child before childChange
        // on parent
        System.out.println("null in handleDataChange");
        printChangesFor(dataPath);
        String parent = dataPath.substring(0, dataPath.lastIndexOf('/'));
        printChangesFor(parent);

      }
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  @Override
  public void handleDataDeleted(String dataPath) throws Exception
  {
    System.out.println("Handle handleDataDeleted:" + dataPath);
    // TODO: sync on path
    try
    {
      _lock.writeLock().lock();
      changes.add(dataPath + "-" + "datadeleted" + "-" + System.currentTimeMillis());
      // if (dataPath.equals(_rootPath)) {
      // return;
      // }
      _map.remove(dataPath);
      _client.unsubscribeChildChanges(dataPath, this);
      _client.unsubscribeDataChanges(dataPath, this);
      String parent = dataPath.substring(0, dataPath.lastIndexOf('/'));

      ZNode zNode = _map.get(parent);
      if (zNode != null)
      {
        String name = dataPath.substring(dataPath.lastIndexOf('/') + 1);
        System.out.println("Removing child:" + name + " from child set of parent:"
            + parent);
        zNode._childSet.remove(name);
      }
      else
      {
        // this is because we see child dataDeleted change before parent
        // dataDeleted change
        System.out.println("null in handleDataDelete");

        printChangesFor(dataPath);
        printChangesFor(parent);

      }
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  @Override
  public void handleChildChange(String parentPath, List<String> currentChildsOnZk) throws Exception
  {
    System.out.println("Handle handleChildChange:" + parentPath);
    if (currentChildsOnZk == null)
    {
      return;
    }
    changes.add(parentPath + "-" + "childchange" + "-" + currentChildsOnZk + "-"
        + System.currentTimeMillis());
    ZNode zNode = _map.get(parentPath);
    if (zNode == null)
    {
      // no subscription available
      return;
    }
    for (String child : currentChildsOnZk)
    {
      if (!zNode._childSet.contains(child))
      {
        zNode.addChild(child);
        updateCache(parentPath + "/" + child);
      }
    }
  }

  void createRecursive(String key)
  {
    try
    {
      _client.subscribeChildChanges(key, this);
      _client.subscribeDataChanges(key, this);
      _client.create(key, null, CreateMode.PERSISTENT);
      
      // TODO: optimize it
      Stat stat = new Stat();
      Object readData = _client.readData(key, stat);

      _map.putIfAbsent(key, new ZNode(key, readData, stat));

      String parentDir = key.substring(0, key.lastIndexOf('/'));
      ZNode parent = _map.get(parentDir);
      if (parent != null)
      {
        parent.addChild(key.substring(key.lastIndexOf('/') + 1));
      }
    }
    catch (ZkNodeExistsException e)
    {
      // OK
    }
    catch (ZkNoNodeException e)
    {
      String parentDir = key.substring(0, key.lastIndexOf('/'));
      createRecursive(parentDir);
      createRecursive(key);
    }

  }

  public boolean set(String key, Object data)
  {
    // TODO sync on key
    // TODO create whole parent
    // TODO check if there is a subscription
    try
    {
      _lock.writeLock().lock();
      System.out.println("Writing key: " + key);
      createRecursive(key);
//      if (data != null)
//      {
        _client.writeData(key, data);
        Stat stat = _client.getStat(key); // TODO: may get a stat later than that for write()
        
//      }
      ZNode zNode = _map.get(key);
      if (zNode != null)
      {
        zNode.setData(data);
      }
      else
      {
        // String name = key.substring(key.lastIndexOf('/'));
        _map.put(key, new ZNode(key, data, stat));
      }
      return true;
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  public Object get(String key)
  {
    try
    {
      _lock.readLock().lock();
      ZNode zNode = _map.get(key);
      if (zNode == null)
      {
        return null;
      }
      return zNode._data;
    }
    finally
    {
      _lock.readLock().unlock();
    }
  }

  void getKeysRecursive(String path, List<String> keys)
  {
    ZNode node = _map.get(path);
    if (node != null)
    {
      keys.add(path);
      Set<String> childs = node._childSet;
      for (String child : childs)
      {
        String childPath = path.equals("/") ? path + child : path + "/" + child;
        getKeysRecursive(childPath, keys);
      }

    }
  }

  public List<String> getKeys(String path)
  {
    List<String> keys = new ArrayList<String>();
    try
    {
      _lock.readLock().lock();
      getKeysRecursive(path, keys);

      return keys;
    }
    finally
    {
      _lock.readLock().unlock();
    }
  }

  boolean removeRecursive(String key)
  {
    List<String> children;
    try
    {
      children = _client.getChildren(key);
    }
    catch (ZkNoNodeException e)
    {
      return true;
    }

    for (String subPath : children)
    {
      if (!removeRecursive(key + "/" + subPath))
      {
        return false;
      }
    }

    boolean succeed = _client.delete(key);
    _map.remove(key);
    String parent = key.substring(0, key.lastIndexOf('/'));

    ZNode zNode = _map.get(parent);
    if (zNode != null)
    {
      String name = key.substring(key.lastIndexOf('/') + 1);
      System.out.println("Removing child:" + name + " from child set of parent:" + parent);
      zNode._childSet.remove(name);
    }

    return succeed;
  }

  public boolean remove(String key)
  {
    // TODO: add recursive
    // TODO: is it ok to rely on callback to unsubscribe
    // _client.delete(key);
    // _map.remove(key);
    // _client.unsubscribeChildChanges(key, this);
    // _client.unsubscribeDataChanges(key, this);
    try
    {
      _lock.writeLock().lock();

      // do unsubscribe in the dataDelete callback
      removeRecursive(key);
    }
    finally
    {
      _lock.writeLock().unlock();
    }
    return true;
  }

//  public void updateSerialized(String path, DataUpdater<Object> updater)
//  {
//    Stat stat = new Stat();
//    boolean retry;
//    
//    Object oldData = null;
//    ZNode node = _map.get(path);
//    if (node != null)
//    {
//      oldData = node._data;
//    }
//    do
//    {
//      retry = false;
//      try
//      {
//        // do optimistic update
//        Object newData = updater.update(oldData);
//        _client.wr
//        T oldData = (T) readData(path, stat);
//        T newData = updater.update(oldData);
//        writeData(path, newData, stat.getVersion());
//      }
//      catch (ZkBadVersionException e)
//      {
//        retry = true;
//      }
//    }
//    while (retry);
//  }

  public static void main(String[] args) throws Exception
  {
    final String rootNamespace = "/testZkCache";
    String zkAddress = "localhost:2191";
    // ZkServer server = startZkSever(zkAddress, rootNamespace);
    ZkClient client = new ZkClient(zkAddress);
    client.deleteRecursive(rootNamespace);

    int count = 0;
    int maxDepth = 10;
    String delim = "/";
    // Map<String, String> map = new HashMap<String, String>();
    while (count < 100)
    {
      int depth = ((int) (Math.random() * 10000)) % maxDepth + 1;
      StringBuilder sb = new StringBuilder(rootNamespace);
      for (int i = 0; i < depth; i++)
      {
        int childId = ((int) (Math.random() * 10000)) % 5;
        sb.append(delim).append("child-" + childId);
      }
      String key = sb.toString();
      String val = key;

      String keyToCreate = key;
      while (keyToCreate.startsWith(rootNamespace))
      {
        if (client.exists(keyToCreate))
        {
          break;
        }
        changes.add(keyToCreate + "-" + "create" + "-" + System.currentTimeMillis());
        keyToCreate = keyToCreate.substring(0, keyToCreate.lastIndexOf('/'));
      }

      client.createPersistent(key, true);

      System.out.println("Writing key:" + key);
      client.writeData(key, val);
      count = count + 1;
      // addOp(map, key, val, "write");
      changes.add(key + "-" + "write" + "-" + System.currentTimeMillis());
    }

    ZkCache cache = new ZkCache(rootNamespace, client);
    cache.init();
    verify(cache, client, rootNamespace);
    System.out.println("init verification done. changes: " + changes.size());

    // for (String child : map.keySet()) {
    // System.out.println("Verifiying:" + child);
    // Object actual = cache.get(child);
    // String expected = map.get(child);
    // if (expected != null) {
    // boolean equals = expected.equals(actual);
    // if (!equals) {
    // throw new Exception("Exepected:" + expected + ","
    // + "Actual:" + actual);
    // }
    // } else {
    // if (actual != null) {
    // throw new Exception("Exepected:" + expected + ","
    // + "Actual:" + actual);
    // }
    // }
    // }

    count = 0;
    int newWrites = 0;
    int updates = 0;
    int deletion = 0;
    while (count < 100)
    {
      int depth = ((int) (Math.random() * 10000)) % maxDepth + 1;
      StringBuilder sb = new StringBuilder(rootNamespace);
      for (int i = 0; i < depth; i++)
      {
        int childId = ((int) (Math.random() * 10000)) % 5;
        sb.append(delim).append("child-" + childId);
      }
      String key = sb.toString();
      String val = key;
      if (!client.exists(key))
      {
        String keyToCreate = key;
        while (keyToCreate.startsWith(rootNamespace))
        {
          if (client.exists(keyToCreate))
          {
            break;
          }
          changes.add(keyToCreate + "-" + "create" + "-" + System.currentTimeMillis());
          keyToCreate = keyToCreate.substring(0, keyToCreate.lastIndexOf('/'));
        }

        client.createPersistent(key, true);

        System.out.println("Writing key:" + key);
        client.writeData(key, val);
        changes.add(key + "-" + "write" + "-" + System.currentTimeMillis());

        newWrites++;
      }
      else
      {
        int op = ((int) (Math.random() * 10000)) % 2;
        if (op == 0)
        {
          System.out.println("Deleting key:" + key);

          Map<String, ZNode> toDelete = new HashMap<String, ZNode>();
          read(toDelete, client, key);
          for (String child : toDelete.keySet())
          {
            changes.add(child + "-" + "delete" + "-" + System.currentTimeMillis());
          }

          client.deleteRecursive(key);
          deletion++;
        }
        else
        {
          System.out.println("Updating key:" + key);
          Object data = client.readData(key);

          String object = (data != null ? data.toString() : key) + "-updated";
          client.writeData(key, object);
          // addOp(map, key, object, "update");
          changes.add(key + "-" + "write" + "-" + System.currentTimeMillis());
          updates++;
        }
      }
      count = count + 1;
    }

    System.out.println("newWrites:" + newWrites + " updates:" + updates + " deletions:"
        + deletion);
    Thread.sleep(5000);

    // for (String child : map.keySet()) {
    // System.out.println("Verifiying:" + child);
    // Object actual = cache.get(child);
    // String expected = map.get(child);
    // if (expected == null) {
    // if (actual != null) {
    // cache.printChangesFor(child);
    // throw new Exception(
    // "key is not null in cache event after it is deleted"
    // + child);
    // }
    // } else if (expected.equals(DELETED)) {
    // if (actual != null) {
    // throw new Exception(
    // "key is not null in cache event after it is deleted"
    // + child);
    // }
    // } else {
    // boolean equals = expected.equals(actual);
    // if (!equals) {
    // cache.printChangesFor(child);
    // throw new Exception("Expected:" + expected + ","
    // + "Actual:" + actual);
    //
    // }
    // }
    // }

    verify(cache, client, rootNamespace);

    count = 0;
    newWrites = 0;
    updates = 0;
    deletion = 0;
    while (count < 100)
    {
      int depth = ((int) (Math.random() * 10000)) % maxDepth + 1;
      StringBuilder sb = new StringBuilder(rootNamespace);
      for (int i = 0; i < depth; i++)
      {
        int childId = ((int) (Math.random() * 10000)) % 5;
        sb.append(delim).append("child-" + childId);
      }
      String key = sb.toString();
      String val = key;
      if (!client.exists(key))
      {
        cache.set(key, val);
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

          cache.remove(key);
          deletion++;
        }
        else
        {
          System.out.println("Updating key:" + key);
          Object data = client.readData(key);

          String object = (data != null ? data.toString() : key) + "-updated";
          cache.set(key, object);
          // changes.add(key + "-" + "write" + "-"
          // + System.currentTimeMillis());
          updates++;
        }
      }
      count++;
    }

    System.out.println("newWrites:" + newWrites + " updates:" + updates + " deletions:"
        + deletion);
    Thread.sleep(5000);

    verify(cache, client, rootNamespace);

    System.out.println("Verification passed");
    client.close();
    // stopZkServer(server);
  }

  // private static void addOp(Map<String, String> map, String key,
  // String object, String op) {
  // // cache.addClientOp(key+"-"+op);
  // changes.add(key + "-" + op);
  // map.put(key, object);
  // }

  // private void addClientOp(String string)
  // {
  // changes.add(string);
  // }

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

  static void verify(ZkCache cache, ZkClient client, String root) throws Exception
  {
    Map<String, ZNode> zkMap = new HashMap<String, ZNode>();

    read(zkMap, client, root);
    System.out.println("actual size: " + zkMap.size() + ", cached size: "
        + cache._map.size());
    if (cache._map.size() != zkMap.size())
    {
      throw new Exception("size not same. actual: " + zkMap.size() + ", cache: "
          + cache._map.size());
    }
    for (String key : zkMap.keySet())
    {
      String actual = (String) (zkMap.get(key)._data);
      String cached = (String) cache.get(key);

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

  // move from TestHelper
  static public ZkServer startZkSever(final String zkAddress, final String rootNamespace) throws Exception
  {
    List<String> rootNamespaces = new ArrayList<String>();
    rootNamespaces.add(rootNamespace);
    return startZkSever(zkAddress, rootNamespaces);
  }

  static public ZkServer startZkSever(final String zkAddress,
                                      final List<String> rootNamespaces) throws Exception
  {
    System.out.println("Start zookeeper at " + zkAddress + " in thread "
        + Thread.currentThread().getName());

    String zkDir = zkAddress.replace(':', '_');
    final String logDir = "/tmp/" + zkDir + "/logs";
    final String dataDir = "/tmp/" + zkDir + "/dataDir";
    FileUtils.deleteDirectory(new File(dataDir));
    FileUtils.deleteDirectory(new File(logDir));
    ZKClientPool.reset();

    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace()
    {
      @Override
      public void createDefaultNameSpace(org.I0Itec.zkclient.ZkClient zkClient)
      {
        for (String rootNamespace : rootNamespaces)
        {
          try
          {
            zkClient.deleteRecursive(rootNamespace);
          }
          catch (Exception e)
          {
            System.err.println("fail to deleteRecursive path:" + rootNamespace
                + "\nexception:" + e);
          }
        }
      }
    };

    int port = Integer.parseInt(zkAddress.substring(zkAddress.lastIndexOf(':') + 1));
    ZkServer zkServer = new ZkServer(dataDir, logDir, defaultNameSpace, port);
    zkServer.start();

    return zkServer;
  }

  static public void stopZkServer(ZkServer zkServer)
  {
    if (zkServer != null)
    {
      zkServer.shutdown();
      System.out.println("Shut down zookeeper at port " + zkServer.getPort()
          + " in thread " + Thread.currentThread().getName());
    }
  }
}
