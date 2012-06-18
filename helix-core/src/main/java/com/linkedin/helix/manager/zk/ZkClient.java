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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.I0Itec.zkclient.IZkConnection;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;

/**
 * ZKClient does not provide some functionalities, this will be used for quick fixes if
 * any bug found in ZKClient or if we need additional features but can't wait for the new
 * ZkClient jar Ideally we should commit the changes we do here to ZKClient.
 * 
 * @author kgopalak
 * 
 */
public class ZkClient extends org.I0Itec.zkclient.ZkClient
{
  private static Logger                   LOG                        =
                                                                         Logger.getLogger(ZkClient.class);
  private static final ACL                DEFAULT_ACL                =
                                                                         new ACL(31,
                                                                                 new Id("world",
                                                                                        "anyone"));
  private static final int                ASYNC_RETRY_LIMIT          = 3;

  public static final int                 DEFAULT_CONNECTION_TIMEOUT = 10000;

  public static String                    sessionId;
  public static String                    sessionPassword;

  // TODO need to remove when connection expired
  private static final Set<IZkConnection> zkConnections              =
                                                                         new CopyOnWriteArraySet<IZkConnection>();
  private ZkSerializer                    _zkSerializer;

  class AsyncContext
  {
    final byte[]     _data;
    final CreateMode _mode;
    int              _retry;

    public AsyncContext(byte[] data, CreateMode mode, int retry)
    {
      _data = data;
      _mode = mode;
      _retry = retry;
    }
  }

  class AsyncReadCallback implements DataCallback
  {
    AtomicBoolean success = new AtomicBoolean(false);
    byte[]        _data;
    Stat          _stat;

    public byte[] getData()
    {
      return _data;
    }

    public Stat getStat()
    {
      return _stat;
    }

    public boolean waitForSuccess()
    {
      try
      {
        while (!success.get())
        {
          synchronized (success)
          {
            success.wait();
          }
        }
      }
      catch (InterruptedException e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      return true;
    }

    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)
    {
      synchronized (success)
      {
        if (rc != 0)
        {
          success.set(false);
        }
        else
        {
          success.set(true);
        }
        success.notify();
      }

      _data = data;
      _stat = stat;
    }

  }

  /**
   * Default callback for asyncCreate() with a retry upto 3 times
   */
  class AsyncCreateCallback implements StringCallback
  {
    final ZkClient _zkClient;
    final String   _asyncMethodName;

    public AsyncCreateCallback(String asyncMethodName, ZkClient zkClient)
    {
      _asyncMethodName = asyncMethodName;
      _zkClient = zkClient;
    }

    @Override
    public void processResult(int rc, String path, Object ctx, String name)
    {
      if (rc == 0)
      {
        LOG.info("succeed in async " + _asyncMethodName + ". rc: " + rc + ", path: "
            + path + ", name: " + name);
      }
      else
      {
        AsyncContext context = (AsyncContext) ctx;
        context._retry--;
        if (context._retry < 0)
        {
          LOG.error("fail in async " + _asyncMethodName + ". rc: " + rc + ", path: "
              + path + ", name: " + name);
        }
        else
        {
          _zkClient.retryAsyncCreate(path, context);
        }
      }
    }
  }

  public ZkClient(IZkConnection connection,
                  int connectionTimeout,
                  ZkSerializer zkSerializer)
  {
    super(connection, connectionTimeout, zkSerializer);
    _zkSerializer = zkSerializer;
    zkConnections.add(_connection);
  }

  public ZkClient(IZkConnection connection, int connectionTimeout)
  {
    this(connection, connectionTimeout, new SerializableSerializer());
  }

  public ZkClient(IZkConnection connection)
  {
    this(connection, Integer.MAX_VALUE, new SerializableSerializer());
  }

  public ZkClient(String zkServers,
                  int sessionTimeout,
                  int connectionTimeout,
                  ZkSerializer zkSerializer)
  {
    this(new ZkConnection(zkServers, sessionTimeout), connectionTimeout, zkSerializer);
  }

  public ZkClient(String zkServers, int sessionTimeout, int connectionTimeout)
  {
    this(new ZkConnection(zkServers, sessionTimeout),
         connectionTimeout,
         new SerializableSerializer());
  }

  public ZkClient(String zkServers, int connectionTimeout)
  {
    this(new ZkConnection(zkServers), connectionTimeout, new SerializableSerializer());
  }

  public ZkClient(String zkServers)
  {
    this(new ZkConnection(zkServers), Integer.MAX_VALUE, new SerializableSerializer());
  }

  @Override
  public void setZkSerializer(ZkSerializer zkSerializer)
  {
    super.setZkSerializer(zkSerializer);
    _zkSerializer = zkSerializer;
  }

  public IZkConnection getConnection()
  {
    return _connection;
  }

  @Override
  public void close() throws ZkInterruptedException
  {
    zkConnections.remove(_connection);
    super.close();
  }

  public static int getNumberOfConnections()
  {
    return zkConnections.size();
  }

  public Stat getStat(final String path)
  {
    long start = System.currentTimeMillis();

    try
    {
      Stat stat = retryUntilConnected(new Callable<Stat>()
      {

        @Override
        public Stat call() throws Exception
        {
          Stat stat = ((ZkConnection) _connection).getZookeeper().exists(path, false);
          return stat;
        }
      });

      return stat;
    }
    finally
    {
      long end = System.currentTimeMillis();
      LOG.info("exists. path: " + path + ", time: " + (end - start));
    }

  }

  @Override
  public <T extends Object> T readData(String path, Stat stat)
  {
    long start = System.currentTimeMillis();
    try
    {
      return super.<T> readData(path, stat);
    }
    finally
    {
      long end = System.currentTimeMillis();
      LOG.info("getData. path: " + path + ", time: " + (end - start));
    }
  }

  // public <T extends Object> List<T> asyncReadChildData(final String parentPath, Watcher
  // watcher,
  // DataCallback cb, Object ctx)
  public <T extends Object> List<T> asyncReadChildData(final String parentPath,
                                                       Watcher watcher,
                                                       Object ctx,
                                                       List<Stat> stats)
  {
    long start = System.currentTimeMillis();
    List<String> children = getChildren(parentPath);
    try
    {

      // List<String> children = getChildren(parentPath);
      if (children == null || children.size() == 0)
      {
        return Collections.emptyList();
      }

      List<T> childRecords = new ArrayList<T>();
      List<AsyncReadCallback> cbList = new ArrayList<AsyncReadCallback>(children.size());
      for (String child : children)
      {
        String childPath = parentPath + "/" + child;
        AsyncReadCallback cb = new AsyncReadCallback();
        cbList.add(cb);
        ((ZkConnection) _connection).getZookeeper().getData(childPath, watcher, cb, ctx);
      }

      for (AsyncReadCallback cb : cbList)
      {
        cb.waitForSuccess();
        if(!cb.success.get()){
          LOG.info("Async cb failed");
          continue;
        }
        byte[] data = cb.getData();
        Stat stat = cb.getStat();

        if (data != null && stat != null)
        {
          T datat = (T) _zkSerializer.deserialize(data);
          childRecords.add(datat);
          if (stats != null)
          {
            stats.add(stat);
          }
        }
      }

      return childRecords;
    }
    finally
    {
      long end = System.currentTimeMillis();
      LOG.info("getData. path: " + parentPath + ", time: " + (end - start));
    }
  }

  @Override
  public String create(final String path, Object data, final CreateMode mode) throws ZkInterruptedException,
      IllegalArgumentException,
      ZkException,
      RuntimeException
  {
    long start = System.currentTimeMillis();
    try
    {
      return super.create(path, data, mode);
    }
    finally
    {
      long end = System.currentTimeMillis();
      LOG.info("create. path: " + path + ", time: " + (end - start));
    }
  }

  @Override
  public void writeData(final String path, Object datat, final int expectedVersion)
  {
    Stat stat;
    long start = System.currentTimeMillis();
    try
    {
      super.writeData(path, datat, expectedVersion);
    }
    finally
    {
      long end = System.currentTimeMillis();
      LOG.info("setData. path: " + path + ", time: " + (end - start));
    }
  }

  public Stat writeData2(final String path, Object datat, final int expectedVersion) throws NoNodeException
  {
    Stat stat = null;
    long start = System.currentTimeMillis();
    try
    {
      byte[] bytes = _zkSerializer.serialize(datat);
      stat = ((ZkConnection) _connection).getZookeeper().setData(path, bytes, expectedVersion);
      return stat;
    }
    catch (NoNodeException e)
    {
      throw e;
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    finally
    {
      long end = System.currentTimeMillis();
      LOG.info("setData. path: " + path + ", time: " + (end - start));
    }
    return stat;
  }

  @Override
  public boolean exists(final String path)
  {
    long start = System.currentTimeMillis();
    try
    {
      return super.exists(path);
    }
    finally
    {
      long end = System.currentTimeMillis();
      LOG.info("exists. path: " + path + ", time: " + (end - start));
    }
  }

  // @Override
  // public void createPersistent(String path, boolean createParents) throws
  // ZkInterruptedException,
  // IllegalArgumentException,
  // ZkException,
  // RuntimeException
  // {
  // long start = System.currentTimeMillis();
  // try
  // {
  // super.createPersistent(path, createParents);
  // }
  // finally
  // {
  // long end = System.currentTimeMillis();
  // LOG.info("create. path: " + path + ", time: " + (end - start));
  // }
  //
  // }

  // @Override
  // public <T extends Object> T readData(String path, boolean returnNullIfPathNotExists)
  // {
  // long start = System.currentTimeMillis();
  // try
  // {
  // return super.<T> readData(path, returnNullIfPathNotExists);
  // }
  // finally
  // {
  // long end = System.currentTimeMillis();
  // LOG.info("zk-readData. path: " + path + ", time: " + (end - start));
  // }
  // }

  @Override
  public boolean delete(final String path)
  {
    long start = System.currentTimeMillis();
    try
    {
      return super.delete(path);
    }
    finally
    {
      long end = System.currentTimeMillis();
      LOG.info("delete. path: " + path + ", time: " + (end - start));
    }

  }

  @Override
  public List<String> getChildren(String path)
  {
    long start = System.currentTimeMillis();
    try
    {
      return super.getChildren(path);
    }
    finally
    {
      long end = System.currentTimeMillis();
      LOG.info("getChildren. path: " + path + ", time: " + (end - start));
    }

  }

  @SuppressWarnings("unchecked")
  public <T extends Object> T readDataAndStat(String path,
                                              Stat stat,
                                              boolean returnNullIfPathNotExists)
  {
    long start = System.currentTimeMillis();

    T data = null;
    try
    {
      data = (T) this.readData(path, stat);
    }
    catch (ZkNoNodeException e)
    {
      if (!returnNullIfPathNotExists)
      {
        throw e;
      }
    }
    // finally
    // {
    // long end = System.currentTimeMillis();
    // // if (path.indexOf("EXTERNALVIEW") != -1)
    // // {
    // // System.out.println("ZkClient.readDataAndStat()");
    // // }
    //
    // LOG.info("zk-readData. path: " + path + ", time: " + (end - start));
    // }
    return data;
  }

  public String getServers()
  {
    return _connection.getServers();
  }

  class LogStatCallback implements StatCallback
  {
    final String _asyncMethodName;

    public LogStatCallback(String asyncMethodName)
    {
      _asyncMethodName = asyncMethodName;
    }

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat)
    {
      if (rc == 0)
      {
        LOG.info("succeed in async " + _asyncMethodName + ". rc: " + rc + ", path: "
            + path + ", stat: " + stat);
      }
      else
      {
        LOG.error("fail in async " + _asyncMethodName + ". rc: " + rc + ", path: " + path
            + ", stat: " + stat);
      }
    }

  }

  public void asyncWriteData(final String path, Object datat)
  {
    // long start = System.currentTimeMillis();
    // try
    // {
    Stat stat = getStat(path);
    this.asyncWriteData(path,
                        datat,
                        (stat!=null)?stat.getVersion():-1,
                        new LogStatCallback("asyncSetData"),
                        null);
    // }
    // finally
    // {
    // long end = System.currentTimeMillis();
    // LOG.info("asyncWriteData. path: " + path + ", time: " + (end - start));
    // }
  }

  public void asyncWriteData(final String path,
                             Object datat,
                             final int version,
                             final StatCallback cb,
                             final Object ctx)
  {
    long start = System.currentTimeMillis();
    try
    {
      final byte[] data = _zkSerializer.serialize(datat);
      ((ZkConnection) _connection).getZookeeper().setData(path, data, version, cb, ctx);
    }
    finally
    {
      long end = System.currentTimeMillis();
      LOG.info("asyncSetData. path: " + path + ", time: " + (end - start));
    }

  }

  /**
   * Retry asyncCreate()
   * 
   * @param path
   * @param ctx
   */
  void retryAsyncCreate(String path, AsyncContext ctx)
  {
    long start = System.currentTimeMillis();
    try
    {

      ((ZkConnection) _connection).getZookeeper()
                                  .create(path,
                                          ctx._data,
                                          Arrays.asList(DEFAULT_ACL),
                                          ctx._mode,
                                          new AsyncCreateCallback("asyncCreate", this),
                                          ctx);
    }
    finally
    {
      long end = System.currentTimeMillis();
      LOG.info("asyncCreate. path: " + path + ", time: " + (end - start));
    }
  }

  public void asyncDelete(String path)
  {
    long start = System.currentTimeMillis();
    try
    {
      Stat stat = getStat(path);
      ((ZkConnection) _connection).getZookeeper().delete(path,
                                                         stat.getVersion(),
                                                         new VoidCallback()
                                                         {
                                                           @Override
                                                           public void processResult(int rc,
                                                                                     String path,
                                                                                     Object ctx)
                                                           {
                                                             // LOG.info("deleted " +
                                                             // path);
                                                           }
                                                         },
                                                         null);
    }
    finally
    {
      long end = System.currentTimeMillis();
      LOG.info("asyncDelete. path: " + path + ", time: " + (end - start));
    }

  }

  /**
   * AsyncCreate with a retry up to 3 times
   * 
   * @param path
   * @param datat
   */
  public void asyncCreate(final String path, Object datat, CreateMode mode)
  {
    // long start = System.currentTimeMillis();
    // try
    // {
    final byte[] data = _zkSerializer.serialize(datat);

    this.retryAsyncCreate(path, new AsyncContext(data, mode, ASYNC_RETRY_LIMIT));

    // ((ZkConnection) _connection).getZookeeper()
    // .create(path,
    // data,
    // Arrays.asList(DEFAULT_ACL),
    // mode,
    // new AsyncCreateCallback("asyncCreate", this),
    // new AsyncContext(data, mode, ASYNC_RETRY_LIMIT));
    // }
    // finally
    // {
    // long end = System.currentTimeMillis();
    // LOG.info("asyncCreate. path: " + path + ", time: " + (end - start));
    // }
  }

  public void asyncCreate(final String path,
                          Object datat,
                          List<ACL> acl,
                          CreateMode createMode,
                          StringCallback cb,
                          Object ctx)
  {
    long start = System.currentTimeMillis();

    final byte[] data = _zkSerializer.serialize(datat);
    try
    {

      ((ZkConnection) _connection).getZookeeper().create(path,
                                                         data,
                                                         acl,
                                                         createMode,
                                                         cb,
                                                         ctx);
    }
    finally
    {
      long end = System.currentTimeMillis();
      LOG.info("asyncCreate. path: " + path + ", time: " + (end - start));
    }
  }

}
