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

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArraySet;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.IZkConnection;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
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
      LOG.info("getStat. path: " + path + ", time: " + (end - start));
    }

  }

  @Override
  public <T extends Object> void updateDataSerialized(String path, DataUpdater<T> updater)
  {
    long start = System.currentTimeMillis();
    try
    {
      super.updateDataSerialized(path, updater);
    }
    finally
    {
      long end = System.currentTimeMillis();
      LOG.info("updateDataSerialized. path: " + path + ", time: " + (end - start));
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
  public void writeData(String path, Object object) 
  {
    long start = System.currentTimeMillis();
    try
    {
      super.writeData(path, object);
    }
    finally
    {
      long end = System.currentTimeMillis();
      LOG.info("write. path: " + path + ", time: " + (end - start));
    }
    
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

  @Override
  public void createPersistent(String path, boolean createParents) throws ZkInterruptedException,
      IllegalArgumentException,
      ZkException,
      RuntimeException
  {
    long start = System.currentTimeMillis();
    try
    {
      super.createPersistent(path, createParents);
    }
    finally
    {
      long end = System.currentTimeMillis();
      LOG.info("createPersistent. path: " + path + ", time: " + (end - start));
    }

  }

  @Override
  public <T extends Object> T readData(String path, boolean returnNullIfPathNotExists) 
  {
    long start = System.currentTimeMillis();
    try
    {
      return super.readData(path, returnNullIfPathNotExists);
    }
    finally
    {
      long end = System.currentTimeMillis();
      LOG.info("readData. path: " + path + ", time: " + (end - start));
    }
  }

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
      data = (T) super.readData(path, stat);
    }
    catch (ZkNoNodeException e)
    {
      if (!returnNullIfPathNotExists)
      {
        throw e;
      }
    } finally
    {
      long end = System.currentTimeMillis();
      LOG.info("readDataAndStat. path: " + path + ", time: " + (end - start));
    }
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
    long start = System.currentTimeMillis();
    try
    {
      Stat stat = getStat(path);
      this.asyncWriteData(path,
                          datat,
                          stat.getVersion(),
                          new LogStatCallback("asyncSetData"),
                          null);
    } finally
    {
      long end = System.currentTimeMillis();
      LOG.info("asyncWriteData. path: " + path + ", time: " + (end - start));
    }
  }

  public void asyncWriteData(final String path,
                             Object datat,
                             final int version,
                             final StatCallback cb,
                             final Object ctx)
  {
    final byte[] data = _zkSerializer.serialize(datat);
    ((ZkConnection) _connection).getZookeeper().setData(path, data, version, cb, ctx);

  }

  /**
   * Retry asyncCreate()
   * 
   * @param path
   * @param ctx
   */
  void retryAsyncCreate(String path, AsyncContext ctx)
  {
    ((ZkConnection) _connection).getZookeeper()
                                .create(path,
                                        ctx._data,
                                        Arrays.asList(DEFAULT_ACL),
                                        ctx._mode,
                                        new AsyncCreateCallback("asyncCreate", this),
                                        ctx);
  }

  /**
   * AsyncCreate with a retry up to 3 times
   * 
   * @param path
   * @param datat
   */
  public void asyncCreate(final String path, Object datat, CreateMode mode)
  {
    long start = System.currentTimeMillis();
    try
    {
      final byte[] data = _zkSerializer.serialize(datat);
      ((ZkConnection) _connection).getZookeeper()
                                  .create(path,
                                          data,
                                          Arrays.asList(DEFAULT_ACL),
                                          mode,
                                          new AsyncCreateCallback("asyncCreate", this),
                                          new AsyncContext(data, mode, ASYNC_RETRY_LIMIT));
    } finally
    {
      long end = System.currentTimeMillis();
      LOG.info("asyncCreate. path: " + path + ", time: " + (end - start));
    }
  }

  public void asyncCreate(final String path,
                          Object datat,
                          List<ACL> acl,
                          CreateMode createMode,
                          StringCallback cb,
                          Object ctx)
  {
    final byte[] data = _zkSerializer.serialize(datat);
    ((ZkConnection) _connection).getZookeeper().create(path,
                                                       data,
                                                       acl,
                                                       createMode,
                                                       cb,
                                                       ctx);
  }

}
