package com.linkedin.clustermanager;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.controller.ClusterManagerMain;
import com.linkedin.clustermanager.mock.storage.DummyProcess.DummyLeaderStandbyStateModelFactory;
import com.linkedin.clustermanager.mock.storage.DummyProcess.DummyOnlineOfflineStateModelFactory;
import com.linkedin.clustermanager.mock.storage.DummyProcess.DummyStateModelFactory;
import com.linkedin.clustermanager.model.Message.MessageType;
import com.linkedin.clustermanager.participant.StateMachineEngine;
import com.linkedin.clustermanager.tools.ClusterSetup;
import com.linkedin.clustermanager.util.ZKClientPool;

public class TestHelper
{
  private static final Logger logger = Logger.getLogger(TestHelper.class);

  static public ZkServer startZkSever(final String zkAddress) throws Exception
  {
    List<String> empty = Collections.emptyList();
    return TestHelper.startZkSever(zkAddress, empty);
  }

  static public ZkServer startZkSever(final String zkAddress, final String rootNamespace) throws Exception
  {
    List<String> rootNamespaces = new ArrayList<String>();
    rootNamespaces.add(rootNamespace);
    return TestHelper.startZkSever(zkAddress, rootNamespaces);
  }

  static public ZkServer startZkSever(final String zkAddress, final List<String> rootNamespaces) throws Exception
  {
    System.out.println("Starting zookeeper at " + zkAddress + " in thread "+ Thread.currentThread().getName());
    
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
            logger.error("fail to deleteRecursive path:" + rootNamespace + "\nexception:" + e);
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
      System.out.println("Shutting down ZK at port " + zkServer.getPort() 
                         + " in thread " + Thread.currentThread().getName());
    }
  }
  
  public static void setupCluster(String clusterName, String ZkAddr, int startPort,
      String participantNamePrefix, String resourceNamePrefix, int resourceNb, int partitionNb,
      int nodesNb, int replica, String stateModelDef, boolean doRebalance) throws Exception
  {
    ZkClient zkClient = new ZkClient(ZkAddr);
    zkClient.setZkSerializer(new ZNRecordSerializer());

    try
    {
      if (zkClient.exists("/" + clusterName))
      {
        logger.warn("Cluster already exists:" + clusterName + ". Deleting it");
        zkClient.deleteRecursive("/" + clusterName);
      }
  
      ClusterSetup setupTool = new ClusterSetup(ZkAddr);
      setupTool.addCluster(clusterName, true);
  
      for (int i = 0; i < nodesNb; i++)
      {
        int port = startPort + i;
        setupTool.addInstanceToCluster(clusterName, participantNamePrefix + ":" + port);
      }
  
      for (int i = 0; i < resourceNb; i++)
      {
        String dbName = resourceNamePrefix + i;
        setupTool.addResourceGroupToCluster(clusterName, dbName, partitionNb, stateModelDef);
        if (doRebalance)
        {
          setupTool.rebalanceStorageCluster(clusterName, dbName, replica);
        }
      }
    } finally
    {
      zkClient.close();
    }
  }


  /**
   * start dummy cluster participant with a pre-created zkClient for testing session
   * expiry
   * 
   * @param zkAddr
   * @param clusterName
   * @param instanceName
   * @param zkClient
   * @return
   * @throws Exception
   */
  public static StartCMResult startDummyProcess(final String zkAddr,
                                                     final String clusterName,
                                                     final String instanceName,
                                                     final ZkClient zkClient) throws Exception
  {
    StartCMResult result = new StartCMResult();
    ClusterManager manager = null;
    manager =
        ClusterManagerFactory.getZKBasedManagerForParticipant(clusterName,
                                                              instanceName,
                                                              zkAddr,
                                                              zkClient);
    result._manager = manager;
    Thread thread = new Thread(new DummyProcessThread(manager, instanceName));
    result._thread = thread;
    thread.start();

    return result;
  }

  /**
   * start cluster controller with a pre-created zkClient for testing session expiry
   * 
   * @param clusterName
   * @param controllerName
   * @param zkConnectString
   * @param zkClient
   * @return
 * @throws Exception 
   */
  public static StartCMResult startClusterController(final String clusterName,
                                              final String controllerName,
                                              final String zkConnectString,
                                              final String controllerMode,
                                              final ZkClient zkClient) throws Exception
  {
    final StartCMResult result = new StartCMResult();
    final ClusterManager manager =
        ClusterManagerMain.startClusterManagerMain(zkConnectString,
                                                   clusterName,
                                                   controllerName,
                                                   controllerMode,
                                                   zkClient);
    manager.connect();
    result._manager = manager;
    
    Thread thread = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        // ClusterManager manager = null;

        try
        {
         
          Thread.currentThread().join();
        }
        catch (InterruptedException e)
        {
          String msg =
              "controller:" + controllerName + ", " + Thread.currentThread().getName()
                  + " interrupted";
          logger.info(msg);
          // System.err.println(msg);

        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
        /*
        finally
        {
          if (manager != null)
          {
            manager.disconnect();
          }
        }
        */
      }
    });

    thread.start();
    result._thread = thread;
    return result;
  }
  
  public static class StartCMResult
  {
    public Thread _thread;
    public ClusterManager _manager;
    
  }

  static class DummyProcessThread implements Runnable
  {
    ClusterManager _manager;
    String _instanceName;

    public DummyProcessThread(ClusterManager manager, String instanceName)
    {
      _manager = manager;
      _instanceName = instanceName;
    }

    @Override
    public void run()
    {
      try
      {
        _manager.connect();
        DummyStateModelFactory stateModelFactory = new DummyStateModelFactory(0);
        StateMachineEngine genericStateMachineHandler =
            new StateMachineEngine();
        genericStateMachineHandler.registerStateModelFactory("MasterSlave", stateModelFactory);

        DummyLeaderStandbyStateModelFactory stateModelFactory1 = new DummyLeaderStandbyStateModelFactory(10);
        DummyOnlineOfflineStateModelFactory stateModelFactory2 = new DummyOnlineOfflineStateModelFactory(10);
        genericStateMachineHandler.registerStateModelFactory("LeaderStandby", stateModelFactory1);
        genericStateMachineHandler.registerStateModelFactory("OnlineOffline", stateModelFactory2);
        _manager.getMessagingService()
                .registerMessageHandlerFactory(MessageType.STATE_TRANSITION.toString(),
                                               genericStateMachineHandler);

        Thread.currentThread().join();
      }
      catch (InterruptedException e)
      {
        String msg =
            "participant:" + _instanceName + ", " + Thread.currentThread().getName()
                + " interrupted";
        logger.info(msg);
        // System.err.println(msg);

      }
      catch (Exception e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      /*
      finally
      {
        if (_manager != null)
        {
          _manager.disconnect();
        }
      }
      */
    }
  }
  
  public static void setupEmptyCluster(ZkClient zkClient, String clusterName)
  {
    String path = "/" + clusterName;
    zkClient.createPersistent(path);
    zkClient.createPersistent(path + "/" + PropertyType.STATEMODELDEFS.toString());
    zkClient.createPersistent(path + "/" + PropertyType.INSTANCES.toString());
    zkClient.createPersistent(path + "/" + PropertyType.CONFIGS.toString());
    zkClient.createPersistent(path + "/" + PropertyType.IDEALSTATES.toString());
    zkClient.createPersistent(path + "/" + PropertyType.EXTERNALVIEW.toString());
    zkClient.createPersistent(path + "/" + PropertyType.LIVEINSTANCES.toString());
    zkClient.createPersistent(path + "/" + PropertyType.CONTROLLER.toString());

    path = path + "/" + PropertyType.CONTROLLER.toString();
    zkClient.createPersistent(path + "/" + PropertyType.MESSAGES.toString());
    zkClient.createPersistent(path + "/" + PropertyType.HISTORY.toString());
    zkClient.createPersistent(path + "/" + PropertyType.ERRORS.toString());
    zkClient.createPersistent(path + "/" + PropertyType.STATUSUPDATES.toString());
  }
  
  public static <T> Map<String, T> startThreadsConcurrently(final int nrThreads,
      final Callable<T> method, final long timeout)
  {
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch finishCounter = new CountDownLatch(nrThreads);
    final Map<String, T> resultsMap = new ConcurrentHashMap<String, T>();
    final List<Thread> threadList = new ArrayList<Thread>();

    for (int i = 0; i < nrThreads; i++)
    {
      Thread thread = new Thread() {
        @Override
        public void run()
        {
          try
          {
            boolean isTimeout = !startLatch.await(timeout, TimeUnit.SECONDS);
            if (isTimeout)
            {
              logger.error("Timeout while waiting for start latch");
            }
          } catch (InterruptedException ex)
          {
            logger.error("Interrupted while waiting for start latch");
          }

          try
          {
            T result = method.call();
            if (result != null)
            {
              resultsMap.put("thread_" + this.getId(), result);
            }
            logger.debug("result=" + result);
          } catch (Exception e)
          {
            logger.error("Exeption in executing " + method.getClass().getName(), e);
          }

          finishCounter.countDown();
        }
      };
      threadList.add(thread);
      thread.start();
    }
    startLatch.countDown();

    // wait for all thread to complete
    try
    {
      boolean isTimeout = !finishCounter.await(timeout, TimeUnit.SECONDS);
      if (isTimeout)
      {
        logger.error("Timeout while waiting for finish latch. Interrupt all threads");
        for (Thread thread : threadList)
        {
          thread.interrupt();
        }
      }
    } catch (InterruptedException e)
    {
      logger.error("Interrupted while waiting for finish latch", e);
    }

    return resultsMap;
  }
}
