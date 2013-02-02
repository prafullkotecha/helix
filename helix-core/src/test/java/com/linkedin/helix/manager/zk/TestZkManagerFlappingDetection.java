package com.linkedin.helix.manager.zk;

import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.InstanceType;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZkHelixTestManager;
import com.linkedin.helix.ZkTestHelper;
import com.linkedin.helix.integration.ZkIntegrationTestBase;

public class TestZkManagerFlappingDetection extends ZkIntegrationTestBase
{
  @Test
  public void testDisconnectHistory() throws Exception
  {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    final String clusterName = className + "_" + methodName;

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            10, // partitions per resource
                            5, // number of nodes
                            3, // replicas
                            "MasterSlave",
                            true); // do rebalance
    
    
      String instanceName = "localhost_" + (12918 + 0);
      ZkHelixTestManager manager =
          new ZkHelixTestManager(clusterName,
                                 instanceName,
                                 InstanceType.PARTICIPANT,
                                 ZK_ADDR);
      manager.connect();
      ZkClient zkClient = manager.getZkClient();
      ZkTestHelper.expireSession(zkClient);
      for(int i = 0;i < 4; i++)
      {
        ZkTestHelper.expireSession(zkClient);
        Thread.sleep(1000);
        if(i < 5)
        {
          //System.out.println(i);
          Assert.assertTrue(manager.isConnected());
        }
      }
      ZkTestHelper.disconnectSession(zkClient);
      for(int i = 0; i < 20; i++)
      {
        Thread.sleep(500);
        System.out.println(i);
        if(!manager.isConnected()) break;
      }
      Assert.assertFalse(manager.isConnected());
  }
  
  @Test
  public void testDisconnectFlappingWindow() throws Exception
  {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String instanceName = "localhost_" + (12918 + 1);
    final String clusterName = className + "_" + methodName + UUID.randomUUID();

    testDisconnectFlappingWindow2(instanceName, InstanceType.PARTICIPANT);
    testDisconnectFlappingWindow2("admin", InstanceType.ADMINISTRATOR);
  }
  
  public void testDisconnectFlappingWindow2(String instanceName, InstanceType type) throws Exception
  {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    final String clusterName = className + "_" + methodName + UUID.randomUUID();

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            10, // partitions per resource
                            5, // number of nodes
                            3, // replicas
                            "MasterSlave",
                            true); // do rebalance
    
    
      // flapping time window to 5 sec
      System.setProperty("helixmanager.flappingTimeWindow", "15000");
      System.setProperty("helixmanager.maxDisconnectThreshold", "7");
      ZkHelixTestManager manager2 =
          new ZkHelixTestManager(clusterName,
                                 instanceName,
                                 type,
                                 ZK_ADDR);
      manager2.connect();
      ZkClient zkClient = manager2.getZkClient();
      for(int i = 0;i < 3; i++)
      {
        ZkTestHelper.expireSession(zkClient);
        Thread.sleep(500);
        Assert.assertTrue(manager2.isConnected());
      }
      Thread.sleep(15000);
      // Old entries should be cleaned up
      for(int i = 0;i < 7; i++)
      {
        ZkTestHelper.expireSession(zkClient);
        Thread.sleep(1000);
        Assert.assertTrue(manager2.isConnected());
      }
      ZkTestHelper.disconnectSession(zkClient);
      for(int i = 0; i < 20; i++)
      {
        Thread.sleep(500);
        if(!manager2.isConnected()) break;
      }
      Assert.assertFalse(manager2.isConnected());
  }
  
  //@Test
  public void testDisconnectFlappingWindowController() throws Exception
  {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    final String clusterName = className + "_" + methodName;

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            10, // partitions per resource
                            5, // number of nodes
                            3, // replicas
                            "MasterSlave",
                            true); // do rebalance
    
    
      // flapping time window to 5 sec
      System.setProperty("helixmanager.flappingTimeWindow", "5000");
      System.setProperty("helixmanager.maxDisconnectThreshold", "3");
      ZkHelixTestManager manager2 =
          new ZkHelixTestManager(clusterName,
                                 null,
                                 InstanceType.CONTROLLER,
                                 ZK_ADDR);
      manager2.connect();
      Thread.sleep(100);
      ZkClient zkClient = manager2.getZkClient();
      for(int i = 0;i < 2; i++)
      {
        ZkTestHelper.expireSession(zkClient);
        Thread.sleep(500);
        Assert.assertTrue(manager2.isConnected());
      }
      Thread.sleep(5000);
      // Old entries should be cleaned up
      for(int i = 0;i < 3; i++)
      {
        ZkTestHelper.expireSession(zkClient);
        Thread.sleep(500);
        Assert.assertTrue(manager2.isConnected());
      }
      ZkTestHelper.disconnectSession(zkClient);
      for(int i = 0; i < 20; i++)
      {
        Thread.sleep(500);
        if(!manager2.isConnected()) break;
      }
      Assert.assertFalse(manager2.isConnected());
  }
}
