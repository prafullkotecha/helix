package com.linkedin.helix.integration;

import java.util.Date;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZKHelixDataAccessor;
import com.linkedin.helix.manager.zk.ZkBaseDataAccessor;
import com.linkedin.helix.mock.controller.ClusterController;
import com.linkedin.helix.mock.storage.MockParticipant;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.tools.ClusterStateVerifier;
import com.linkedin.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;

public class HelixIntegrationTest extends ZkIntegrationTestBase {
	  @Test
	  public void testBasic() throws Exception
	  {
	    // Logger.getRootLogger().setLevel(Level.INFO);
	    String className = TestHelper.getTestClassName();
	    String methodName = TestHelper.getTestMethodName();
	    String clusterName = className + "_" + methodName;
	    int n = 2;

	    System.out.println("START " + clusterName + " at "
	        + new Date(System.currentTimeMillis()));

	    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
	                            "localhost", // participant name prefix
	                            "TestDB", // resource name prefix
	                            1, // resources
	                            4, // partitions per resource
	                            n, // number of nodes
	                            1, // replicas
	                            "MasterSlave",
	                            true); // do rebalance
	    
	    // enable group message
	    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
	    Builder keyBuilder = accessor.keyBuilder();
	    IdealState idealState = accessor.getProperty(keyBuilder.idealStates("TestDB0"));
	    idealState.setGroupMessageMode(true);
	    accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);

	    ClusterController controller =
	        new ClusterController(clusterName, "controller_0", ZK_ADDR);
	    controller.syncStart();

	    // start participants
	    MockParticipant[] participants = new MockParticipant[n];
	    for (int i = 0; i < n; i++)
	    {
	      String instanceName = "localhost_" + (12918 + i);

	      participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR, null);
	      participants[i].syncStart();
	    }

	    boolean result =
	        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
	                                                                                 clusterName));
	    Assert.assertTrue(result);
	    
	    // clean up
	    // wait for all zk callbacks done
	    Thread.sleep(1000);
	    controller.syncStop();
	    for (int i = 0; i < n; i++)
	    {
	      participants[i].syncStop();
	    }

	    System.out.println("END " + clusterName + " at "
	        + new Date(System.currentTimeMillis()));
	  }
}
