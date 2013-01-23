package com.linkedin.helix;

import java.util.Date;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.manager.zk.ZKHelixDataAccessor;
import com.linkedin.helix.manager.zk.ZkBaseDataAccessor;
import com.linkedin.helix.messaging.handling.BatchMsgWrapper;
import com.linkedin.helix.mock.controller.ClusterController;
import com.linkedin.helix.mock.storage.MockMSModelFactory;
import com.linkedin.helix.mock.storage.MockParticipant;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.tools.ClusterStateVerifier;
import com.linkedin.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;

public class TestBatchMsgWrapper extends ZkUnitTestBase {
	class MockBatchMsgWrapper extends BatchMsgWrapper {
		int _startCount = 0;
		int _endCount = 0;
		
		@Override
		public void start(Message batchMsg, NotificationContext context) {
			System.out.println("test batchMsg.start() invoked, " + batchMsg.getTgtName());
			_startCount++;
		}

		@Override
		public void end(Message batchMsg, NotificationContext context) {
			System.out.println("test batchMsg.end() invoked, " + batchMsg.getTgtName());
			_endCount++;
		}
	}

	class TestMockMSModelFactory extends MockMSModelFactory {
		@Override
		public BatchMsgWrapper createBatchMsgWrapper(String resourceName)
		{
		   return new MockBatchMsgWrapper();
		}
	}
	
	@Test
	public void testBasic() throws Exception {

		String className = TestHelper.getTestClassName();
		String methodName = TestHelper.getTestMethodName();
		String clusterName = className + "_" + methodName;
		final int n = 2;

		System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

		TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
		        "localhost", // participant name prefix
		        "TestDB", // resource name prefix
		        1, // resources
		        2, // partitions per resource
		        n, // number of nodes
		        2, // replicas
		        "MasterSlave", true); // do rebalance

		// enable batch message
		ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName,
		        new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
		Builder keyBuilder = accessor.keyBuilder();
		IdealState idealState = accessor.getProperty(keyBuilder.idealStates("TestDB0"));
		idealState.setGroupMessageMode(true);
		accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);

		ClusterController controller = new ClusterController(clusterName, "controller_0", ZK_ADDR);
		controller.syncStart();

		// start participants
		MockParticipant[] participants = new MockParticipant[n];
		TestMockMSModelFactory[] ftys = new TestMockMSModelFactory[n];
		
		for (int i = 0; i < n; i++) {
			String instanceName = "localhost_" + (12918 + i);
			ftys[i] = new TestMockMSModelFactory();
			participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR, ftys[i]);
			participants[i].syncStart();
			// wait until the state transition is done, so we will have deterministic results
			Thread.sleep(3000); 
		}
	    boolean result =
	        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
	                                                                                 clusterName));
	    Assert.assertTrue(result);
	    
	    // check batch-msg-wrapper count
	    MockBatchMsgWrapper wrapper = (MockBatchMsgWrapper) ftys[0].getBatchMsgWrapper("TestDB0");
	    // System.out.println("startCount: " + wrapper._startCount);
	    Assert.assertEquals(wrapper._startCount, 3, "start(). Expect 3 batch messages: O->S, S->M, and M->S for 1st participant");
	    // System.out.println("endCount: " + wrapper._endCount);
	    Assert.assertEquals(wrapper._endCount, 3, "end(). Expect 3 batch messages: O->S, S->M, and M->S for 1st participant");

	    wrapper = (MockBatchMsgWrapper) ftys[1].getBatchMsgWrapper("TestDB0");
	    // System.out.println("startCount: " + wrapper._startCount);
	    Assert.assertEquals(wrapper._startCount, 2, "start(). Expect 2 batch messages: O->S, S->M, and M->S for 2nd participant");
	    // System.out.println("endCount: " + wrapper._endCount);
	    Assert.assertEquals(wrapper._endCount, 2, "end(). Expect 2 batch messages: O->S, S->M, and M->S for 2nd participant");


		System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

	}
}
