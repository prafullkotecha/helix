package com.linkedin.helix;

import java.util.List;

import com.linkedin.helix.manager.zk.CallbackHandler;
import com.linkedin.helix.manager.zk.ZKHelixManager;
import com.linkedin.helix.manager.zk.ZkClient;

// HelixManager used for test only
// expose more class members
public class ZkHelixTestManager extends ZKHelixManager {

	public ZkHelixTestManager(String clusterName, String instanceName, InstanceType instanceType,
            String zkConnectString) throws Exception {
	    super(clusterName, instanceName, instanceType, zkConnectString);
	    // TODO Auto-generated constructor stub
    }

	public ZkClient getZkClient() {
		return _zkClient;
	}
	
	public List<CallbackHandler> getHandlers() {
		return _handlers;
	}
}
