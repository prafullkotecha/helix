package com.linkedin.clustermanager.integration;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.agent.zk.ZKDataAccessor;
import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.tools.IdealStateCalculatorForStorageNode;

public class TestRenamePartition extends ZkStandAloneCMHandler
{
  @Test()
  public void testRenamePartitionAutoIS() throws Exception
  {
    System.out.println("START testRenamePartitionAutoIS at " + new Date(System.currentTimeMillis()));

    // rename partition name TestDB_0 to TestDB_100
    ZKDataAccessor accessor = new ZKDataAccessor(CLUSTER_NAME, _zkClient);
    ZNRecord idealState = accessor.getProperty(PropertyType.IDEALSTATES, "TestDB");
    
    List<String> prioList = idealState.getListFields().remove("TestDB_0");
    idealState.getListFields().put("TestDB_100", prioList);
    Map<String, String> stateMap = idealState.getMapFields().remove("TestDB_0");
    idealState.getMapFields().put("TestDB_100", stateMap);
    accessor.setProperty(PropertyType.IDEALSTATES, idealState, "TestDB");
    
    verifyIdealAndCurrentStateTimeout(CLUSTER_NAME);
    System.out.println("END testRenamePartitionAutoIS at " + new Date(System.currentTimeMillis()));
  }
  
  @Test()
  public void testRenamePartitionCustomIS() throws Exception
  {
    System.out.println("START testRenamePartitionCustomIS at " + new Date(System.currentTimeMillis()));

    // add a resource group using customized idealState
    ZKDataAccessor accessor = new ZKDataAccessor(CLUSTER_NAME, _zkClient);
    
    // calculate idealState
    List<String> instanceNames = Arrays.asList("localhost_12918", "localhost_12919", "localhost_12920",
        "localhost_12921", "localhost_12922");
    ZNRecord destIS = IdealStateCalculatorForStorageNode.calculateIdealState(instanceNames,
        10, 3-1, "TestDB0", "MASTER", "SLAVE");
    IdealState idealState = new IdealState(destIS);
    idealState.setIdealStateMode("CUSTOMIZED");
    idealState.setStateModelDefRef("MasterSlave");
    accessor.setProperty(PropertyType.IDEALSTATES, idealState.getRecord(), "TestDB0");

    verifyIdealAndCurrentStateTimeout(CLUSTER_NAME);

    // rename partition name TestDB0_0 to TestDB0_100
    Map<String, String> stateMap = idealState.getRecord().getMapFields().remove("TestDB0_0");
    idealState.getRecord().getMapFields().put("TestDB0_100", stateMap);
    accessor.setProperty(PropertyType.IDEALSTATES, idealState.getRecord(), "TestDB0");
    
    verifyIdealAndCurrentStateTimeout(CLUSTER_NAME);

    System.out.println("END testRenamePartitionCustomIS at " + new Date(System.currentTimeMillis()));
  }

}
