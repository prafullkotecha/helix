package com.linkedin.clustermanager.integration;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.agent.zk.ZKDataAccessor;
import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;
import com.linkedin.clustermanager.agent.zk.ZkClient;

public class TestRenamePartition extends ZkStandAloneCMHandler
{
  @Test()
  public void testRenamePartitionAutoIS() throws Exception
  {
    System.out.println("START testRenamePartitionAutoIS at " + new Date(System.currentTimeMillis()));

    // rename partition name TestDB0_0 tp TestDB0_100
    ZkClient zkClient = new ZkClient(ZK_ADDR);
    zkClient.setZkSerializer(new ZNRecordSerializer());
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
  

}
