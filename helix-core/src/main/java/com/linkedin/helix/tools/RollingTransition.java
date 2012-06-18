package com.linkedin.helix.tools;

import java.util.ArrayList;

import com.linkedin.helix.ConfigScope.ConfigScopeProperty;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixManagerFactory;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.InstanceConfig;

public class RollingTransition
{
  public static void main(String[] args) throws Exception
  {
    HelixManager zkHelixManager =
        HelixManagerFactory.getZKHelixManager("test-cluster",
                                              "ADMIN",
                                              InstanceType.ADMINISTRATOR,
                                              "localhost:2191");
    zkHelixManager.connect();
    IdealState is =
        zkHelixManager.getDataAccessor().getProperty(IdealState.class,
                                                     PropertyType.IDEALSTATES,
                                                     "test-db");
    int numPartitions = is.getNumPartitions();

    InstanceConfig nodeConfig =
        zkHelixManager.getDataAccessor()
                      .getProperty(InstanceConfig.class,
                                   PropertyType.CONFIGS,
                                   ConfigScopeProperty.PARTICIPANT.toString(),
                                   "localhost_8901");
    ZkClient client = new ZkClient("localhost:2191");
    client.waitUntilConnected();
    ExternalView ev =
        zkHelixManager.getDataAccessor().getProperty(ExternalView.class,
                                                     PropertyType.EXTERNALVIEW,
                                                     "test-db");
    ArrayList<String> masterPartitionsonNode1= new ArrayList<String>();
    for (String partition : ev.getPartitionSet())
    {
      String state = ev.getStateMap(partition).get("localhost_8901");
      if (state.equalsIgnoreCase("MASTER"))
      {
        masterPartitionsonNode1.add(partition);
      }
    }
    System.out.println("Masters on node 1:" +masterPartitionsonNode1.size());
    if(masterPartitionsonNode1.size()!=1000){
      System.exit(1);
    }
    int rounds = 1;
    int batchSize = masterPartitionsonNode1.size()/rounds;
    for (int j = 0; j < rounds; j++)
    {
      int start = j*batchSize;
      int end = batchSize *(j+1) -1;
      for (int i = start; i <= end && i<masterPartitionsonNode1.size(); i++)
      {
        nodeConfig.setInstanceEnabledForPartition(masterPartitionsonNode1.get(i), false);
      }
      zkHelixManager.getDataAccessor()
                    .setProperty(PropertyType.CONFIGS,
                                 nodeConfig,
                                 ConfigScopeProperty.PARTICIPANT.toString(),
                                 "localhost_8901");
      System.out.println("Disable:"
          + nodeConfig.getRecord().getMapField("HELIX_DISABLED_PARTITION").size()
          + " sleeping for 10 seconds");
      Thread.sleep(10000);
      ev =
          zkHelixManager.getDataAccessor().getProperty(ExternalView.class,
                                                       PropertyType.EXTERNALVIEW,
                                                       "test-db");
      int masterCount = 0;
      for (String partition : ev.getPartitionSet())
      {
        String state = ev.getStateMap(partition).get("localhost_8902");
        if (state.equalsIgnoreCase("MASTER"))
        {
          masterCount++;
        }
      }
      System.out.println("Master count on node 2=" + masterCount);

    }
  }
}
