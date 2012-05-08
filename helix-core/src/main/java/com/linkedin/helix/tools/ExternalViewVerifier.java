package com.linkedin.helix.tools;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkDataListener;
import org.apache.log4j.Logger;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.PropertyPathConfig;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZKDataAccessor;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.IdealState.IdealStateModeProperty;
import com.linkedin.helix.model.StateModelDefinition;
import com.linkedin.helix.util.ZKClientPool;

public class ExternalViewVerifier
{
  private static final Logger LOG = Logger.getLogger(ExternalViewVerifier.class);

  static class ExternalViewVerifierListener implements IZkDataListener
  {
    final CountDownLatch _countDown;
    final ZkClient _zkClient;
    final ZNRecord _bestPossState;

    public ExternalViewVerifierListener(CountDownLatch countDown, ZkClient zkClient,
        ZNRecord bestPossState)
    {
      _countDown = countDown;
      _zkClient = zkClient;
      _bestPossState = bestPossState;
    }

    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception
    {
      ZNRecord extView = (ZNRecord) data;
      boolean result = ExternalViewVerifier.verifyExternalView(extView, _bestPossState);
      if (result == true)
      {
        _countDown.countDown();
      }
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception
    {
      // TODO Auto-generated method stub

    }

  }

  private static void statesCounts(ZNRecord record, StateModelDefinition stateModelDef,
      int liveInstances, int replica)
  {
    for (String state : stateModelDef.getStatesPriorityList())
    {
      String num = stateModelDef.getNumInstancesPerState(state);
      int stateCount = -1;
      if ("N".equals(num))
      {
        stateCount = liveInstances;
      } else if ("R".equals(num))
      {
        stateCount = replica;
      } else
      {
        try
        {
          stateCount = Integer.parseInt(num);
        } catch (Exception e)
        {
          LOG.error("Invalid count for state: " + state + ", count: " + num);
        }
      }

      if (stateCount > 0)
      {
        record.setSimpleField(state, Integer.toString(stateCount));
      }
    }
  }

  private static void bestPossStateOfPartition(ZNRecord record, String partitionName,
      List<String> liveInstances, StateModelDefinition stateModelDef, IdealState idealState)
  {
    Map<String, String> instanceStateMap = new HashMap<String, String>();

    // TODO: remove stateModelDef, not used in getPreferenceList()
    List<String> preferenceList = idealState.getPreferenceList(partitionName, stateModelDef);

    int i = 0;
    for (String state : stateModelDef.getStatesPriorityList())
    {
      String stateCountStr = record.getSimpleField(state);
      if (stateCountStr == null)
      {
        continue;
      }
      int stateCount = Integer.parseInt(stateCountStr);
      for (; i < stateCount && i < preferenceList.size(); i++)
      {
        String instanceName = preferenceList.get(i);
        if (liveInstances.contains(instanceName))
        {
          instanceStateMap.put(instanceName, state);
        }
      }
    }
    record.setMapField(partitionName, instanceStateMap);
  }

  private static ZNRecord bestPossState(String resource, int partitionNb, int replica,
      List<String> configInstances, List<String> liveInstances)
  {
    ZNRecord record = new ZNRecord(resource);

    // TODO get stateModelDef from stateModelName
    StateModelDefinition stateModelDef = new StateModelDefinition(
        new StateModelConfigGenerator().generateConfigForMasterSlave());

    // TODO calculate idealState
    ZNRecord idealStateRecord = IdealStateCalculatorForStorageNode.calculateIdealState(
        configInstances, partitionNb, replica - 1, resource, "MASTER", "SLAVE");
    IdealState idealState = new IdealState(idealStateRecord);
    idealState.setIdealStateMode(IdealStateModeProperty.AUTO.toString());
    idealState.setNumPartitions(partitionNb);
    idealState.setStateModelDefRef("MasterSlave");

    statesCounts(record, stateModelDef, liveInstances.size(), replica);

    for (String partitionName : idealState.getPartitionSet())
    {
      bestPossStateOfPartition(record, partitionName, liveInstances, stateModelDef, idealState);
    }
    return record;
  }

  private static boolean verifyExternalView(ZNRecord extView, ZNRecord bestPossState)
  {
    // step 1: externalView and bestPossibleState has equal size
    int extViewSize = extView.getMapFields().size();
    int bestPossStateSize = bestPossState.getMapFields().size();
    if (extViewSize != bestPossStateSize)
    {
      LOG.info("exterView size (" + extViewSize + ") is different from bestPossState size ("
          + bestPossStateSize + ")");
      // System.out.println("extView: " + extView.getRecord().getMapFields());
      // System.out.println("bestPossState: " +
      // bestPossOutput.getResourceMap(resourceName));
      return false;
    }

    // step 2: every entry in external view is contained in best possible state
    for (String partition : extView.getMapFields().keySet())
    {
      Map<String, String> evInstanceStateMap = extView.getMapField(partition);
      Map<String, String> bpInstanceStateMap = bestPossState.getMapField(partition);

      boolean result = ClusterStateVerifier.<String, String> compareMap(evInstanceStateMap,
          bpInstanceStateMap);
      if (result == false)
      {
        LOG.info("externalView is different from bestPossibleState for partition: " + partition);
        return false;
      }
    }
    return true;
  }

  public static boolean verify(String zkAddr, String clusterName, String resource, int partitionNb,
      int replica, List<String> configInstances, List<String> liveInstances, long timeout)
  {
    ZNRecord bestPossState = bestPossState(resource, partitionNb, replica, configInstances, liveInstances);

    long startTime = System.currentTimeMillis();
    CountDownLatch countDown = new CountDownLatch(1);
    ZkClient zkClient = ZKClientPool.getZkClient(zkAddr);

    ExternalViewVerifierListener listener = new ExternalViewVerifierListener(countDown, zkClient,
        bestPossState);

    String extViewPath = PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName,
        resource);
    zkClient.subscribeDataChanges(extViewPath, listener);

    // do initial verify
    DataAccessor accessor = new ZKDataAccessor(clusterName, zkClient);
    ZNRecord extView = accessor.getProperty(PropertyType.EXTERNALVIEW, resource);
    boolean result = verifyExternalView(extView, bestPossState);
    if (result == false)
    {
      try
      {
        result = countDown.await(timeout, TimeUnit.MILLISECONDS);
        if (result == false)
        {
          // make a final try if timeout
          result = verifyExternalView(extView, bestPossState);
        }
      } catch (Exception e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    // clean up
    zkClient.unsubscribeDataChanges(extViewPath, listener);

    long endTime = System.currentTimeMillis();

    // debug
    System.err.println(result + ": wait " + (endTime - startTime) + "ms to verify " + clusterName);

    return result;
  }

  public static void main(String[] args)
  {
    List<String> configInstances = Arrays.asList("localhost_12918", "localhost_12919", "localhost_12920", "localhost_12921", "localhost_12922");
    List<String> liveInstances = Arrays.asList("localhost_12918", "localhost_12919", "localhost_12920", "localhost_12921", "localhost_12922");
//    ZNRecord record = bestPossState("TestDB", 10, 3, configInstances, liveInstances);
//    System.out.println("bestPossState: " + record);
    verify("localhost:2183", "CLUSTER_IntegrationTest", "TestDB", 20,
         3, configInstances, liveInstances, 10000);
    
  }
}
