package com.linkedin.clustermanager.controller.stages;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManagementService;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterMessagingService;
import com.linkedin.clustermanager.ConfigChangeListener;
import com.linkedin.clustermanager.ControllerChangeListener;
import com.linkedin.clustermanager.CurrentStateChangeListener;
import com.linkedin.clustermanager.ExternalViewChangeListener;
import com.linkedin.clustermanager.IdealStateChangeListener;
import com.linkedin.clustermanager.InstanceType;
import com.linkedin.clustermanager.LiveInstanceChangeListener;
import com.linkedin.clustermanager.MessageListener;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.TestHelper;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ZkUnitTestBase;
import com.linkedin.clustermanager.agent.zk.ZKDataAccessor;
import com.linkedin.clustermanager.controller.ClusterManagerMain;
import com.linkedin.clustermanager.healthcheck.ParticipantHealthReportCollector;
import com.linkedin.clustermanager.model.CurrentState;
import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.model.LiveInstance;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.Attributes;
import com.linkedin.clustermanager.model.ResourceKey;
import com.linkedin.clustermanager.model.StateModelDefinition;
import com.linkedin.clustermanager.pipeline.Pipeline;
import com.linkedin.clustermanager.store.PropertyStore;
import com.linkedin.clustermanager.tools.StateModelConfigGenerator;

public class TestRebalancePipeline extends ZkUnitTestBase
{
  private static final Logger LOG =
      Logger.getLogger(TestRebalancePipeline.class.getName());
  final String _className = getShortClassName();

  class MockClusterManager implements ClusterManager
  {
    ClusterDataAccessor _accessor;
    String _clusterName;
    String _sessionId;

    public MockClusterManager(String clusterName, ClusterDataAccessor accessor)
    {
      _clusterName = clusterName;
      _accessor = accessor;
      _sessionId = "session_" + clusterName;
    }

    @Override
    public void connect() throws Exception
    {
      // TODO Auto-generated method stub

    }

    @Override
    public boolean isConnected()
    {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public void disconnect()
    {
      // TODO Auto-generated method stub

    }

    @Override
    public void addIdealStateChangeListener(IdealStateChangeListener listener) throws Exception
    {
      // TODO Auto-generated method stub

    }

    @Override
    public void addLiveInstanceChangeListener(LiveInstanceChangeListener listener) throws Exception
    {
      // TODO Auto-generated method stub

    }

    @Override
    public void addConfigChangeListener(ConfigChangeListener listener) throws Exception
    {
      // TODO Auto-generated method stub

    }

    @Override
    public void addMessageListener(MessageListener listener, String instanceName) throws Exception
    {
      // TODO Auto-generated method stub

    }

    @Override
    public void addCurrentStateChangeListener(CurrentStateChangeListener listener,
                                              String instanceName,
                                              String sessionId) throws Exception
    {
      // TODO Auto-generated method stub

    }

    @Override
    public void addExternalViewChangeListener(ExternalViewChangeListener listener) throws Exception
    {
      // TODO Auto-generated method stub

    }

    @Override
    public boolean removeListener(Object listener)
    {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public ClusterDataAccessor getDataAccessor()
    {
      return _accessor;
    }

    @Override
    public String getClusterName()
    {
      return _clusterName;
    }

    @Override
    public String getInstanceName()
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String getSessionId()
    {
      return _sessionId;
    }

    @Override
    public long getLastNotificationTime()
    {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public void addControllerListener(ControllerChangeListener listener)
    {
      // TODO Auto-generated method stub

    }


    @Override
    public PropertyStore<ZNRecord> getPropertyStore()
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public ClusterMessagingService getMessagingService()
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public ParticipantHealthReportCollector getHealthReportCollector()
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public InstanceType getInstanceType()
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public ClusterManagementService getClusterManagmentTool()
    {
      // TODO Auto-generated method stub
      return null;
    }

  }

  @Test
  public void testDuplicateMsg()
  {
    String clusterName = "CLUSTER_" + _className + "_dup";
    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    ClusterDataAccessor accessor = new ZKDataAccessor(clusterName, _gZkClient);
    ClusterManager manager = new MockClusterManager(clusterName, accessor);
    ClusterEvent event = new ClusterEvent("testEvent");

    final String resourceName = "testResource_dup";
    String[] resourceGroups = new String[] { resourceName };
    // ideal state: node0 is SLAVE on partition_0
    // and node1 is MASTER on partition_0
    // replica=2 means 1 master and 1 slave
    setupIdealState(accessor, new int[] { 0, 1 }, resourceGroups, 1, 2);
    setupStateModel(accessor);
    setupLiveInstances(accessor, new int[] { 0, 1 }, new String[] { "0", "1" });

    // cluster data cache refresh pipeline
    Pipeline dataRefresh = new Pipeline();
    dataRefresh.addStage(new ReadClusterDataStage());

    // rebalance pipeline
    Pipeline rebalancePipeline = new Pipeline();
    rebalancePipeline.addStage(new ResourceComputationStage());
    rebalancePipeline.addStage(new CurrentStateComputationStage());
    rebalancePipeline.addStage(new BestPossibleStateCalcStage());
    rebalancePipeline.addStage(new MessageGenerationPhase());
    rebalancePipeline.addStage(new MessageSelectionStage());
    rebalancePipeline.addStage(new TaskAssignmentStage());
    // round1: set node0's currentState to SLAVE on partition_0
    // and node1's currentState to OFFLINE on partition_0
    setCurrentState(accessor,
                    "localhost_0",
                    resourceName,
                    resourceName + "_0",
                    "session_0",
                    "SLAVE");
    setCurrentState(accessor,
                    "localhost_1",
                    resourceName,
                    resourceName + "_0",
                    "session_1",
                    "OFFLINE");

    runPipeline(manager, event, dataRefresh);
    runPipeline(manager, event, rebalancePipeline);
    MessageSelectionStageOutput msgSelOutput =
        event.getAttribute(AttributeName.MESSAGES_SELECTED.toString());
    List<Message> messages =
        msgSelOutput.getMessages(resourceName, new ResourceKey(resourceName + "_0"));
    Assert.assertEquals(messages.size(),
                        1,
                        "Should output 1 message: OFFLINE-SLAVE for node1");
    Message message = messages.get(0);
    Assert.assertEquals(message.getFromState(), "OFFLINE");
    Assert.assertEquals(message.getToState(), "SLAVE");
    Assert.assertEquals(message.getTgtName(), "localhost_1");

    // round2: localhost_1 updates its currentState to SLAVE but haven't removed the
    // message yet
    // make sure controller should not send S->M message until removal is done
    setCurrentState(accessor,
                    "localhost_1",
                    resourceName,
                    resourceName + "_0",
                    "session_1",
                    "SLAVE");

    runPipeline(manager, event, dataRefresh);
    runPipeline(manager, event, rebalancePipeline);
    msgSelOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.toString());
    messages =
        msgSelOutput.getMessages(resourceName, new ResourceKey(resourceName + "_0"));
    Assert.assertEquals(messages.size(),
                        0,
                        "Should NOT output 1 message: SLAVE-MASTER for node1");

    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testMsgTriggeredRebalance() throws Exception
  {
    String clusterName = "CLUSTER_" + _className + "_msgTrigger";
    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    ClusterDataAccessor accessor = new ZKDataAccessor(clusterName, _gZkClient);
    ClusterManager manager = new MockClusterManager(clusterName, accessor);
    ClusterEvent event = new ClusterEvent("testEvent");

    final String resourceName = "testResource_dup";
    String[] resourceGroups = new String[] { resourceName };

    TestHelper.setupEmptyCluster(_gZkClient, clusterName);

    // ideal state: node0 is SLAVE on partition_0
    // and node1 is MASTER on partition_0
    // replica=2 means 1 master and 1 slave
    setupIdealState(accessor, new int[] { 0, 1 }, resourceGroups, 1, 2);
    setupStateModel(accessor);
    setupLiveInstances(accessor, new int[] { 0, 1 }, new String[] { "0", "1" });

    TestHelper.startClusterController(clusterName,
                                      "controller",
                                      ZK_ADDR,
                                      ClusterManagerMain.STANDALONE,
                                      _gZkClient);

    // round0: controller sends O->S to both node0 and node1
    Thread.sleep(1000);
    List<String> messages = accessor.getChildNames(PropertyType.MESSAGES, "localhost_0");
    Assert.assertEquals(messages.size(), 1);
    messages = accessor.getChildNames(PropertyType.MESSAGES, "localhost_1");
    Assert.assertEquals(messages.size(), 1);

    // round1: node0 and node1 update current states but not removing messages
    // controller's rebalance pipeline should be triggered but since messages are not removed
    // no new messages will be sent
    setCurrentState(accessor,
                    "localhost_0",
                    resourceName,
                    resourceName + "_0",
                    "session_0",
                    "SLAVE");
    setCurrentState(accessor,
                    "localhost_1",
                    resourceName,
                    resourceName + "_0",
                    "session_1",
                    "SLAVE");
    Thread.sleep(1000);
    messages = accessor.getChildNames(PropertyType.MESSAGES, "localhost_0");
    Assert.assertEquals(messages.size(), 1);
    messages = accessor.getChildNames(PropertyType.MESSAGES, "localhost_1");
    Assert.assertEquals(messages.size(), 1);

    // round2: node1 removes message and controller's rebalance pipeline should be triggered
    //  and sends another S->M to node1
    accessor.removeProperty(PropertyType.MESSAGES, "localhost_1", messages.get(0));
    Thread.sleep(1000);
    messages = accessor.getChildNames(PropertyType.MESSAGES, "localhost_1");
    Assert.assertEquals(messages.size(), 1);
    ZNRecord msg = accessor.getProperty(PropertyType.MESSAGES, "localhost_1", messages.get(0));
    String toState = msg.getSimpleField(Attributes.TO_STATE.toString());
    Assert.assertEquals(toState, "MASTER");

    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

  }
  protected List<IdealState> setupIdealState(ClusterDataAccessor accessor,
                                             int[] nodes,
                                             String[] resourceGroups,
                                             int partitions,
                                             int replicas)
  {
    List<IdealState> idealStates = new ArrayList<IdealState>();
    List<String> instances = new ArrayList<String>();
    for (int i : nodes)
    {
      instances.add("localhost_" + i);
    }

    for (String resourceGroupName : resourceGroups)
    {
      IdealState idealState = new IdealState(resourceGroupName);
      for (int p = 0; p < partitions; p++)
      {
        List<String> value = new ArrayList<String>();
        for (int r = 0; r < replicas; r++)
        {
          value.add("localhost_" + (p + r + 1) % nodes.length);
        }
        idealState.getRecord().setListField(resourceGroupName + "_" + p, value);
      }

      idealState.setStateModelDefRef("MasterSlave");
      idealState.setNumPartitions(partitions);
      idealStates.add(idealState);

      // System.out.println(idealState);
      accessor.setProperty(PropertyType.IDEALSTATES,
                            idealState.getRecord(),
                            resourceGroupName);
    }
    return idealStates;
  }

  protected void setupLiveInstances(ClusterDataAccessor accessor,
                                    int[] liveInstances, String[] sessionIds)
  {
    for (int i = 0; i < liveInstances.length; i++)
    {
      String instance = "localhost_" + liveInstances[i];
      LiveInstance liveInstance = new LiveInstance(new ZNRecord(instance));
      liveInstance.setSessionId("session_" + sessionIds[i]);
      accessor.setProperty(PropertyType.LIVEINSTANCES,
                            liveInstance.getRecord(),
                            instance);
    }
  }

  protected void setupStateModel(ClusterDataAccessor accessor)
  {
    StateModelConfigGenerator generator = new StateModelConfigGenerator();
    StateModelDefinition masterSlave =
        new StateModelDefinition(generator.generateConfigForMasterSlave());
    accessor.setProperty(PropertyType.STATEMODELDEFS,
                          masterSlave.getRecord(),
                          masterSlave.getId());
    StateModelDefinition leaderStandby =
        new StateModelDefinition(generator.generateConfigForLeaderStandby());
    accessor.setProperty(PropertyType.STATEMODELDEFS,
                          leaderStandby.getRecord(),
                          leaderStandby.getId());
    StateModelDefinition onlineOffline =
        new StateModelDefinition(generator.generateConfigForOnlineOffline());
    accessor.setProperty(PropertyType.STATEMODELDEFS,
                          onlineOffline.getRecord(),
                          onlineOffline.getId());
  }

  protected void setCurrentState(ClusterDataAccessor accessor,
                                 String instance,
                                 String resourceGroupName,
                                 String resourceKey,
                                 String sessionId,
                                 String state)
  {
    CurrentState curState = new CurrentState(new ZNRecord(resourceGroupName));
    curState.setState(resourceKey, state);
    curState.setSessionId(sessionId);
    curState.setStateModelDefRef("MasterSlave");
    accessor.setProperty(PropertyType.CURRENTSTATES,
                          curState.getRecord(),
                          instance,
                          sessionId,
                          resourceGroupName);
  }

  protected void runPipeline(ClusterManager manager,
                             ClusterEvent event, Pipeline pipeline)
  {
    event.addAttribute("clustermanager", manager);
    try
    {
      pipeline.handle(event);
      pipeline.finish();
    }
    catch (Exception e)
    {
      LOG.error("Exception while executing pipeline:" + pipeline
          + ". Will not continue to next pipeline", e);
    }
  }
}