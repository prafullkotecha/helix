/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import com.linkedin.helix.ConfigChangeListener;
import com.linkedin.helix.ControllerChangeListener;
import com.linkedin.helix.CurrentStateChangeListener;
import com.linkedin.helix.ExternalViewChangeListener;
import com.linkedin.helix.HealthStateChangeListener;
import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.IdealStateChangeListener;
import com.linkedin.helix.LiveInstanceChangeListener;
import com.linkedin.helix.MessageListener;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.NotificationContext.Type;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.controller.pipeline.Pipeline;
import com.linkedin.helix.controller.pipeline.PipelineRegistry;
import com.linkedin.helix.controller.stages.BestPossibleStateCalcStage;
import com.linkedin.helix.controller.stages.ClusterEvent;
import com.linkedin.helix.controller.stages.CompatibilityCheckStage;
import com.linkedin.helix.controller.stages.CurrentStateComputationStage;
import com.linkedin.helix.controller.stages.ExternalViewComputeStage;
import com.linkedin.helix.controller.stages.MessageGenerationPhase;
import com.linkedin.helix.controller.stages.MessageSelectionStage;
import com.linkedin.helix.controller.stages.MessageThrottleStage;
import com.linkedin.helix.controller.stages.ReadClusterDataStage;
import com.linkedin.helix.controller.stages.ResourceComputationStage;
import com.linkedin.helix.controller.stages.TaskAssignmentStage;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.HealthStat;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.InstanceConfig;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.PauseSignal;
import com.linkedin.helix.monitoring.mbeans.ClusterStatusMonitor;

/**
 * Cluster Controllers main goal is to keep the cluster state as close as possible to
 * Ideal State. It does this by listening to changes in cluster state and scheduling new
 * tasks to get cluster state to best possible ideal state. Every instance of this class
 * can control can control only one cluster
 *
 *
 * Get all the partitions use IdealState, CurrentState and Messages <br>
 * foreach partition <br>
 * 1. get the (instance,state) from IdealState, CurrentState and PendingMessages <br>
 * 2. compute best possible state (instance,state) pair. This needs previous step data and
 * state model constraints <br>
 * 3. compute the messages/tasks needed to move to 1 to 2 <br>
 * 4. select the messages that can be sent, needs messages and state model constraints <br>
 * 5. send messages
 */
public class GenericHelixController implements
    ConfigChangeListener,
    IdealStateChangeListener,
    LiveInstanceChangeListener,
    MessageListener,
    CurrentStateChangeListener,
    ExternalViewChangeListener,
    ControllerChangeListener,
    HealthStateChangeListener
{
  private static final Logger    LOG = Logger.getLogger(GenericHelixController.class.getName());
  volatile boolean               init   = false;
  private final PipelineRegistry _registry;

  // keep track of last-seen live-instance list index by instanceName and sessionId respectively
  // so we can figure out which instance/session is no longer valid when on-live-instance change comes
  final AtomicReference<Map<String, LiveInstance>>	_lastSeenInstances;
  final AtomicReference<Map<String, LiveInstance>>	_lastSeenSessions;

  ClusterStatusMonitor           _clusterStatusMonitor;

  /**
   * The _paused flag is checked by function handleEvent(), while if the flag is set
   * handleEvent() will be no-op. Other event handling logic keeps the same when the flag
   * is set.
   */
  private boolean                _paused;

  /**
   * The timer that can periodically run the rebalancing pipeline. The timer will start if there
   * is one resource group has the config to use the timer.
   */
  Timer _rebalanceTimer = null;
  int _timerPeriod = Integer.MAX_VALUE;

  /**
   * Default constructor that creates a default pipeline registry. This is sufficient in
   * most cases, but if there is a some thing specific needed use another constructor
   * where in you can pass a pipeline registry
   */
  public GenericHelixController()
  {
    this(createDefaultRegistry());
  }

  class RebalanceTask extends TimerTask
  {
    HelixManager _manager;
    
    public RebalanceTask(HelixManager manager)
    {
      _manager = manager;
    }
    
    @Override
    public void run()
    {
      NotificationContext changeContext = new NotificationContext(_manager);
      changeContext.setType(NotificationContext.Type.CALLBACK);
      ClusterEvent event = new ClusterEvent("periodicalRebalance");
      event.addAttribute("helixmanager", changeContext.getManager());
      event.addAttribute("changeContext", changeContext);
      List<ZNRecord> dummy = new ArrayList<ZNRecord>();
      event.addAttribute("eventData", dummy);
      // Should be able to process  
      handleEvent(event);
    }
  }
  
  /**
   * Starts the rebalancing timer with the specified period. Start the timer if necessary;
   * If the period is smaller than the current period, cancel the current timer and use 
   * the new period.
   */
  void startRebalancingTimer(int period, HelixManager manager)
  {
    LOG.info("Controller starting timer at period " + period);
    if(period < _timerPeriod)
    {
      if(_rebalanceTimer != null)
      {
        _rebalanceTimer.cancel();
      }
      _rebalanceTimer = new Timer(true);
      _timerPeriod = period;
      _rebalanceTimer.scheduleAtFixedRate(new RebalanceTask(manager), _timerPeriod, _timerPeriod);
    }
    else
    {
      LOG.info("Controller already has timer at period " + _timerPeriod);
    }
  }
  
  /**
   * Starts the rebalancing timer 
   */
  void stopRebalancingTimer()
  {
    if(_rebalanceTimer != null)
    {
      _rebalanceTimer.cancel();
      _rebalanceTimer = null;
    }
    _timerPeriod = Integer.MAX_VALUE;
  }
  
  private static PipelineRegistry createDefaultRegistry()
  {
    LOG.info("createDefaultRegistry");
    synchronized (GenericHelixController.class)
    {
      PipelineRegistry registry = new PipelineRegistry();

      // cluster data cache refresh
      Pipeline dataRefresh = new Pipeline();
      dataRefresh.addStage(new ReadClusterDataStage());

      // rebalance pipeline
      Pipeline rebalancePipeline = new Pipeline();
      rebalancePipeline.addStage(new ResourceComputationStage());
      rebalancePipeline.addStage(new CurrentStateComputationStage());
      rebalancePipeline.addStage(new BestPossibleStateCalcStage());
      rebalancePipeline.addStage(new MessageGenerationPhase());
      rebalancePipeline.addStage(new MessageSelectionStage());
      rebalancePipeline.addStage(new MessageThrottleStage());
      rebalancePipeline.addStage(new TaskAssignmentStage());

      // external view generation
      Pipeline externalViewPipeline = new Pipeline();
      externalViewPipeline.addStage(new ExternalViewComputeStage());

      // backward compatibility check
      Pipeline liveInstancePipeline = new Pipeline();
      liveInstancePipeline.addStage(new CompatibilityCheckStage());

      registry.register("idealStateChange", dataRefresh, rebalancePipeline);
      registry.register("currentStateChange",
                        dataRefresh,
                        rebalancePipeline,
                        externalViewPipeline);
      registry.register("configChange", dataRefresh, rebalancePipeline);
      registry.register("liveInstanceChange",
                        dataRefresh,
                        liveInstancePipeline,
                        rebalancePipeline,
                        externalViewPipeline);

      registry.register("messageChange",
                        dataRefresh,
                        rebalancePipeline);
      registry.register("externalView", dataRefresh);
      registry.register("resume", dataRefresh, rebalancePipeline, externalViewPipeline);
      registry.register("periodicalRebalance", dataRefresh, rebalancePipeline, externalViewPipeline);

      // health stats pipeline
      // Pipeline healthStatsAggregationPipeline = new Pipeline();
      // StatsAggregationStage statsStage = new StatsAggregationStage();
      // healthStatsAggregationPipeline.addStage(new ReadHealthDataStage());
      // healthStatsAggregationPipeline.addStage(statsStage);
      // registry.register("healthChange", healthStatsAggregationPipeline);

      return registry;
    }
  }

  public GenericHelixController(PipelineRegistry registry)
  {
    _paused = false;
    _registry = registry;
    
    _lastSeenInstances = new AtomicReference<Map<String, LiveInstance>>();
    _lastSeenSessions = new AtomicReference<Map<String,LiveInstance>>();
  }

  /**
   * lock-always: caller always needs to obtain an external lock before call, calls to
   * handleEvent() should be serialized
   *
   * @param event
   */
  protected synchronized void handleEvent(ClusterEvent event)
  {
    HelixManager manager = event.getAttribute("helixmanager");
    if (manager == null)
    {
      LOG.error("No cluster manager in event:" + event.getName());
      return;
    }

    if (!manager.isLeader())
    {
      LOG.error("Cluster manager: " + manager.getInstanceName()
          + " is not leader. Pipeline will not be invoked");
      return;
    }

    if (_paused)
    {
      LOG.info("Cluster is paused. Ignoring the event:" + event.getName());
      return;
    }

    NotificationContext context = null;
    if (event.getAttribute("changeContext") != null)
    {
      context = (NotificationContext) (event.getAttribute("changeContext"));
    }

    // Initialize _clusterStatusMonitor
    if (context != null)
    {
      if (context.getType() == Type.FINALIZE)
      {
        if (_clusterStatusMonitor != null)
        {
          _clusterStatusMonitor.reset();
          _clusterStatusMonitor = null;
        }
        
        stopRebalancingTimer();
        LOG.info("Get FINALIZE notification, skip the pipeline. Event :" + event.getName());
        return;
      }
      else
      {
        if (_clusterStatusMonitor == null)
        {
          _clusterStatusMonitor = new ClusterStatusMonitor(manager.getClusterName());
        }
        
        event.addAttribute("clusterStatusMonitor", _clusterStatusMonitor);
      }
    }

    List<Pipeline> pipelines = _registry.getPipelinesForEvent(event.getName());
    if (pipelines == null || pipelines.size() == 0)
    {
      LOG.info("No pipeline to run for event:" + event.getName());
      return;
    }

    for (Pipeline pipeline : pipelines)
    {
      try
      {
        pipeline.handle(event);
        pipeline.finish();
      }
      catch (Exception e)
      {
        LOG.error("Exception while executing pipeline: " + pipeline
            + ". Will not continue to next pipeline", e);
        break;
      }
    }
  }

  // TODO since we read data in pipeline, we can get rid of reading from zookeeper in
  // callback

  @Override
  public void onExternalViewChange(List<ExternalView> externalViewList,
                                   NotificationContext changeContext)
  {
//    logger.info("START: GenericClusterController.onExternalViewChange()");
//    ClusterEvent event = new ClusterEvent("externalViewChange");
//    event.addAttribute("helixmanager", changeContext.getManager());
//    event.addAttribute("changeContext", changeContext);
//    event.addAttribute("eventData", externalViewList);
//    // handleEvent(event);
//    logger.info("END: GenericClusterController.onExternalViewChange()");
  }

  @Override
  public void onStateChange(String instanceName,
                            List<CurrentState> statesInfo,
                            NotificationContext changeContext)
  {
    LOG.info("START: GenericClusterController.onStateChange()");
    ClusterEvent event = new ClusterEvent("currentStateChange");
    event.addAttribute("helixmanager", changeContext.getManager());
    event.addAttribute("instanceName", instanceName);
    event.addAttribute("changeContext", changeContext);
    event.addAttribute("eventData", statesInfo);
    handleEvent(event);
    LOG.info("END: GenericClusterController.onStateChange()");
  }

  @Override
  public void onHealthChange(String instanceName,
                             List<HealthStat> reports,
                             NotificationContext changeContext)
  {
    /**
     * When there are more participant ( > 20, can be in hundreds), This callback can be
     * called quite frequently as each participant reports health stat every minute. Thus
     * we change the health check pipeline to run in a timer callback.
     */
  }

  @Override
  public void onMessage(String instanceName,
                        List<Message> messages,
                        NotificationContext changeContext)
  {
    LOG.info("START: GenericClusterController.onMessage()");
    
    ClusterEvent event = new ClusterEvent("messageChange");
    event.addAttribute("helixmanager", changeContext.getManager());
    event.addAttribute("instanceName", instanceName);
    event.addAttribute("changeContext", changeContext);
    event.addAttribute("eventData", messages);
    handleEvent(event);
    
    if (_clusterStatusMonitor != null && messages != null)
    {
      _clusterStatusMonitor.addMessageQueueSize(instanceName, messages.size());
    }
        
    LOG.info("END: GenericClusterController.onMessage()");
  }

  @Override
  public void onLiveInstanceChange(List<LiveInstance> liveInstances,
                                   NotificationContext changeContext)
  {
    LOG.info("START: Generic GenericClusterController.onLiveInstanceChange()");
    if (liveInstances == null)
    {
      liveInstances = Collections.emptyList();
    }
    // Go though the live instance list and make sure that we are observing them
    // accordingly. The action is done regardless of the paused flag.
    if (changeContext.getType() == NotificationContext.Type.INIT ||
        changeContext.getType() == NotificationContext.Type.CALLBACK)
    {
      checkLiveInstancesObservation(liveInstances, changeContext);
    }

    ClusterEvent event = new ClusterEvent("liveInstanceChange");
    event.addAttribute("helixmanager", changeContext.getManager());
    event.addAttribute("changeContext", changeContext);
    event.addAttribute("eventData", liveInstances);
    handleEvent(event);
    LOG.info("END: Generic GenericClusterController.onLiveInstanceChange()");
  }
  
  void checkRebalancingTimer(HelixManager manager, List<IdealState> idealStates)
  {
    if (manager.getConfigAccessor() == null)
    {
      LOG.warn(manager.getInstanceName() + " config accessor doesn't exist. should be in file-based mode.");
      return;
    }
    
    for(IdealState idealState : idealStates)
    {
      int period = idealState.getRebalanceTimerPeriod();
      if(period > 0)
      {
        startRebalancingTimer(period, manager);
      }
    }
  }
  
  @Override
  public void onIdealStateChange(List<IdealState> idealStates,
                                 NotificationContext changeContext)
  {
    LOG.info("START: Generic GenericClusterController.onIdealStateChange()");
    ClusterEvent event = new ClusterEvent("idealStateChange");
    event.addAttribute("helixmanager", changeContext.getManager());
    event.addAttribute("changeContext", changeContext);
    event.addAttribute("eventData", idealStates);
    handleEvent(event);
    
    if(changeContext.getType() != Type.FINALIZE)
    {
      checkRebalancingTimer(changeContext.getManager(), idealStates);
    }
    
    LOG.info("END: Generic GenericClusterController.onIdealStateChange()");
  }

  @Override
  public void onConfigChange(List<InstanceConfig> configs,
                             NotificationContext changeContext)
  {
    LOG.info("START: GenericClusterController.onConfigChange()");
    ClusterEvent event = new ClusterEvent("configChange");
    event.addAttribute("changeContext", changeContext);
    event.addAttribute("helixmanager", changeContext.getManager());
    event.addAttribute("eventData", configs);
    handleEvent(event);
    LOG.info("END: GenericClusterController.onConfigChange()");
  }

  @Override
  public void onControllerChange(NotificationContext changeContext)
  {
    LOG.info("START: GenericClusterController.onControllerChange()");
    if (changeContext!= null && changeContext.getType() == Type.FINALIZE)
    {
      LOG.info("GenericClusterController.onControllerChange() FINALIZE");
      return;
    }
    HelixDataAccessor accessor = changeContext.getManager().getHelixDataAccessor();

    // double check if this controller is the leader
    Builder keyBuilder = accessor.keyBuilder();
    LiveInstance leader =
        accessor.getProperty(keyBuilder.controllerLeader());
    if (leader == null)
    {
      LOG.warn("No controller exists for cluster:"
          + changeContext.getManager().getClusterName());
      return;
    }
    else
    {
      String leaderName = leader.getInstanceName();

      String instanceName = changeContext.getManager().getInstanceName();
      if (leaderName == null || !leaderName.equals(instanceName))
      {
        LOG.warn("leader name does NOT match, my name: " + instanceName + ", leader: "
            + leader);
        return;
      }
    }

    PauseSignal pauseSignal = accessor.getProperty(keyBuilder.pause());
    if (pauseSignal != null)
    {
      _paused = true;
      LOG.info("controller is now paused");
    }
    else
    {
      if (_paused)
      {
        // it currently paused
        LOG.info("controller is now resumed");
        _paused = false;
        ClusterEvent event = new ClusterEvent("resume");
        event.addAttribute("changeContext", changeContext);
        event.addAttribute("helixmanager", changeContext.getManager());
        event.addAttribute("eventData", pauseSignal);
        handleEvent(event);
      }
      else
      {
        _paused = false;
      }
    }
    LOG.info("END: GenericClusterController.onControllerChange()");
  }

  /**
   * Go through the list of liveinstances in the cluster, and add currentstateChange
   * listener and Message listeners to them if they are newly added. For current state
   * change, the observation is tied to the session id of each live instance.
   *
   */
  protected void checkLiveInstancesObservation(List<LiveInstance> liveInstances,
                                               NotificationContext changeContext)
  {
	// construct maps for current live-instances
	Map<String, LiveInstance> curInstances = new HashMap<String, LiveInstance>();
	Map<String, LiveInstance> curSessions = new HashMap<String, LiveInstance>();
	for(LiveInstance liveInstance : liveInstances) {
		curInstances.put(liveInstance.getInstanceName(), liveInstance);
		curSessions.put(liveInstance.getSessionId(), liveInstance);
	}
	  
	Map<String, LiveInstance> lastInstances = _lastSeenInstances.get();
	Map<String, LiveInstance> lastSessions = _lastSeenSessions.get();
	
    HelixManager manager = changeContext.getManager();
    Builder keyBuilder = new Builder(manager.getClusterName());
    if (lastSessions != null) {
    	for (String session : lastSessions.keySet()) {
    		if (!curSessions.containsKey(session)) {
    			// remove current-state listener for expired session
    		    String instanceName = lastSessions.get(session).getInstanceName();
    			manager.removeListener(keyBuilder.currentStates(instanceName, session), this); 
    		}
    	}
    }
    
    if (lastInstances != null) {
    	for (String instance : lastInstances.keySet()) {
    		if (!curInstances.containsKey(instance)) {
    			// remove message listener for disconnected instances
    			manager.removeListener(keyBuilder.messages(instance), this);
    		}
    	}
    }
    
	for (String session : curSessions.keySet()) {
		if (lastSessions == null || !lastSessions.containsKey(session)) {
	      String instanceName = curSessions.get(session).getInstanceName();
          try {
            // add current-state listeners for new sessions
	        manager.addCurrentStateChangeListener(this, instanceName, session);
	        LOG.info("Succeed in addling current state listener for instance: " + instanceName + " with session: " + session);

          } catch (Exception e) {
        	  LOG.error("Fail to add current state listener for instance: "
        		  + instanceName + " with session: " + session, e);
          }
		}
	}
	
	for (String instance : curInstances.keySet()) {
		if (lastInstances == null || !lastInstances.containsKey(instance)) {
	        try {
	          // add message listeners for new sessions
	          manager.addMessageListener(this, instance);
	          LOG.info("Succeed in adding message listener for " + instance);
	        }
	        catch (Exception e)
	        {
	          LOG.error("Fail to add message listener for instance:" + instance, e);
	        }
		}
	}
	
	// update last-seen
	_lastSeenInstances.set(curInstances);
	_lastSeenSessions.set(curSessions);
  }

}
