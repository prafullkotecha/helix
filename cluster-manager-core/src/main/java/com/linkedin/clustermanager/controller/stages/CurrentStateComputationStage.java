package com.linkedin.clustermanager.controller.stages;

import java.util.List;
import java.util.Map;

import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.model.CurrentState;
import com.linkedin.clustermanager.model.LiveInstance;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.MessageType;
import com.linkedin.clustermanager.model.ResourceGroup;
import com.linkedin.clustermanager.model.ResourceKey;
import com.linkedin.clustermanager.model.StateModelDefinition;
import com.linkedin.clustermanager.pipeline.AbstractBaseStage;
import com.linkedin.clustermanager.pipeline.StageException;

/**
 * For each LiveInstances select currentState and message whose sessionId
 * matches sessionId from LiveInstance Get ResourceKey,State for all the
 * resources computed in previous State [ResourceComputationStage]
 *
 * @author kgopalak
 *
 */
public class CurrentStateComputationStage extends AbstractBaseStage
{
  @Override
  public void process(ClusterEvent event) throws Exception
  {
    ClusterManager manager = event.getAttribute("clustermanager");
    if (manager == null)
    {
      throw new StageException("clustermanager attribute value is null");
    }
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");

    Map<String, LiveInstance> liveInstances = cache.getLiveInstances();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    Map<String, ResourceGroup> resourceGroupMap = event
        .getAttribute(AttributeName.RESOURCE_GROUPS.toString());

    for (LiveInstance instance : liveInstances.values())
    {
      String instanceName = instance.getInstanceName();
      List<Message> instanceMessages;
      instanceMessages = cache.getMessages(instanceName);
      for (Message message  : instanceMessages)
      {
        if (!MessageType.STATE_TRANSITION.toString().equalsIgnoreCase(
            message.getMsgType()))
        {
          continue;
        }
        if (!instance.getSessionId().equals(message.getTgtSessionId()))
        {
          continue;
        }
        String resourceGroupName = message.getResourceGroupName();
        ResourceGroup resourceGroup = resourceGroupMap.get(resourceGroupName);
        if (resourceGroup == null)
        {
          continue;
        }
        ResourceKey resourceKey = resourceGroup.getResourceKey(message
            .getResourceKey());
        if (resourceKey == null)
        {
          // log
          continue;
        }

        CurrentState curStates =
            cache.getCurrentState(instanceName, instance.getSessionId())
                 .get(resourceGroupName);
        String curState = curStates == null? null : curStates.getState(resourceKey.getResourceKeyName());
        if (curState == null)
        {
          StateModelDefinition stateModelDef =
              cache.getStateModelDef(resourceGroup.getStateModelDefRef());
          curState = stateModelDef.getInitialState();
        }

        if (curState.equalsIgnoreCase(message.getFromState()))
        {
          currentStateOutput.setPendingState(resourceGroupName, resourceKey,
              instanceName, message.getToState());
        }
      }
    }
    for (LiveInstance instance : liveInstances.values())
    {
      String instanceName = instance.getInstanceName();	// record.getId();

      String clientSessionId = instance.getSessionId();
      Map<String, CurrentState> currentStateMap = cache.getCurrentState(instanceName, clientSessionId);
      for (CurrentState currentState : currentStateMap.values())
      {

        if (!instance.getSessionId().equals(currentState.getSessionId()))
        {
          continue;
        }
        String resourceGroupName = currentState.getResourceGroupName();
        String stateModelDefName = currentState.getStateModelDefRef();
        ResourceGroup resourceGroup = resourceGroupMap.get(resourceGroupName);
        if (resourceGroup == null)
        {
          continue;
        }
        if (stateModelDefName != null)
        {
          currentStateOutput.setResourceGroupStateModelDef(resourceGroupName,
              stateModelDefName);
        }
        Map<String, String> resourceKeyStateMap = currentState
            .getResourceKeyStateMap();
        for (String resourceKeyStr : resourceKeyStateMap.keySet())
        {
          ResourceKey resourceKey = resourceGroup
              .getResourceKey(resourceKeyStr);
          if (resourceKey != null)
          {
            currentStateOutput.setCurrentState(resourceGroupName, resourceKey,
                instanceName, currentState.getState(resourceKeyStr));

          } else
          {
            // log
          }
        }
      }
    }
    event.addAttribute(AttributeName.CURRENT_STATE.toString(),
        currentStateOutput);
  }
}
