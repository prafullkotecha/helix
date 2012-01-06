package com.linkedin.clustermanager.controller.stages;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.ExternalView;
import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.model.ResourceGroup;
import com.linkedin.clustermanager.model.ResourceKey;
import com.linkedin.clustermanager.model.StateModelDefinition;
import com.linkedin.clustermanager.pipeline.AbstractBaseStage;
import com.linkedin.clustermanager.pipeline.StageException;

public class ExternalViewComputeStage extends AbstractBaseStage
{
  private static Logger log = Logger.getLogger(ExternalViewComputeStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    ClusterManager manager = event.getAttribute("clustermanager");
    if (manager == null)
    {
      throw new StageException("ClusterManager attribute value is null");
    }
    log.info("START ExternalViewComputeStage.process()");
    ClusterDataAccessor dataAccessor = manager.getDataAccessor();
    Map<String, ResourceGroup> resourceGroupMap = event
        .getAttribute(AttributeName.RESOURCE_GROUPS.toString());
    if (resourceGroupMap == null)
    {
      throw new StageException("ResourceGroupMap attribute value is null");
    }

    CurrentStateOutput currentStateOutput = event
        .getAttribute(AttributeName.CURRENT_STATE.toString());
    
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");
    if (cache == null)
    {
      throw new StageException("ClusterDataCache attribute value is null");
    }
    Map<String, IdealState> idealStateMap = cache.getIdealStates();
    
    for (String resourceGroupName : resourceGroupMap.keySet())
    {
      ZNRecord viewRecord = new ZNRecord(resourceGroupName);
      ExternalView view = new ExternalView(viewRecord);
      ResourceGroup resourceGroup = resourceGroupMap.get(resourceGroupName);
      IdealState idealState = idealStateMap.get(resourceGroupName);
      
      for (ResourceKey resourceKey : resourceGroup.getResourceKeys())
      {
        Map<String, String> resourceKeyExternalViewStateMap = new HashMap<String, String>();
        // If idealstate is present for the resource group, fill up with default state for
        // each instance
        if(idealState != null)
        {
          String stateModelName = idealState.getStateModelDefRef();
          StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelName); 
          String initialState = stateModelDef.getInitialState();
          for(String instanceName : idealState.getInstanceStateMap(resourceKey.getResourceKeyName()).keySet())
          {
            resourceKeyExternalViewStateMap.put(instanceName, initialState);
          }
        }
        // Then fill in the actual current states
        Map<String, String> currentStateMap = currentStateOutput
            .getCurrentStateMap(resourceGroupName, resourceKey);
        if (currentStateMap != null && currentStateMap.size() > 0)
        {
          resourceKeyExternalViewStateMap.putAll(currentStateMap);
        }
        view.setStateMap(resourceKey.getResourceKeyName(), resourceKeyExternalViewStateMap);
      }
      dataAccessor.setProperty(PropertyType.EXTERNALVIEW,
          view.getRecord(), resourceGroupName);
    }
    log.info("END ExternalViewComputeStage.process()");
  }

}
