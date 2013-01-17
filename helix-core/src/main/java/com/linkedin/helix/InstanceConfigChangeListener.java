package com.linkedin.helix;

import java.util.List;

import com.linkedin.helix.model.InstanceConfig;

public interface InstanceConfigChangeListener 
{
  /**
   * Invoked when participant config changes
   * 
   * @param configs
   * @param changeContext
   */
  public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context);
}
