package com.linkedin.helix;

import java.util.List;

public interface ScopedConfigChangeListener 
{
    /**
     * Invoked when configs of a scope (cluster, participant, or resource) change
     * 
     * @param configs
     * @param context
     */
    public void onConfigChange(List<HelixProperty> configs, NotificationContext context);
}
