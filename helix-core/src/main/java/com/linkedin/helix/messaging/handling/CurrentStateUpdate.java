package com.linkedin.helix.messaging.handling;

import com.linkedin.helix.PropertyKey;
import com.linkedin.helix.model.CurrentState;

public class CurrentStateUpdate {
    public final PropertyKey  _key;
    public final CurrentState _curStateDelta;

    public CurrentStateUpdate(PropertyKey key, CurrentState curStateDelta)
    {
      _key = key;
      _curStateDelta = curStateDelta;
    }

    public void merge(CurrentState curState)
    {
      _curStateDelta.getRecord().merge(curState.getRecord());
    }

}
