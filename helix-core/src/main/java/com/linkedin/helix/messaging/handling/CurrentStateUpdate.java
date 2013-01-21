package com.linkedin.helix.messaging.handling;

import com.linkedin.helix.PropertyKey;
import com.linkedin.helix.model.CurrentState;

public class CurrentStateUpdate {
	final PropertyKey _key;
	final CurrentState _delta;
	
	CurrentStateUpdate(PropertyKey key, CurrentState delta)
	{
		_key = key;
		_delta = delta;
	}
	
	public void merge(CurrentState anotherDelta)
	{
		_delta.getRecord().merge(anotherDelta.getRecord());
	}

}
