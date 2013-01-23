package com.linkedin.helix.mock.storage;

import com.linkedin.helix.participant.statemachine.StateModelFactory;

public class MockBootstrapModelFactory extends StateModelFactory<MockBootstrapStateModel> {
	@Override
	public MockBootstrapStateModel createNewStateModel(String partitionKey) {
		MockBootstrapStateModel model = new MockBootstrapStateModel();
		return model;
	}
}
