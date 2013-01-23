package com.linkedin.helix.mock.storage;

import com.linkedin.helix.participant.statemachine.StateModelFactory;

public class MockSchemataModelFactory extends StateModelFactory<MockSchemataStateModel> {
    @Override
    public MockSchemataStateModel createNewStateModel(String partitionKey)
    {
      MockSchemataStateModel model = new MockSchemataStateModel();
      return model;
    }

}
