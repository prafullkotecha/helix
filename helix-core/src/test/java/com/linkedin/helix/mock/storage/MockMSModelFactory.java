package com.linkedin.helix.mock.storage;

import java.util.Map;

import com.linkedin.helix.participant.statemachine.StateModelFactory;

public class MockMSModelFactory extends StateModelFactory<MockMSStateModel> {
    final MockTransition _transition;

    public MockMSModelFactory()
    {
      this(null);
    }

    public MockMSModelFactory(MockTransition transition)
    {
      _transition = transition;
    }

    public void setTrasition(MockTransition transition)
    {
      Map<String, MockMSStateModel> stateModelMap = getStateModelMap();
      for (MockMSStateModel stateModel : stateModelMap.values())
      {
        stateModel.setTransition(transition);
      }
    }

    @Override
    public MockMSStateModel createNewStateModel(String partitionKey)
    {
      MockMSStateModel model = new MockMSStateModel(_transition);

      return model;
    }
}
