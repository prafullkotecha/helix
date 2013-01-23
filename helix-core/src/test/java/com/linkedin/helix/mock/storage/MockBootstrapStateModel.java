package com.linkedin.helix.mock.storage;

import org.apache.log4j.Logger;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.participant.statemachine.StateModel;
import com.linkedin.helix.participant.statemachine.StateModelInfo;
import com.linkedin.helix.participant.statemachine.Transition;

@StateModelInfo(initialState = "OFFLINE", states = { "ONLINE", "BOOTSTRAP", "OFFLINE", "IDLE" })
public class MockBootstrapStateModel extends StateModel {
	private static Logger LOG = Logger.getLogger(MockBootstrapStateModel.class);

	// Overwrite the default value of initial state
	MockBootstrapStateModel() {
		_currentState = "IDLE";
	}

	@Transition(to = "OFFLINE", from = "IDLE")
	public void onBecomeOfflineFromIdle(Message message, NotificationContext context) {
		LOG.info("Become OFFLINE from IDLE");
	}

	@Transition(to = "BOOTSTRAP", from = "OFFLINE")
	public void onBecomeBootstrapFromOffline(Message message, NotificationContext context) {
		LOG.info("Become BOOTSTRAP from OFFLINE");
	}

	@Transition(to = "ONLINE", from = "BOOSTRAP")
	public void onBecomeOnlineFromBootstrap(Message message, NotificationContext context) {
		LOG.info("Become ONLINE from BOOTSTRAP");
	}

	@Transition(to = "OFFLINE", from = "ONLINE")
	public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
		LOG.info("Become OFFLINE from ONLINE");
	}
}