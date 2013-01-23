package com.linkedin.helix.mock.storage;

import org.apache.log4j.Logger;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.participant.statemachine.StateModel;
import com.linkedin.helix.participant.statemachine.StateModelInfo;
import com.linkedin.helix.participant.statemachine.Transition;

@StateModelInfo(initialState = "OFFLINE", states = { "MASTER", "DROPPED", "ERROR" })
public class MockSchemataStateModel extends StateModel {
	private static Logger LOG = Logger.getLogger(MockSchemataStateModel.class);

	@Transition(to = "MASTER", from = "OFFLINE")
	public void onBecomeMasterFromOffline(Message message, NotificationContext context) {
		LOG.info("Become MASTER from OFFLINE");
	}

	@Transition(to = "OFFLINE", from = "MASTER")
	public void onBecomeOfflineFromMaster(Message message, NotificationContext context) {
		LOG.info("Become OFFLINE from MASTER");
	}

	@Transition(to = "DROPPED", from = "OFFLINE")
	public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
		LOG.info("Become DROPPED from OFFLINE");
	}

	@Transition(to = "OFFLINE", from = "ERROR")
	public void onBecomeOfflineFromError(Message message, NotificationContext context) {
		LOG.info("Become OFFLINE from ERROR");
	}
}