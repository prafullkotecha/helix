package com.linkedin.helix.mock.storage;

import org.apache.log4j.Logger;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.participant.statemachine.StateModel;
import com.linkedin.helix.participant.statemachine.StateModelInfo;
import com.linkedin.helix.participant.statemachine.Transition;

// mock master-slave state model
@StateModelInfo(initialState = "OFFLINE", states = { "MASTER", "SLAVE", "ERROR" })
public class MockMSStateModel extends StateModel {
	private static Logger LOG = Logger.getLogger(MockMSStateModel.class);

	protected MockTransition _transition;

	public MockMSStateModel(MockTransition transition) {
		_transition = transition;
	}

	public void setTransition(MockTransition transition) {
		_transition = transition;
	}

	@Transition(to = "SLAVE", from = "OFFLINE")
	public void onBecomeSlaveFromOffline(Message message, NotificationContext context)
	        throws InterruptedException {
		LOG.info("Become SLAVE from OFFLINE");
		// System.out.println("\tBecome SLAVE from OFFLINE, " + message.getPartitionName());
		if (_transition != null) {
			_transition.doTransition(message, context);

		}
	}

	@Transition(to = "MASTER", from = "SLAVE")
	public void onBecomeMasterFromSlave(Message message, NotificationContext context)
	        throws InterruptedException {
		LOG.info("Become MASTER from SLAVE");
		// System.out.println("\tBecome MASTER from SLAVE, " + message.getPartitionName());

		if (_transition != null) {
			_transition.doTransition(message, context);
		}
	}

	@Transition(to = "SLAVE", from = "MASTER")
	public void onBecomeSlaveFromMaster(Message message, NotificationContext context)
	        throws InterruptedException {
		LOG.info("Become SLAVE from MASTER");
		// System.out.println("\tBecome SLAVE from MASTER, " + message.getPartitionName());

		if (_transition != null) {
			_transition.doTransition(message, context);
		}
	}

	@Transition(to = "OFFLINE", from = "SLAVE")
	public void onBecomeOfflineFromSlave(Message message, NotificationContext context)
	        throws InterruptedException {
		LOG.info("Become OFFLINE from SLAVE");
		if (_transition != null) {
			_transition.doTransition(message, context);
		}
	}

	@Transition(to = "DROPPED", from = "OFFLINE")
	public void onBecomeDroppedFromOffline(Message message, NotificationContext context)
	        throws InterruptedException {
		LOG.info("Become DROPPED from OFFLINE");
		if (_transition != null) {
			_transition.doTransition(message, context);
		}
	}

	@Transition(to = "OFFLINE", from = "ERROR")
	public void onBecomeOfflineFromError(Message message, NotificationContext context)
	        throws InterruptedException {
		LOG.info("Become OFFLINE from ERROR");
		// System.err.println("Become OFFLINE from ERROR");
		if (_transition != null) {
			_transition.doTransition(message, context);
		}
	}

	@Override
	public void reset() {
		LOG.info("Default MockMSStateModel.reset() invoked");
		if (_transition != null) {
			_transition.doReset();
		}
	}
}
