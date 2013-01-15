/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.participant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixConstants;
import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixException;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.messaging.handling.HelixBatchMessageHandler;
import com.linkedin.helix.messaging.handling.HelixStateTransitionHandler;
import com.linkedin.helix.messaging.handling.HelixTaskExecutor;
import com.linkedin.helix.messaging.handling.MessageHandler;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.Attributes;
import com.linkedin.helix.model.Message.MessageType;
import com.linkedin.helix.model.StateModelDefinition;
import com.linkedin.helix.participant.statemachine.StateModel;
import com.linkedin.helix.participant.statemachine.StateModelFactory;
import com.linkedin.helix.participant.statemachine.StateModelParser;

public class HelixStateMachineEngine implements StateMachineEngine {
	private static Logger LOG = Logger.getLogger(HelixStateMachineEngine.class);

	// StateModelName->FactoryName->StateModelFactory
	private final Map<String, Map<String, StateModelFactory<? extends StateModel>>> _stateModelFactoryMap;
	StateModelParser _stateModelParser;

	private final HelixManager _manager;

	private final ConcurrentHashMap<String, StateModelDefinition> _stateModelDefs;

	public StateModelFactory<? extends StateModel> getStateModelFactory(String stateModelName) {
		return getStateModelFactory(stateModelName, HelixConstants.DEFAULT_STATE_MODEL_FACTORY);
	}

	public StateModelFactory<? extends StateModel> getStateModelFactory(String stateModelName,
	        String factoryName) {
		if (!_stateModelFactoryMap.containsKey(stateModelName)) {
			return null;
		}
		return _stateModelFactoryMap.get(stateModelName).get(factoryName);
	}

	public HelixStateMachineEngine(HelixManager manager) {
		_stateModelParser = new StateModelParser();
		_manager = manager;

		_stateModelFactoryMap = new ConcurrentHashMap<String, Map<String, StateModelFactory<? extends StateModel>>>();
		_stateModelDefs = new ConcurrentHashMap<String, StateModelDefinition>();
	}

	@Override
	public boolean registerStateModelFactory(String stateModelDef,
	        StateModelFactory<? extends StateModel> factory) {
		return registerStateModelFactory(stateModelDef, factory,
		        HelixConstants.DEFAULT_STATE_MODEL_FACTORY);
	}

	@Override
	public boolean registerStateModelFactory(String stateModelName,
	        StateModelFactory<? extends StateModel> factory, String factoryName) {
		if (stateModelName == null || factory == null || factoryName == null) {
			throw new HelixException("stateModelDef|stateModelFactory|factoryName cannot be null");
		}

		LOG.info("Register state model factory for state model " + stateModelName
		        + " using factory name " + factoryName + " with " + factory);

		if (!_stateModelFactoryMap.containsKey(stateModelName)) {
			_stateModelFactoryMap.put(stateModelName,
			        new ConcurrentHashMap<String, StateModelFactory<? extends StateModel>>());
		}

		if (_stateModelFactoryMap.get(stateModelName).containsKey(factoryName)) {
			LOG.warn("stateModelFactory for " + stateModelName + " using factoryName "
			        + factoryName + " has already been registered.");
			return false;
		}

		_stateModelFactoryMap.get(stateModelName).put(factoryName, factory);
		sendNopMessage();
		return true;
	}

	// TODO: duplicated code in DefaultMessagingService
	private void sendNopMessage() {
		if (_manager.isConnected()) {
			try {
				Message nopMsg = new Message(MessageType.NO_OP, UUID.randomUUID().toString());
				nopMsg.setSrcName(_manager.getInstanceName());

				HelixDataAccessor accessor = _manager.getHelixDataAccessor();
				Builder keyBuilder = accessor.keyBuilder();

				if (_manager.getInstanceType() == InstanceType.CONTROLLER
				        || _manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT) {
					nopMsg.setTgtName("Controller");
					accessor.setProperty(keyBuilder.controllerMessage(nopMsg.getId()), nopMsg);
				}

				if (_manager.getInstanceType() == InstanceType.PARTICIPANT
				        || _manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT) {
					nopMsg.setTgtName(_manager.getInstanceName());
					accessor.setProperty(keyBuilder.message(nopMsg.getTgtName(), nopMsg.getId()),
					        nopMsg);
				}
				LOG.info("Send NO_OP message to " + nopMsg.getTgtName() + ", msgId: "
				        + nopMsg.getId());
			} catch (Exception e) {
				LOG.error(e);
			}
		}
	}

	@Override
	public void reset() {
		for (Map<String, StateModelFactory<? extends StateModel>> ftyMap : _stateModelFactoryMap
		        .values()) {
			for (StateModelFactory<? extends StateModel> stateModelFactory : ftyMap.values()) {
				Map<String, ? extends StateModel> modelMap = stateModelFactory.getStateModelMap();
				if (modelMap == null || modelMap.isEmpty()) {
					continue;
				}

				for (String resourceKey : modelMap.keySet()) {
					StateModel stateModel = modelMap.get(resourceKey);
					stateModel.reset();
					String initialState = _stateModelParser.getInitialState(stateModel.getClass());
					stateModel.updateState(initialState);
					// TODO probably should update the state on ZK. Shi confirm
					// what needs
					// to be done here.
				}
			}
		}
	}

	@Override
	public MessageHandler createHandler(Message message, NotificationContext context) {
		String type = message.getMsgType();

		if (!type.equals(MessageType.STATE_TRANSITION.toString())) {
			throw new HelixException("Unexpected msg type for message " + message.getMsgId()
			        + " type:" + message.getMsgType());
		}

		// String partitionKey = message.getPartitionName();
		// List<String> partitionKeys = message.getExePartitionNames();
		List<String> partitionKeys = message.getPartitionNames();

		String stateModelName = message.getStateModelDef();
		String resourceName = message.getResourceName();
		String sessionId = message.getTgtSessionId();
		int bucketSize = message.getBucketSize();

		if (stateModelName == null) {
			LOG.error("message does not contain stateModelDef");
			return null;
		}

		String factoryName = message.getStateModelFactoryName();
		if (factoryName == null) {
			factoryName = HelixConstants.DEFAULT_STATE_MODEL_FACTORY;
		}

		StateModelFactory stateModelFactory = getStateModelFactory(stateModelName, factoryName);
		if (stateModelFactory == null) {
			LOG.warn("Cannot find stateModelFactory for model:" + stateModelName
			        + " using factoryName:" + factoryName + " for resourceGroup:" + resourceName);
			return null;
		}

		// check if the state model definition exists and cache it
		if (!_stateModelDefs.containsKey(stateModelName)) {
			HelixDataAccessor accessor = _manager.getHelixDataAccessor();
			Builder keyBuilder = accessor.keyBuilder();
			StateModelDefinition stateModelDef = accessor.getProperty(keyBuilder
			        .stateModelDef(stateModelName));
			if (stateModelDef == null) {
				throw new HelixException("stateModelDef for " + stateModelName + " does NOT exists");
			}
			_stateModelDefs.put(stateModelName, stateModelDef);
		}

		// create currentStateDelta for this partition
		String initState = _stateModelDefs.get(message.getStateModelDef()).getInitialState();

		CurrentState currentStateDelta = new CurrentState(resourceName);
		currentStateDelta.setSessionId(sessionId);
		currentStateDelta.setStateModelDefRef(stateModelName);
		currentStateDelta.setStateModelFactoryName(factoryName);
		currentStateDelta.setBucketSize(bucketSize);

		// StateModel stateModel =
		// stateModelFactory.getStateModel(partitionKey);
		List<StateModel> stateModels = stateModelFactory.getStateModel(partitionKeys);
		for (int i = 0; i < partitionKeys.size(); i++) {
			StateModel stateModel = stateModels.get(i);
			String partitionKey = partitionKeys.get(i);
			if (stateModel == null) {
				stateModelFactory.createAndAddStateModel(partitionKey);
				stateModel = stateModelFactory.getStateModel(partitionKey);
				stateModel.updateState(initState);
			}

			String state = (stateModel.getCurrentState() == null) ? initState : stateModel.getCurrentState();
			currentStateDelta.setState(partitionKey, state);
//			System.err.println("set currentStateDelta: " + partitionKey + ", " + state);
		}

		stateModels = stateModelFactory.getStateModel(partitionKeys);

		// currentStateDelta.setState(partitionKey,
		// (stateModel.getCurrentState() == null)
		// ? initState : stateModel.getCurrentState());

		HelixTaskExecutor executor = (HelixTaskExecutor) context
		        .get(NotificationContext.TASK_EXECUTOR_KEY);

		if (message.getGroupMessageMode() == false) {
			message.setBatchPartitionNames(Arrays.asList(message.getPartitionName()));
			return new HelixStateTransitionHandler(stateModels, message, context, currentStateDelta,
		        executor);
		} else {
			// create all sub-handlers and a batch message handler
			List<MessageHandler> subHandlers = new ArrayList<MessageHandler>();
			List<Message> subMsgs = new ArrayList<Message>();

			List<String> partitionNames = message.getPartitionNames();
			int exeBatchSize = message.getExeBatchSize();
			int partitionNb = partitionNames.size();
			for (int i = 0; i < partitionNb; i += exeBatchSize) {
				Message subMsg = new Message(message.getRecord());
				subMsg.setPartitionName(partitionNames.get(i));
				subMsg.setAttribute(Attributes.PARENT_MSG_ID, message.getId());

				LOG.info("Creating handler for group message " + subMsg.getMsgId() + "/"
				        + partitionNames.get(i));

				if (i + exeBatchSize <= partitionNb) {
					subMsg.setBatchPartitionNames(partitionNames.subList(i, i + exeBatchSize));

				} else {
					subMsg.setBatchPartitionNames(partitionNames.subList(i, partitionNb));
				}

				MessageHandler handler = new HelixStateTransitionHandler(stateModels, subMsg, context, currentStateDelta,
				        executor);
//				System.err.println("create handler " + handler + " for " + partitionNames.get(i));
				if (handler != null) {
					subHandlers.add(handler);
					subMsgs.add(subMsg);
				}
				
			}

			return new HelixBatchMessageHandler(stateModels, message, context, currentStateDelta, executor, subHandlers, subMsgs);
		}
	}

	@Override
	public String getMessageType() {
		return MessageType.STATE_TRANSITION.toString();
	}

	@Override
	public boolean removeStateModelFactory(String stateModelDef,
	        StateModelFactory<? extends StateModel> factory) {
		throw new UnsupportedOperationException("Remove not yet supported");
	}

	@Override
	public boolean removeStateModelFactory(String stateModelDef,
	        StateModelFactory<? extends StateModel> factory, String factoryName) {
		throw new UnsupportedOperationException("Remove not yet supported");
	}
}
