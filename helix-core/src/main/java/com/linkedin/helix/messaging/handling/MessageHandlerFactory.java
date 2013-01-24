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
package com.linkedin.helix.messaging.handling;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;

public interface MessageHandlerFactory {
	/**
	 * create message handler
	 * @param message
	 * @param context
	 * @return
	 */
	public MessageHandler createHandler(Message message, NotificationContext context);

	/**
	 * message type the factory handles
	 * @return
	 */
	public String getMessageType();

	/**
	 * reset factory
	 */
	public void reset();
}
