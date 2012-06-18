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
package com.linkedin.helix.controller.stages;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.controller.pipeline.AbstractBaseStage;
import com.linkedin.helix.controller.pipeline.StageException;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Resource;
import com.linkedin.helix.model.Partition;

public class TaskAssignmentStage extends AbstractBaseStage
{
  private static Logger logger = Logger.getLogger(TaskAssignmentStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    HelixManager manager = event.getAttribute("helixmanager");
    Map<String, Resource> resourceMap =
        event.getAttribute(AttributeName.RESOURCES.toString());
    MessageSelectionStageOutput messageSelectionStageOutput =
        event.getAttribute(AttributeName.MESSAGES_SELECTED.toString());

    if (manager == null || resourceMap == null || messageSelectionStageOutput == null)
    {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires HelixManager|RESOURCES|MESSAGES_SELECTED");
    }

    DataAccessor dataAccessor = manager.getDataAccessor();
    List<Message> allMsg = new ArrayList<Message>();
    for (String resourceName : resourceMap.keySet())
    {
      Resource resource = resourceMap.get(resourceName);
      for (Partition partition : resource.getPartitions())
      {
        List<Message> messages =
            messageSelectionStageOutput.getMessages(resourceName, partition);
        allMsg.addAll(messages);
      }
    }
    sendMessages(dataAccessor, allMsg);
  }

  protected void sendMessages(DataAccessor dataAccessor, List<Message> messages)
  {
    if (messages == null || messages.size() == 0)
    {
      return;
    }
    if (true)
    {
      Map<String, List<Message>> groupedMessages = new HashMap<String, List<Message>>();
      for (Message message : messages)
      {
        String key =
            message.getTgtName() + "_" + message.getFromState() + "_"
                + message.getToState();
        if (!groupedMessages.containsKey(key))
        {
          groupedMessages.put(key, new ArrayList<Message>());
        }
        groupedMessages.get(key).add(message);
      }
      for (String key : groupedMessages.keySet())
      {
        List<Message> list = groupedMessages.get(key);
        StringBuffer sb = new StringBuffer();
        String delim = "";
        Message groupedMessage = null;
        for (Message message : list)
        {
          if (groupedMessage == null)
          {
            groupedMessage =
                new Message(message.getRecord(), UUID.randomUUID().toString());
          }
          sb.append(delim).append(message.getPartitionName());
          delim = ",";
        }
        if (groupedMessage != null)
        {
          groupedMessage.setPartitionName(sb.toString());
         System.out.println("Sending message to " + groupedMessage.getTgtName() + " transition "
              + groupedMessage.getPartitionName() + " from:" + groupedMessage.getFromState() + " to:"
              + groupedMessage.getToState());
          logger.info("Sending message to " + groupedMessage.getTgtName() + " transition "
              + groupedMessage.getPartitionName() + " from:" + groupedMessage.getFromState() + " to:"
              + groupedMessage.getToState());
          
          dataAccessor.setProperty(PropertyType.MESSAGES,
                                   groupedMessage,
                                   groupedMessage.getTgtName(),
                                   groupedMessage.getId());
        }
      }
    }
    else
    {

      for (Message message : messages)
      {
        logger.info("Sending message to " + message.getTgtName() + " transition "
            + message.getPartitionName() + " from:" + message.getFromState() + " to:"
            + message.getToState());
        dataAccessor.setProperty(PropertyType.MESSAGES,
                                 message,
                                 message.getTgtName(),
                                 message.getId());
      }
    }
  }
}
