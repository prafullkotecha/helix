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
package com.linkedin.helix.integration;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.controller.restlet.ZKPropertyTransferServer;
import com.linkedin.helix.controller.restlet.ZkPropertyTransferClient;
import com.linkedin.helix.model.StatusUpdate;
/**
 * 
 * setup a storage cluster and start a zk-based cluster controller in stand-alone mode
 * start 5 dummy participants verify the current states at end
 */

public class ZkStandAloneCMTestBaseWithPropertyServerCheck extends ZkStandAloneCMTestBase
{
  @BeforeClass
  public void beforeClass() throws Exception
  {
    ZKPropertyTransferServer.PERIOD = 500;
    ZkPropertyTransferClient.SEND_PERIOD = 500;
    ZKPropertyTransferServer.getInstance().init(19999, ZK_ADDR);
    super.beforeClass();
    
    Thread.sleep(1000);
    for (int i = 0; i < NODE_NR; i++)
    {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      if (_startCMResultMap.get(instanceName) != null)
      {
        HelixDataAccessor accessor = _startCMResultMap.get(instanceName)._manager.getHelixDataAccessor();
        Builder kb = accessor.keyBuilder();
        List<StatusUpdate> statusUpdates = accessor.getChildValues(
            kb.stateTransitionStatus(instanceName, _startCMResultMap.get(instanceName)._manager.getSessionId(),
                TEST_DB));
        Assert.assertTrue(statusUpdates.size() > 0);
        for(StatusUpdate update : statusUpdates)
        {
          Assert.assertTrue(update.getRecord().getSimpleField(ZkPropertyTransferClient.USE_PROPERTYTRANSFER).equals("true"));
          Assert.assertTrue(update.getRecord().getSimpleField(ZKPropertyTransferServer.SERVER) != null);
        }
      }
    }
  }

  @AfterClass
  public void afterClass() throws Exception
  {
    super.afterClass();
    ZKPropertyTransferServer.getInstance().shutdown();
    ZKPropertyTransferServer.getInstance().reset();
  }
}
