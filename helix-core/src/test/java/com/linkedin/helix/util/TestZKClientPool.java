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
package com.linkedin.helix.util;

import java.util.Date;

import org.I0Itec.zkclient.ZkServer;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZkClient;

public class TestZKClientPool
{

  @Test
  public void test() throws Exception
  {
	final String zkAddr = "localhost:2179";
	ZkServer zkServer = null;

	try
	{
		
	
    Logger.getRootLogger().setLevel(Level.INFO);

    String testName = "TestZKClientPool";
    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    zkServer = TestHelper.startZkSever(zkAddr);
    ZKClientPool.reset();
    ZkClient zkClient = ZKClientPool.getZkClient(zkAddr);
    
    zkClient.createPersistent("/" + testName, new ZNRecord(testName));
    ZNRecord record = zkClient.readData("/" + testName);
    Assert.assertEquals(record.getId(), testName);
    
    TestHelper.stopZkServer(zkServer);
    
    // restart zk 
    zkServer = TestHelper.startZkSever(zkAddr);
    try
    {
      zkClient = ZKClientPool.getZkClient(zkAddr);
      record = zkClient.readData("/" + testName);
      Assert.fail("should fail on zk no node exception");
    } catch (ZkNoNodeException e)
    {
      // OK
    } catch (Exception e)
    {
      Assert.fail("should not fail on exception other than ZkNoNodeException");
    }
    
    zkClient.createPersistent("/" + testName, new ZNRecord(testName));
    record = zkClient.readData("/" + testName);
    Assert.assertEquals(record.getId(), testName);
    
    zkClient.close();
    TestHelper.stopZkServer(zkServer);
    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));
    
	} catch (Exception e) {
		e.printStackTrace();
	} finally {
	    TestHelper.stopZkServer(zkServer);
	    Logger.getRootLogger().setLevel(Level.ERROR);
	}
  }
}
