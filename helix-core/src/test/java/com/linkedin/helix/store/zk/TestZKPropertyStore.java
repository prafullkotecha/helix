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
package com.linkedin.helix.store.zk;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.store.PropertyJsonComparator;
import com.linkedin.helix.store.PropertyJsonSerializer;
import com.linkedin.helix.store.PropertyListener;
import com.linkedin.helix.store.PropertySerializer;
import com.linkedin.helix.store.PropertyStore;
import com.linkedin.helix.store.PropertyStoreException;

// TODO need to write performance test for zk-property store
public class TestZKPropertyStore extends ZkUnitTestBase
{
  private static final Logger LOG = Logger.getLogger(TestZKPropertyStore.class);
  final String className = getShortClassName();
  final int bufSize = 128;
  final int mapNr = 10;
  final int firstLevelNr = 10;
  final int secondLevelNr = 10;
  final int totalNodes = firstLevelNr * secondLevelNr;

  class TestListener implements PropertyListener<ZNRecord>
  {
    Map<String, String> _keySet;
    public TestListener(Map<String, String> keySet)
    {
      _keySet = keySet;
    }

    @Override
    public void onPropertyChange(String key)
    {
      long now = System.currentTimeMillis();
      _keySet.put(key, Long.toString(now));
    }

    @Override
    public void onPropertyCreate(String key)
    {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void onPropertyDelete(String key)
    {
      // TODO Auto-generated method stub
      
    }
  }
  
  private class TestUpdater implements DataUpdater<ZNRecord>
  {
    @Override
    public ZNRecord update(ZNRecord current)
    {
      char[] data = new char[bufSize];

      for (int i = 0; i < bufSize; i++)
      {
        data[i] = 'e';
      }

      Map<String, String> map = new TreeMap<String, String>();
      for (int i = 0; i < mapNr; i++)
      {
        map.put("key_" + i, new String(data));
      }

      String nodeId = current.getId();
      ZNRecord record = new ZNRecord(nodeId);
      record.setSimpleFields(map);
      return record;
    }
  }

  String getNodeId(int i, int j)
  {
    return "childNode_" + i + "_" + j;
  }

  String getSecondLevelKey(int i, int j)
  {
    return "/node_" + i + "/" + getNodeId(i, j);
  }

  String getFirstLevelKey(int i)
  {
    return "/node_" + i;
  }

  //@Test
  public void testZKPropertyStore() throws Exception
  {
  	System.out.println("START " + className + " at " + new Date(System.currentTimeMillis()));
    // LOG.info("number of connections is " + ZkClient.getNumberOfConnections());

    // clean up zk
    final String propertyStoreRoot = "/" + className;
    if (_gZkClient.exists(propertyStoreRoot))
    {
      _gZkClient.deleteRecursive(propertyStoreRoot);
    }

    ZKPropertyStore<ZNRecord> store = new ZKPropertyStore<ZNRecord>(new ZkClient(ZK_ADDR),
        new PropertyJsonSerializer<ZNRecord>(ZNRecord.class), propertyStoreRoot);

    // zookeeper has a default 1M limit on size
    char[] data = new char[bufSize];
    for (int i = 0; i < bufSize; i++)
    {
      data[i] = 'a';
    }

    Map<String, String> map = new TreeMap<String, String>();
    for (int i = 0; i < mapNr; i++)
    {
      map.put("key_" + i, new String(data));
    }
    String node = "node";
    ZNRecord record = new ZNRecord(node);
    record.setSimpleFields(map);

    ZNRecordSerializer serializer = new ZNRecordSerializer();
    int bytesPerNode = serializer.serialize(record).length;
    System.out.println("use znode of size " + bytesPerNode/1024 + "K");
    Assert.assertTrue(bytesPerNode < 1024 * 1024, "zookeeper has a default 1M limit on size");

    // test getPropertyRootNamespace()
    String root = store.getPropertyRootNamespace();
    Assert.assertEquals(root, propertyStoreRoot);

    // set 100 nodes and get 100 nodes, verify get what we set
    long start = System.currentTimeMillis();
    setNodes(store, 'a', false);
    long end = System.currentTimeMillis();
    System.out.println("ZKPropertyStore write throughput is " + bytesPerNode * totalNodes / (end-start) + " kilo-bytes per second");

    start = System.currentTimeMillis();
    for (int i = 0; i < 10; i++)
    {
      for (int j = 0; j < 10; j++)
      {
        String nodeId = getNodeId(i, j);
        String key = getSecondLevelKey(i, j);
        record = store.getProperty(key);
        Assert.assertEquals(record.getId(), nodeId);
      }
    }
    end = System.currentTimeMillis();
    System.out.println("ZKPropertyStore read throughput is " + bytesPerNode * totalNodes / (end-start) + " kilo-bytes per second");


    // test subscribe
    Map<String, String> keyMap = new TreeMap<String, String>();
    // verify initial callbacks invoked for all 100 nodes
    PropertyListener<ZNRecord> listener = new TestListener(keyMap);
    store.subscribeForPropertyChange("", listener);
    System.out.println("keyMap size: " + keyMap.size());
    Assert.assertTrue(keyMap.size() > 100);
    for (int i = 0; i < firstLevelNr; i++)
    {
      for (int j = 0; j < secondLevelNr; j++)
      {
        String key = getSecondLevelKey(i, j);
        Assert.assertTrue(keyMap.containsKey(key));
      }
    }

    // change nodes via property store interface
    // and verify all notifications have been received (TODO: without latency?)
    start = System.currentTimeMillis();
    keyMap.clear();
    setNodes(store, 'b', true);

    // wait for all callbacks completed
    for (int i = 0; i < 10; i++)
    {
      System.out.println("keySet size: " + keyMap.size());
      if (keyMap.size() == totalNodes)
      {
        break;
      }
      Thread.sleep(500);
    }
    Assert.assertEquals(keyMap.size(), totalNodes, "should receive " + totalNodes + " callbacks");
    end = System.currentTimeMillis();
    long waitTime = (end - start) * 2;

    long maxLatency = 0;
    for (int i = 0; i < firstLevelNr; i++)
    {
      for (int j = 0; j < secondLevelNr; j++)
      {
        String key = getSecondLevelKey(i, j);
        Assert.assertTrue(keyMap.containsKey(key));
        record = store.getProperty(key);
        start = Long.parseLong(record.getSimpleField("SetTimestamp"));
        end = Long.parseLong(keyMap.get(key));
        long latency = end - start;
        if (latency > maxLatency)
        {
          maxLatency = latency;
        }
      }
    }
    System.out.println("ZKPropertyStore callback latency is " + maxLatency + " millisecond");

    // change nodes via native zkclient interface
    // and verify all notifications have been received with some latency
    keyMap.clear();
    setNodes(_gZkClient, propertyStoreRoot, 'a', true);

    // wait for all callbacks completed
    Thread.sleep(waitTime);
    Assert.assertEquals(keyMap.size(), totalNodes, "should receive " + totalNodes + " callbacks");

    // remove node via native zkclient interface
    // should receive callbacks on parent key
    keyMap.clear();
    for (int i = 0; i < firstLevelNr; i++)
    {
      int j = 0;
      String key = getSecondLevelKey(i, j);
      _gZkClient.delete(propertyStoreRoot + key);
    }
    Thread.sleep(waitTime);
    for (int i = 0; i < firstLevelNr; i++)
    {
      String key = getFirstLevelKey(i);
      Assert.assertTrue(keyMap.containsKey(key), "should receive callbacks on " + key);
    }

    keyMap.clear();
    for (int j = 1; j < secondLevelNr; j++)
    {
      int i = 0;
      String key = getSecondLevelKey(i, j);
      _gZkClient.delete(propertyStoreRoot + key);
    }
    Thread.sleep(waitTime);
    String node0Key = getFirstLevelKey(0);
    Assert.assertTrue(keyMap.containsKey(node0Key), "should receive callback on " + node0Key);


    // add back removed nodes
    // should receive callbacks on parent key
    keyMap.clear();
    for (int i = 0; i < bufSize; i++)
    {
      data[i] = 'a';
    }

    map = new TreeMap<String, String>();
    for (int i = 0; i < mapNr; i++)
    {
      map.put("key_" + i, new String(data));
    }

    for (int i = 0; i < firstLevelNr; i++)
    {
      int j = 0;
      String nodeId = getNodeId(i, j);
      String key = getSecondLevelKey(i, j);
      record = new ZNRecord(nodeId);
      record.setSimpleFields(map);
      store.setProperty(key, record);
    }
    Thread.sleep(waitTime);
    for (int i = 0; i < firstLevelNr; i++)
    {
      String key = getFirstLevelKey(i);
      Assert.assertTrue(keyMap.containsKey(key), "should receive callbacks on " + key);
    }

    keyMap.clear();
    for (int j = 1; j < secondLevelNr; j++)
    {
      int i = 0;
      String nodeId = getNodeId(i, j);
      String key = getSecondLevelKey(i, j);
      record = new ZNRecord(nodeId);
      record.setSimpleFields(map);
      store.setProperty(key, record);
    }
    Thread.sleep(waitTime);
    node0Key = getFirstLevelKey(0);
    Assert.assertTrue(keyMap.containsKey(node0Key), "should receive callback on " + node0Key);

    // test unsubscribe
    store.unsubscribeForPropertyChange("", listener);
    // change all nodes and verify no notification happens
    keyMap.clear();
    setNodes(store, 'c', false);
    Thread.sleep(waitTime);
    Assert.assertEquals(keyMap.size(), 0);

    // test getPropertyNames
    List<String> names = store.getPropertyNames("");
    int cnt = 0;
    for (String name : names)
    {
      int i = cnt / 10;
      int j = cnt % 10;
      cnt++;
      String key = getSecondLevelKey(i, j);
      Assert.assertEquals(name, key);
    }

    // test compare and set
    char[] updateData = new char[bufSize];
    for (int i = 0; i < bufSize; i++)
    {
      data[i] = 'c';
      updateData[i] = 'd';
    }

    Map<String, String> updateMap = new TreeMap<String, String>();
    for (int i = 0; i < 10; i++)
    {
      map.put("key_" + i, new String(data));
      updateMap.put("key_" + i, new String(updateData));
    }

    PropertyJsonComparator<ZNRecord> comparator
      = new PropertyJsonComparator<ZNRecord>(ZNRecord.class);
    for (int i = 0; i < firstLevelNr; i++)
    {
      for (int j = 0; j < secondLevelNr; j++)
      {
        String nodeId = getNodeId(i, j);
        record = new ZNRecord(nodeId);
        record.setSimpleFields(map);
        String key = getSecondLevelKey(i, j);

        ZNRecord update = new ZNRecord(nodeId);
        update.setSimpleFields(updateMap);
//        boolean succeed = store.compareAndSet(key, record, update, comparator);
//        Assert.assertTrue(succeed);
//        record = store.getProperty(key);
//        Assert.assertEquals(record.getSimpleField("key_0").charAt(0), 'd');
      }
    }

    // test updateUntilSucceed
    TestUpdater updater = new TestUpdater();
    for (int i = 0; i < firstLevelNr; i++)
    {
      for (int j = 0; j < secondLevelNr; j++)
      {
        String key = getSecondLevelKey(i, j);

        store.updatePropertyUntilSucceed(key, updater);
        record = store.getProperty(key);
        Assert.assertEquals(record.getSimpleField("key_0").charAt(0), 'e');
      }
    }

    // test exist
    for (int i = 0; i < firstLevelNr; i++)
    {
      for (int j = 0; j < secondLevelNr; j++)
      {
        String key = getSecondLevelKey(i, j);

        boolean exist = store.exists(key);
        Assert.assertTrue(exist);
      }
    }

    // test removeProperty
    for (int i = 0; i < firstLevelNr; i++)
    {
      int j = 0;

      String key = getSecondLevelKey(i, j);

      store.removeProperty(key);
      record = store.getProperty(key);
      Assert.assertNull(record);
    }

    // test removePropertyNamespace
    for (int i = 0; i < firstLevelNr; i++)
    {
      String key = "/node_" + i;
      store.removeNamespace(key);
    }

    for (int i = 0; i < firstLevelNr; i++)
    {
      for (int j = 0; j < secondLevelNr; j++)
      {
        String key = getSecondLevelKey(i, j);

        store.removeProperty(key);
        record = store.getProperty(key);
        Assert.assertNull(record);
      }
    }

    System.out.println("END " + className + " at " + new Date(System.currentTimeMillis()));
  }

  private void setNodes(ZKPropertyStore<ZNRecord> store, char c, boolean needTimestamp)
      throws PropertyStoreException
  {
    char[] data = new char[bufSize];

    for (int i = 0; i < bufSize; i++)
    {
      data[i] = c;
    }

    Map<String, String> map = new TreeMap<String, String>();
    for (int i = 0; i < mapNr; i++)
    {
      map.put("key_" + i, new String(data));
    }

    for (int i = 0; i < firstLevelNr; i++)
    {
      for (int j = 0; j < secondLevelNr; j++)
      {
        String nodeId = getNodeId(i, j);
        ZNRecord record = new ZNRecord(nodeId);
        record.setSimpleFields(map);
        if (needTimestamp)
        {
          long now = System.currentTimeMillis();
          record.setSimpleField("SetTimestamp", Long.toString(now));
        }
        String key = getSecondLevelKey(i, j);
        store.setProperty(key, record);
      }
    }
  }

  private void setNodes(ZkClient zkClient, String root, char c, boolean needTimestamp)
      throws PropertyStoreException
  {
    char[] data = new char[bufSize];

    for (int i = 0; i < bufSize; i++)
    {
      data[i] = c;
    }

    Map<String, String> map = new TreeMap<String, String>();
    for (int i = 0; i < mapNr; i++)
    {
      map.put("key_" + i, new String(data));
    }

    for (int i = 0; i < firstLevelNr; i++)
    {
      for (int j = 0; j < secondLevelNr; j++)
      {
        String nodeId = getNodeId(i, j);
        ZNRecord record = new ZNRecord(nodeId);
        record.setSimpleFields(map);
        if (needTimestamp)
        {
          long now = System.currentTimeMillis();
          record.setSimpleField("SetTimestamp", Long.toString(now));
        }
        String key = getSecondLevelKey(i, j);
        zkClient.writeData(root + key, record);
      }
    }
  }
  
  @Test
  public void testCallback() throws Exception
  {
	class CountingListener implements PropertyListener<String>
	{
		OperationRecorder opRecorder = new OperationRecorder();

		@Override
		public void onPropertyChange(String key) 
		{
			opRecorder.recordUpdate(key);
		}

		@Override
		public void onPropertyCreate(String key) 
		{
			opRecorder.recordCreate(key);
		}

		@Override
		public void onPropertyDelete(String key) 
		{
			opRecorder.recordDelete(key);
		}
	}
	
    ZkClient zkClient = new ZkClient(ZK_ADDR);
    zkClient.setZkSerializer(new StringZKSerializer());
    
    // clean up zk
    final String propertyStoreRoot = "/" + className;
    if (zkClient.exists(propertyStoreRoot))
    {
    	zkClient.deleteRecursive(propertyStoreRoot);
    }
    
    HashMap<String, String> properties = new HashMap<String, String>();
    properties.put("/key1/key2/key3", "val1"); 
    properties.put("/a/b/c", "val2"); 
    properties.put("/xxx/yyy/zzz", "val3");
    
    ZKPropertyStore<String> store =
            new ZKPropertyStore<String>(new ZkClient(ZK_ADDR), new PropertyStoreStringSerializer(), propertyStoreRoot);
	store.subscribeForPropertyChange("/",  new CountingListener());

    CountingListener countingListener = new CountingListener();

    ZKPropertyStore<String> anotherStore =
            new ZKPropertyStore<String>(new ZkClient(ZK_ADDR), new PropertyStoreStringSerializer(), propertyStoreRoot);
    anotherStore.subscribeForPropertyChange("/", countingListener);

    
    //Test create
    for(String key : properties.keySet())
    {
    	store.setProperty(key, properties.get(key));
    }

    // wait for cache being updated by zk callbacks
    Thread.sleep(100);
    
    for(String key : properties.keySet())
    {
    	byte[] byteArray = (byte[]) anotherStore._cache.get(propertyStoreRoot + key, new Stat());
    	Assert.assertEquals(new String(byteArray), properties.get(key));
    }
    
    Assert.assertTrue(countingListener.opRecorder._createdKeys.size() != 0);
    
    
    //test update
    for(String key : properties.keySet())
    {
    	store.setProperty(key, "updated" + properties.get(key));
    }

    // wait for cache being updated by zk callbacks
    Thread.sleep(100);
    
    for(String key : properties.keySet())
    {
    	byte[] byteArray = (byte[]) anotherStore._cache.get(propertyStoreRoot + key, new Stat());
    	Assert.assertEquals(new String(byteArray), "updated"  + properties.get(key));
    }
    
    Assert.assertTrue(countingListener.opRecorder._updateCounts.size() != 0);
    
    //test delete
    for(String key : properties.keySet())
    {
    	store.removeProperty(key);
    }
    
    // wait for cache being updated by zk callbacks
    Thread.sleep(100);
    
    for(String key : properties.keySet())
    {
    	Assert.assertNull(anotherStore._cache.get(propertyStoreRoot + key, new Stat()));
    }
    
    Assert.assertTrue(countingListener.opRecorder._deleteCounts.size() != 0);

    System.out.println(countingListener.opRecorder);
    
    zkClient.close();
  }
}

class OperationRecorder {
	Map<String, Integer> _deleteCounts = new HashMap<String, Integer>();
	Map<String, Integer> _updateCounts = new HashMap<String, Integer>();
	Set<String> _createdKeys = new HashSet<String>();

	public synchronized void recordDelete(String key) {
		int prevCount = (_deleteCounts.containsKey(key) ? _deleteCounts
				.get(key) : 0);
		_deleteCounts.put(key, prevCount + 1);
	}

	public synchronized void recordUpdate(String key) {
		int prevCount = (_updateCounts.containsKey(key) ? _updateCounts
				.get(key) : 0);
		_updateCounts.put(key, prevCount + 1);
	}

	public synchronized void recordCreate(String key) {
				_createdKeys.add(key);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof OperationRecorder) {
			OperationRecorder rhs = (OperationRecorder) obj;
			return (_deleteCounts.keySet().equals(rhs._deleteCounts.keySet())
					&& _updateCounts.keySet().equals(rhs._updateCounts.keySet()) && _createdKeys.equals(rhs._createdKeys));
		}

		return false;
	}

	public String toString() {
		return _createdKeys.toString() + ":" + _updateCounts.toString() + ":"
				+ _deleteCounts.toString();
	}
	
	public void clear()
	{
		_createdKeys.clear();
		_updateCounts.clear();
		_deleteCounts.clear();
	}
}

class PropertyStoreStringSerializer implements PropertySerializer<String> 
{

    @Override
    public byte[] serialize(String data) throws PropertyStoreException {
            // TODO Auto-generated method stub
            String str = data;
            if (str == null) {
                    return null;
            }

            return str.getBytes();
    }

    @Override
    public String deserialize(byte[] bytes) throws PropertyStoreException {
            // TODO Auto-generated method stub
            if (bytes == null) {
                    return null;
            }
            return new String(bytes);
    }
}

class StringZKSerializer implements ZkSerializer {

    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError {
            // TODO Auto-generated method stub
            String str = (String) data;
            if (str == null) {
                    return null;
            }

            return str.getBytes();
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
            // TODO Auto-generated method stub
            if (bytes == null) {
                    return null;
            }
            return new String(bytes);
    }
};
