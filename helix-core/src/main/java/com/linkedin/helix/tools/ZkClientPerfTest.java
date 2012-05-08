package com.linkedin.helix.tools;

import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.MessageState;
import com.linkedin.helix.model.Message.MessageType;

public class ZkClientPerfTest
{
//  final static String zkAddr     =
//                              "eat1-app20.corp:2181,eat1-app21.corp:2181,eat1-app22.corp:2181";
  final static String testName   = "ZkClientPerfTest";
  final static String parentPath = "/" + testName;

  static class Producer implements Runnable
  {
    final AtomicBoolean stopped = new AtomicBoolean(false);
    final ZkClient      zkClient;

    public Producer(ZkClient zkClient)
    {
      this.zkClient = zkClient;
    }

    @Override
    public void run()
    {
      while (!stopped.get())
      {
        try
        {
          List<String> consumers = zkClient.getChildren(parentPath);
          for (String consumer : consumers)
          {
            Message msg = createMessage();
            // System.out.println("Send msg: " + msg);
            zkClient.asyncCreate(parentPath + "/" + consumer + "/msg/" + msg.getId(),
                                 msg.getRecord(),
                                 CreateMode.PERSISTENT);
          }
          Thread.sleep(300);
        }
        catch (Exception e)
        {
          // TODO: handle exception
          e.printStackTrace();
        }
      }
    }

    public void stop()
    {
      stopped.set(true);
    }
  }

  static class Consumer implements Runnable
  {
    final AtomicBoolean stopped = new AtomicBoolean(false);
    final ZkClient      zkClient;
    final String        consumerName;
    
    public Consumer(ZkClient zkClient, String consumerName)
    {
      this.zkClient = zkClient;
      this.consumerName = consumerName;
    }

    @Override
    public void run()
    {
      final String msgParentPath = parentPath + "/" + consumerName + "/msg";
      zkClient.createPersistent(msgParentPath, true);

      // TODO Auto-generated method stub
      while (!stopped.get())
      {
        try
        {
          List<String> msgIds = zkClient.getChildren(msgParentPath);
          for (String msgId : msgIds)
          {
            String msgPath = msgParentPath + "/" + msgId;
            final ZNRecord msg = zkClient.readData(msgPath, true);
//            zkClient.asyncWriteData(msgPath, msg);
            zkClient.updateDataSerialized(msgPath, new DataUpdater<ZNRecord>()
            {

              @Override
              public ZNRecord update(ZNRecord currentData)
              {
                // TODO Auto-generated method stub
                return msg;
              }
            }); 
//            Thread.sleep(10);
            // System.out.println("Delete msg: " + msg);
            zkClient.delete(msgPath);
          }
          Thread.sleep(200);
        }
        catch (Exception e)
        {
          // TODO: handle exception
          e.printStackTrace();
        }
      }

    }

    public void stop()
    {
      stopped.set(true);
    }

  }

  public static void main(String[] args) throws Exception
  {
    Logger.getRootLogger().setLevel(Level.INFO);
    System.out.println("START " + testName + " at "
        + new Date(System.currentTimeMillis()));
    if (args.length != 1)
    {
      System.err.println("USAGE: ./ZkClientPerfTest zkAddr(,zkAddr)");
      System.exit(1);
    }
    String zkAddr = args[0];
    ZkClient zkClient = new ZkClient(zkAddr, 30000, 10000, new ZNRecordSerializer());
    zkClient.deleteRecursive(parentPath);
    zkClient.createPersistent(parentPath);
    zkClient.close();

    for (int i = 0; i < 20; i++)
    {
      zkClient = new ZkClient(zkAddr, 30000, 10000, new ZNRecordSerializer());
      new Thread(new Consumer(zkClient, "consumer" + i)).start();
    }

    Thread.sleep(100);

    zkClient = new ZkClient(zkAddr, 30000, 10000, new ZNRecordSerializer());
    new Thread(new Producer(zkClient)).start();

    Thread.sleep(Long.MAX_VALUE);
    System.out.println("START " + testName + " at "
        + new Date(System.currentTimeMillis()));
  }

  // TODO move to TestHelper
  private static Message createMessage()
  {
    String uuid = UUID.randomUUID().toString();
    Message message = new Message(MessageType.STATE_TRANSITION, uuid);
    message.setSrcName("srcName");
    message.setTgtName("instanceName");
    message.setMsgState(MessageState.NEW);
    message.setPartitionName("partitionName");
    message.setResourceName("resourceName");
    message.setFromState("currentState");
    message.setToState("nextState");
    message.setTgtSessionId("sessionId0");
    message.setSrcSessionId("manager.getSessionId()");
    message.setStateModelDef("stateModelDefName");
    message.setStateModelFactoryName("stateModelFactoryName");
    return message;
  }
}
