package com.linkedin.helix.tools;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.MessageState;

public class TransitionLatencyAnalyzor
{
  private static Logger LOG = Logger.getLogger(TransitionLatencyAnalyzor.class);

  static String getAttributeValue(String line, String attribute)
  {
    String[] parts = line.split("\\s");
    if (parts != null && parts.length > 0)
    {
      for (int i = 0; i < parts.length; i++)
      {
        if (parts[i].startsWith(attribute))
        {
          String val = parts[i].substring(attribute.length());
          return val;
        }
      }
    }
    return null;
  }

  static class MsgItem
  {
    long    createTime;
    long    sendTime;
    long    readTime;
    long    deleteTime;
    long    latency;
    Message msg;

    public MsgItem(long sendTime, Message msg)
    {
      this.createTime = msg.getCreateTimeStamp();
      this.sendTime = sendTime;
      this.msg = msg;
    }

  }

  static class SessionItem
  {
    long   startTime;
    long   endTime;
    String instanceName;

    public SessionItem(String instanceName, long startTime)
    {
      this.instanceName = instanceName;
      this.startTime = startTime;
    }
  }

  static String findFirstStartSession(Map<String, SessionItem> sessions)
  {
    long firstStartTime = Long.MAX_VALUE;
    String firstStartSession = null;
    for (String sessionId : sessions.keySet())
    {
      SessionItem item = sessions.get(sessionId);
      if (item.startTime < firstStartTime)
      {
        firstStartTime = item.startTime;
        firstStartSession = sessionId;
      }
    }
    return firstStartSession;
  }

  static String findFirstEndSession(Map<String, SessionItem> sessions)
  {
    long firstEndTime = Long.MAX_VALUE;
    String firstEndSession = null;
    for (String sessionId : sessions.keySet())
    {
      SessionItem item = sessions.get(sessionId);
      if (item.endTime < firstEndTime)
      {
        firstEndTime = item.endTime;
        firstEndSession = sessionId;
      }
    }
    return firstEndSession;
  }

  static String findLastMessageIn(Map<String, MsgItem> msgs, long begin, long end)
  {
    long lastMsgSendTime = Long.MIN_VALUE;
    String lastMsg = null;
    for (String msgId : msgs.keySet())
    {
      MsgItem item = msgs.get(msgId);
      if (item.sendTime > begin && item.sendTime < end && item.sendTime > lastMsgSendTime)
      {
        lastMsgSendTime = item.sendTime;
        lastMsg = msgId;
      }
    }
    return lastMsg;
  }

  public static void main(String[] args) throws Exception
  {
    if (args.length != 2)
    {
      System.err.println("USAGE: TransitionLatencyAnalyzor logFile clusterName");
      System.exit(1);
    }

    final ZNRecordSerializer deserializer = new ZNRecordSerializer();

    Map<String, SessionItem> sessions = new HashMap<String, SessionItem>(); // zkSession->sessionItem
    Map<String, MsgItem> msgs = new HashMap<String, MsgItem>(); // msgId->msgItem

    // input file
    String logfilepath = args[0];
    String clusterName = args[1];
    FileInputStream fis = new FileInputStream(logfilepath);
    BufferedReader br = new BufferedReader(new InputStreamReader(fis));

    int pos;
    String inputLine;
    while ((inputLine = br.readLine()) != null)
    {
      if (inputLine.indexOf(clusterName + "/LIVEINSTANCES") != -1)
      {
        pos = inputLine.indexOf("LIVEINSTANCES");
        pos = inputLine.indexOf("data:{", pos);
        if (pos != -1)
        {
          String timestamp = getAttributeValue(inputLine, "time:");
          String session = getAttributeValue(inputLine, "session:");
          ZNRecord record =
              (ZNRecord) deserializer.deserialize(inputLine.substring(pos + 5).getBytes());

          LiveInstance liveInstance = new LiveInstance(record);
          sessions.put(session,
                       new SessionItem(liveInstance.getInstanceName(),
                                       Long.parseLong(timestamp)));
          System.out.println(timestamp + ": start instance "
              + liveInstance.getInstanceName());
        }
      }
      else if (inputLine.indexOf("closeSession") != -1)
      {
        String timestamp = getAttributeValue(inputLine, "time:");
        String session = getAttributeValue(inputLine, "session:");
        if (sessions.containsKey(session))
        {
          sessions.get(session).endTime = Long.parseLong(timestamp);
          System.out.println(timestamp + ": kill instance " + sessions.get(session).instanceName);
        }
      }
      else if (inputLine.indexOf(clusterName + "/EXTERNALVIEW") != -1)
      {
        pos = inputLine.indexOf("EXTERNALVIEW");
        pos = inputLine.indexOf("data:{", pos);
        if (pos != -1)
        {
          String timestamp = getAttributeValue(inputLine, "time:");
          ZNRecord record =
              (ZNRecord) deserializer.deserialize(inputLine.substring(pos + 5).getBytes());
          ExternalView extView = new ExternalView(record);
          int masterCnt = ClusterStateVerifier.countStateNbInExtView(extView, "MASTER");
          int slaveCnt = ClusterStateVerifier.countStateNbInExtView(extView, "SLAVE");
          if (masterCnt == 1200)
          {
            System.out.println(timestamp + ": externalView " + extView.getResourceName()
                + " has " + masterCnt + " MASTER, " + slaveCnt + " SLAVE");
          }
        }
      }
      else if (inputLine.indexOf(clusterName) != -1 && inputLine.indexOf("MESSAGES") != -1)
      {
        String timestamp = getAttributeValue(inputLine, "time:");
        String type = getAttributeValue(inputLine, "type:");
        String path = getAttributeValue(inputLine, "path:");

        if (type.equals("create"))
        {
          pos = inputLine.indexOf("MESSAGES");
          pos = inputLine.indexOf("data:{", pos);
          if (pos != -1)
          {

            byte[] msgBytes = inputLine.substring(pos + 5).getBytes();
            ZNRecord record = (ZNRecord) deserializer.deserialize(msgBytes);
            Message msg = new Message(record);
            MessageState msgState = msg.getMsgState();
            String msgType = msg.getMsgType();
            if (msgType.equals("STATE_TRANSITION") && msgState == MessageState.NEW)
            {
              if (!msgs.containsKey(msg.getMsgId()))
              {
                msgs.put(msg.getMsgId(), new MsgItem(Long.parseLong(timestamp), msg));
              }
              else
              {
                LOG.error("msg: " + msg.getMsgId() + " already sent");
              }

              System.out.println(timestamp + ": sendMsg " + msg.getPartitionName() + "("
                  + msg.getFromState() + "->" + msg.getToState() + ") to "
                  + msg.getTgtName() + ", size: " + msgBytes.length);
            }
          }
        }
        else if (type.equals("setData"))
        {
          pos = inputLine.indexOf("MESSAGES");
          pos = inputLine.indexOf("data:{", pos);
          if (pos != -1)
          {

            byte[] msgBytes = inputLine.substring(pos + 5).getBytes();
            ZNRecord record = (ZNRecord) deserializer.deserialize(msgBytes);
            Message msg = new Message(record);
            MessageState msgState = msg.getMsgState();
            String msgType = msg.getMsgType();
            if (msgType.equals("STATE_TRANSITION") && msgState == MessageState.READ)
            {
              if (!msgs.containsKey(msg.getMsgId()))
              {
                LOG.error("msg: " + msg.getMsgId() + " never sent");
              }
              else
              {
                MsgItem msgItem = msgs.get(msg.getMsgId());
                if (msgItem.readTime == 0)
                {
                  msgItem.readTime = Long.parseLong(timestamp);
                  msgs.put(msg.getMsgId(), msgItem);
                  // System.out.println(timestamp + ": readMsg " + msg.getPartitionName()
                  // + "("
                  // + msg.getFromState() + "->" + msg.getToState() + ") to "
                  // + msg.getTgtName() + ", latency: " + (msgItem.readTime -
                  // msgItem.sendTime));
                }
              }

            }
          }
        }
        else if (type.equals("delete"))
        {
          String msgId = path.substring(path.lastIndexOf('/') + 1);
          if (msgs.containsKey(msgId))
          {
            MsgItem msgItem = msgs.get(msgId);
            Message msg = msgItem.msg;
            msgItem.deleteTime = Long.parseLong(timestamp);
            msgs.put(msgId, msgItem);
            msgItem.latency = msgItem.deleteTime - msgItem.sendTime;
             System.out.println(timestamp + ": delMsg " + msg.getPartitionName() + "("
             + msg.getFromState() + "->" + msg.getToState() + ") to "
             + msg.getTgtName() + ", latency: " + msgItem.latency);
          }
          else
          {
            // messages other than STATE_TRANSITION message
            // LOG.error("msg: " + msgId + " never sent");
          }
        }
      }
    } // end of [br.readLine()) != null]

    // statistics
    String firstStartSession = findFirstStartSession(sessions);
    String firstEndSession = findFirstEndSession(sessions);
    
    String lastMsgSent = findLastMessageIn(msgs, sessions.get(firstStartSession).startTime,
                                           sessions.get(firstEndSession).endTime);
    System.out.println("initial setup time: " + (msgs.get(lastMsgSent).sendTime - sessions.get(firstStartSession).startTime ));
    
    lastMsgSent = findLastMessageIn(msgs, sessions.get(firstEndSession).endTime, Long.MAX_VALUE);
    System.out.println("kill recover time: " + (msgs.get(lastMsgSent).sendTime - sessions.get(firstEndSession).endTime));
    
    
    long maxLatency = Long.MIN_VALUE;
    long minLatency = Long.MAX_VALUE;
    int offlineToSlaveCnt = 0;
    int slaveToMasterCnt = 0;
    for (MsgItem msgItem : msgs.values())
    {
      Message msg = msgItem.msg;
      String fromState = msg.getFromState();
      String toState = msg.getToState();
      if (fromState.equals("OFFLINE") && toState.equals("SLAVE"))
      {
        offlineToSlaveCnt++;
      }
      else if (fromState.equals("SLAVE") && toState.equals("MASTER"))
      {
        slaveToMasterCnt++;
      }
      else
      {
        // LOG.error(msg);
      }

      if (msgItem.latency > maxLatency)
      {
        maxLatency = msgItem.latency;
      }

      if (msgItem.latency < minLatency)
      {
        minLatency = msgItem.latency;
      }
    }

    System.out.println("Total messages: " + msgs.size() + ", offline2Slave: "
        + offlineToSlaveCnt + ", slaveToMaster: " + slaveToMasterCnt
        + ", process latency: " + minLatency + " - " + maxLatency);
  }
}
