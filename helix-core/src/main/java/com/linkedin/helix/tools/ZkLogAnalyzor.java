package com.linkedin.helix.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.MessageState;

public class ZkLogAnalyzor
{
  private static Logger           LOG           =
                                                    Logger.getLogger(ZkLogAnalyzor.class);
  final static ZNRecordSerializer _deserializer = new ZNRecordSerializer();

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

  static String findLastMessageSentBetween(List<String> messageSendLines, long start, long end)
  {
    long lastSendMsgTimestamp = Long.MIN_VALUE;
    String lastSendMsgLine = null;
    for (String line : messageSendLines)
    {
      ZNRecord record = getZNRecord(line);
//      Message msg = new Message(record);
      long timestamp = Long.parseLong(getAttributeValue(line, "time:"));
      if (timestamp >= start && timestamp <= end && timestamp > lastSendMsgTimestamp)
      {
        lastSendMsgTimestamp = timestamp;
        lastSendMsgLine = line;
      }
    }
    assert (lastSendMsgLine != null) : "No message sent between " + start + " - " + end;
    return lastSendMsgLine;
  }

  static ZNRecord getZNRecord(String line)
  {
    ZNRecord record = null;
    String value = getAttributeValue(line, "data:");
    if (value != null)
    {
      record = (ZNRecord) _deserializer.deserialize(value.getBytes());
//      if (record == null)
//      {
//        System.out.println(line);
//      }
    }
    return record;
  }

  /**
   * guess the start time of last run test
   * return list of liveInstance create/close in time order
   */
  static List<String> findStartTimeOfLastTestRun(String zkLog, String clusterName) throws Exception
  {
    Set<String> sessions = new HashSet<String>();
    
    // create/close of liveInstance lines in time order
    List<String> liveInstanceLines = new ArrayList<String>(); 
    
    FileInputStream fis = new FileInputStream(zkLog);
    BufferedReader br = new BufferedReader(new InputStreamReader(fis));

    String inputLine;
    while ((inputLine = br.readLine()) != null)
    {
      if (inputLine.indexOf("/" + clusterName + "/LIVEINSTANCES") != -1 
          || inputLine.indexOf("/" + clusterName + "/CONTROLLER/LEADER") != -1)
      {
        String path = getAttributeValue(inputLine, "path:");
        String type = getAttributeValue(inputLine, "type:");
        if (type.equals("create") 
            && path != null 
            && path.equals("/" + clusterName + "/LIVEINSTANCES"))
        {
          // create of /{clusterName}/LIVEINSTANCES itself
//          System.out.println(inputLine);
          liveInstanceLines.clear();
        }
        else if (type.equals("create"))
        {
          String session = getAttributeValue(inputLine, "session:");
          sessions.add(session);
          liveInstanceLines.add(inputLine);
        }
      }
      else if (inputLine.indexOf("closeSession") != -1)
      {
        String session = getAttributeValue(inputLine, "session:");
        if (sessions.contains(session))
        {
          liveInstanceLines.add(inputLine);
        }
      }
    }
    br.close();
    fis.close();

    return liveInstanceLines;
  }

  public static void main(String[] args) throws Exception
  {
    if (args.length != 2)
    {
      System.err.println("USAGE: ZkLogAnalyzor zkLogDir clusterName");
      System.exit(1);
    }

    // find the latest zk log and parse it
    // save parsed log in /tmp/zkLogAnalyzor_zklog.parsed
    String zkLogDir = args[0];
    while (zkLogDir.endsWith("/"))
    {
      zkLogDir = zkLogDir.substring(0, zkLogDir.length() - 1);
    }
    if (!zkLogDir.endsWith("/version-2"))
    {
      zkLogDir = zkLogDir + "/version-2";
    }
    File dir = new File(zkLogDir);
    File[] zkLogs = dir.listFiles(new FileFilter() {

      @Override
      public boolean accept(File file)
      {
        return file.isFile() && (file.getName().indexOf("log") != -1);
      }
    });
    long lastModTimestamp = Long.MIN_VALUE;
    String lastZkLog = null;
    for (File file : zkLogs)
    {
      if (file.lastModified() > lastModTimestamp)
      {
        lastZkLog = file.getAbsolutePath();
        lastModTimestamp = file.lastModified();
      }
    }
    ZKLogFormatter.main(new String[]{"log", lastZkLog, "/tmp/zkLogAnalyzor_zklog.parsed"});
    System.out.println("Use latest zkLog: " + lastZkLog);
    
    // sessionId -> line
    Map<String, String> sessionMap = new HashMap<String, String>();
    
    // message send lines in time order
    List<String> sendMessageLines = new ArrayList<String>();

    String zkLog = "/tmp/zkLogAnalyzor_zklog.parsed";   // args[0];
    String clusterName = args[1];

    List<String> liveInstanceLines = findStartTimeOfLastTestRun(zkLog, clusterName);
//    System.out.println(liveInstanceLines);

    // find the leader
    String leaderLine = null;
    String leaderCloseLine = null;
    String leaderSession = null;
    for (String line : liveInstanceLines)
    {
      if (line.indexOf("/" + clusterName + "/CONTROLLER/LEADER") != -1)
      {
        leaderLine = line;
        leaderSession = getAttributeValue(line, "session:");
      } else if (line.indexOf("closeSession") != -1 && getAttributeValue(line, "session:").equals(leaderSession))
      {
        leaderCloseLine = line;
      }
    }
    assert(leaderLine != null) : "No leader found";
    liveInstanceLines.remove(leaderLine);

    
    FileInputStream fis = new FileInputStream(zkLog);
    BufferedReader br = new BufferedReader(new InputStreamReader(fis));

//    int pos;
    int msgSentCount = 0;
    int msgSentCount_O2S = 0;   // Offlien to Slave
    int msgSentCount_S2M = 0;   // Slave to Master
    int msgSentCount_M2S = 0;   // Master to Slave
    int msgDeleteCount = 0;
    int msgModifyCount = 0;
    int curStateCreateCount = 0;
    int curStateUpdateCount = 0;
    int extViewCreateCount = 0;
    int extViewUpdateCount = 0;

    
    String inputLine;
    boolean isStarted = false;
    while ((inputLine = br.readLine()) != null)
    {
      if (!isStarted)
      {
        if (!inputLine.equals(liveInstanceLines.get(0)))
        {
          continue;
        } else
        {
          isStarted = true;
        }
      }
      
      if (inputLine.indexOf("/" + clusterName + "/LIVEINSTANCES/") != -1)
      {
        ZNRecord record = getZNRecord(inputLine);
        LiveInstance liveInstance = new LiveInstance(record);
        String session = getAttributeValue(inputLine, "session:");
        String timestamp = getAttributeValue(inputLine, "time:");
        sessionMap.put(session, inputLine);
//        System.out.println(timestamp + ", create LIVEINSTANCE " + liveInstance.getInstanceName());
      }
      else if (inputLine.indexOf("closeSession") != -1)
      {
        String timestamp = getAttributeValue(inputLine, "time:");
        String session = getAttributeValue(inputLine, "session:");
        if (sessionMap.containsKey(session))
        {
          String line = sessionMap.get(session);
          ZNRecord record = getZNRecord(line);
          LiveInstance liveInstance = new LiveInstance(record);

//          System.out.println(timestamp + ", close LIVEINSTANCE "
//              + liveInstance.getInstanceName());
        }
      } 
      else if (inputLine.indexOf("/" + clusterName + "/CONTROLLER/LEADER") != -1)
      {
        ZNRecord record = getZNRecord(inputLine);
        LiveInstance liveInstance = new LiveInstance(record);
        String session = getAttributeValue(inputLine, "session:");
        String timestamp = getAttributeValue(inputLine, "time:");
        sessionMap.put(session, inputLine);
//        System.out.println(timestamp + ", create LEADER " + liveInstance.getInstanceName());
      }
      else if (inputLine.indexOf("/" + clusterName + "/") != -1 
          && inputLine.indexOf("/CURRENTSTATES/") != -1)
      {
        String type = getAttributeValue(inputLine, "type:");
        if (type.equals("create"))
        {
          curStateCreateCount++;
        } else if (type.equals("setData"))
        {
          curStateUpdateCount++;
        }
      }
      else if (inputLine.indexOf("/" + clusterName + "/EXTERNALVIEW/") != -1)
      {
        String session = getAttributeValue(inputLine, "session:");
        if (session.equals(leaderSession))
        {
          String type = getAttributeValue(inputLine, "type:");
          if (type.equals("create"))
          {
            extViewCreateCount++;
          } else if (type.equals("setData"))
          {
            extViewUpdateCount++;
          }
        }        
        
//        pos = inputLine.indexOf("EXTERNALVIEW");
//        pos = inputLine.indexOf("data:{", pos);
//        if (pos != -1)
//        {
//          String timestamp = getAttributeValue(inputLine, "time:");
//          ZNRecord record =
//              (ZNRecord) _deserializer.deserialize(inputLine.substring(pos + 5)
//                                                            .getBytes());
//          ExternalView extView = new ExternalView(record);
//          int masterCnt = ClusterStateVerifier.countStateNbInExtView(extView, "MASTER");
//          int slaveCnt = ClusterStateVerifier.countStateNbInExtView(extView, "SLAVE");
//          if (masterCnt == 1200)
//          {
//            System.out.println(timestamp + ": externalView " + extView.getResourceName()
//                + " has " + masterCnt + " MASTER, " + slaveCnt + " SLAVE");
//          }
//        }
      }
        else if (inputLine.indexOf("/" + clusterName + "/") != -1
            && inputLine.indexOf("/MESSAGES/") != -1)
        {
          String timestamp = getAttributeValue(inputLine, "time:");
          String type = getAttributeValue(inputLine, "type:");
//          String leaderSession = getAttributeValue(leaderLine, "session:");

        if (type.equals("create"))
        {
          ZNRecord record = getZNRecord(inputLine);
          Message msg = new Message(record);
          String sendSession = getAttributeValue(inputLine, "session:");
          if (sendSession.equals(leaderSession) 
              && msg.getMsgType().equals("STATE_TRANSITION") 
              && msg.getMsgState() == MessageState.NEW)
          {
            sendMessageLines.add(inputLine);
            msgSentCount++;
            
            if (msg.getFromState().equals("OFFLINE") && msg.getToState().equals("SLAVE"))
            {
              msgSentCount_O2S++;
            } else if (msg.getFromState().equals("SLAVE") && msg.getToState().equals("MASTER")) 
            {
              msgSentCount_S2M++;
            } else if (msg.getFromState().equals("MASTER") && msg.getToState().equals("SLAVE"))
            {
              msgSentCount_M2S++;
            }
          }
          
//          pos = inputLine.indexOf("MESSAGES");
//          pos = inputLine.indexOf("data:{", pos);
//          if (pos != -1)
//          {
//
//            byte[] msgBytes = inputLine.substring(pos + 5).getBytes();
//            ZNRecord record = (ZNRecord) _deserializer.deserialize(msgBytes);
//            Message msg = new Message(record);
//            MessageState msgState = msg.getMsgState();
//            String msgType = msg.getMsgType();
//            if (msgType.equals("STATE_TRANSITION") && msgState == MessageState.NEW)
//            {
//              if (!msgs.containsKey(msg.getMsgId()))
//              {
//                msgs.put(msg.getMsgId(), new MsgItem(Long.parseLong(timestamp), msg));
//              }
//              else
//              {
//                LOG.error("msg: " + msg.getMsgId() + " already sent");
//              }
//
//              System.out.println(timestamp + ": sendMsg " + msg.getPartitionName() + "("
//                  + msg.getFromState() + "->" + msg.getToState() + ") to "
//                  + msg.getTgtName() + ", size: " + msgBytes.length);
//            }
//          }
        }
        else if (type.equals("setData"))
        {
          msgModifyCount++;
//          pos = inputLine.indexOf("MESSAGES");
//          pos = inputLine.indexOf("data:{", pos);
//          if (pos != -1)
//          {
//
//            byte[] msgBytes = inputLine.substring(pos + 5).getBytes();
//            ZNRecord record = (ZNRecord) _deserializer.deserialize(msgBytes);
//            Message msg = new Message(record);
//            MessageState msgState = msg.getMsgState();
//            String msgType = msg.getMsgType();
//            if (msgType.equals("STATE_TRANSITION") && msgState == MessageState.READ)
//            {
//              if (!msgs.containsKey(msg.getMsgId()))
//              {
//                LOG.error("msg: " + msg.getMsgId() + " never sent");
//              }
//              else
//              {
//                MsgItem msgItem = msgs.get(msg.getMsgId());
//                if (msgItem.readTime == 0)
//                {
//                  msgItem.readTime = Long.parseLong(timestamp);
//                  msgs.put(msg.getMsgId(), msgItem);
//                  // System.out.println(timestamp + ": readMsg " + msg.getPartitionName()
//                  // + "("
//                  // + msg.getFromState() + "->" + msg.getToState() + ") to "
//                  // + msg.getTgtName() + ", latency: " + (msgItem.readTime -
//                  // msgItem.sendTime));
//                }
//              }
//
//            }
//          }
        }
        else if (type.equals("delete"))
        {
          msgDeleteCount++;
//          String msgId = path.substring(path.lastIndexOf('/') + 1);
//          if (msgs.containsKey(msgId))
//          {
//            MsgItem msgItem = msgs.get(msgId);
//            Message msg = msgItem.msg;
//            msgItem.deleteTime = Long.parseLong(timestamp);
//            msgs.put(msgId, msgItem);
//            msgItem.latency = msgItem.deleteTime - msgItem.sendTime;
//            System.out.println(timestamp + ": delMsg " + msg.getPartitionName() + "("
//                + msg.getFromState() + "->" + msg.getToState() + ") to "
//                + msg.getTgtName() + ", latency: " + msgItem.latency);
//          }
//          else
//          {
//            // messages other than STATE_TRANSITION message
//            // LOG.error("msg: " + msgId + " never sent");
//          }
        }
      }
    } // end of [br.readLine()) != null]

    
    // statistics
    // print session create/close duration
    System.out.println("Instance\t\t\t\t duration");
    System.out.println("---------------------------------------------------------------------------------------------");
    long start = Long.parseLong(getAttributeValue(leaderLine, "time:"));
    long end = 0;
    LiveInstance liveInstance = new LiveInstance(getZNRecord(leaderLine));
    if (leaderCloseLine != null)
    {
      end = Long.parseLong(getAttributeValue(leaderCloseLine, "time:"));
      System.out.println(liveInstance.getInstanceName() + "("+ leaderSession +")\t " + start + "-" + end + " ("+(end-start)+"ms)");
    } else
    {
      // the controller waits for 30s to disconnect, so we might not have it in transaction log yet
      System.out.println(liveInstance.getInstanceName() + "("+ leaderSession +")\t " + start + "-?");
    }
    
    
    for (String line : liveInstanceLines)
    {
      if (line.indexOf("/" + clusterName + "/LIVEINSTANCES/") != -1)
      {
        liveInstance = new LiveInstance(getZNRecord(line));
        String session = getAttributeValue(line, "session:");
        start = Long.parseLong(getAttributeValue(line, "time:"));
        Iterator<String> iter = liveInstanceLines.iterator();
        while (iter.hasNext())
        {
          String line2 = iter.next();
          if (line2.indexOf("closeSession") != -1 && getAttributeValue(line2, "session:").equals(session))
          {
            end = Long.parseLong(getAttributeValue(iter.next(), "time:"));
            System.out.println(liveInstance.getInstanceName() + "("+ session +")\t " + start + "-" + end + " ("+(end-start)+"ms)");
          }
        }
      }
    }
    
    // print message related stats
    System.out.println();
    System.out.println("Operation\t\t Total\t O->S\t S->M\t M->S");
    System.out.println("------------------------------------------------------");
    System.out.println("Create message\t\t " + msgSentCount + "\t " + msgSentCount_O2S + "\t " + msgSentCount_S2M + "\t " + msgSentCount_M2S);
    System.out.println("Modify message\t\t " + msgModifyCount);
    System.out.println("Delete message\t\t " + msgDeleteCount);
    System.out.println("Create currentState\t " + curStateCreateCount);
    System.out.println("Update currentState\t " + curStateUpdateCount);
    System.out.println("Create extView\t\t " + extViewCreateCount);
    System.out.println("Update extView\t\t " + extViewUpdateCount);

    
    // print state transition latency related stats
    System.out.println();
    System.out.println("Test duration\t\t\t\t\t\t\t State transition latency");
    System.out.println("------------------------------------------------------------------------------------------");

    String startLine = liveInstanceLines.get(0);
    String endLine = null;
    boolean nextStartFound = true;
    boolean nextEndFound = false;
    for (String line : liveInstanceLines)
    {
      if (!nextEndFound && line.indexOf("closeSession") != -1)
      {
        // find next end session
        endLine = line;
        nextStartFound = false;
        nextEndFound = true;
        long startTimestamp = Long.parseLong(getAttributeValue(startLine, "time:"));
        long endTimestamp = Long.parseLong(getAttributeValue(endLine, "time:"));
        System.out.print("Instances start-end: " + startTimestamp + "-" + endTimestamp 
                           + " (" + (endTimestamp - startTimestamp) + "ms)\t ");
        String lastSendMsgLine = findLastMessageSentBetween(sendMessageLines, startTimestamp, endTimestamp);
        long timestamp = Long.parseLong(getAttributeValue(lastSendMsgLine, "time:"));
        System.out.println("" + (timestamp - startTimestamp) + "ms");
      } else if (!nextStartFound && line.indexOf("/" + clusterName + "/LIVEINSTANCE/") != -1)
      {
        // find next start session
        startLine = line;
        nextStartFound = true;
        nextEndFound = false;
        long startTimestamp = Long.parseLong(getAttributeValue(startLine, "time:"));
        long endTimestamp = Long.parseLong(getAttributeValue(endLine, "time:"));
        System.out.print("Instances end-restart: " + endTimestamp + "-" + startTimestamp 
                         + " (" + (startTimestamp - endTimestamp) + "ms)\t\t ");
        String lastSendMsgLine = findLastMessageSentBetween(sendMessageLines, endTimestamp, startTimestamp);
        long timestamp = Long.parseLong(getAttributeValue(lastSendMsgLine, "time:"));
        System.out.println("" + (timestamp - endTimestamp) + "ms");
      }
    }

//    String firstStartSession = findFirstStartSession(sessions);
//    String firstEndSession = findFirstEndSession(sessions);
//
//    String lastMsgSent =
//        findLastMessageIn(msgs,
//                          sessions.get(firstStartSession).startTime,
//                          sessions.get(firstEndSession).endTime);
//    System.out.println("initial setup time: "
//        + (msgs.get(lastMsgSent).sendTime - sessions.get(firstStartSession).startTime));
//
//    lastMsgSent =
//        findLastMessageIn(msgs, sessions.get(firstEndSession).endTime, Long.MAX_VALUE);
//    System.out.println("kill recover time: "
//        + (msgs.get(lastMsgSent).sendTime - sessions.get(firstEndSession).endTime));
//
//    long maxLatency = Long.MIN_VALUE;
//    long minLatency = Long.MAX_VALUE;
//    int offlineToSlaveCnt = 0;
//    int slaveToMasterCnt = 0;
//    for (MsgItem msgItem : msgs.values())
//    {
//      Message msg = msgItem.msg;
//      String fromState = msg.getFromState();
//      String toState = msg.getToState();
//      if (fromState.equals("OFFLINE") && toState.equals("SLAVE"))
//      {
//        offlineToSlaveCnt++;
//      }
//      else if (fromState.equals("SLAVE") && toState.equals("MASTER"))
//      {
//        slaveToMasterCnt++;
//      }
//      else
//      {
//        // LOG.error(msg);
//      }
//
//      if (msgItem.latency > maxLatency)
//      {
//        maxLatency = msgItem.latency;
//      }
//
//      if (msgItem.latency < minLatency)
//      {
//        minLatency = msgItem.latency;
//      }
//    }

//    System.out.println("Total messages: " + msgs.size() + ", offline2Slave: "
//        + offlineToSlaveCnt + ", slaveToMaster: " + slaveToMasterCnt
//        + ", process latency: " + minLatency + " - " + maxLatency);
  }
}
