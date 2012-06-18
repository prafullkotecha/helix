package com.linkedin.helix.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.InstanceConfig;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.MessageState;

public class RollingUpgradeAnalysis
{
  private static Logger           LOG           =
                                                    Logger.getLogger(RollingUpgradeAnalysis.class);
  private static boolean          dump          = false;                                            ;
  final static ZNRecordSerializer _deserializer = new ZNRecordSerializer();

  static class Stats
  {
    int msgSentCount        = 0;
    int msgSentCount_O2S    = 0; // Offline to Slave
    int msgSentCount_S2M    = 0; // Slave to Master
    int msgSentCount_M2S    = 0; // Master to Slave
    int msgDeleteCount      = 0;
    int msgModifyCount      = 0;
    int curStateCreateCount = 0;
    int curStateUpdateCount = 0;
    int extViewCreateCount  = 0;
    int extViewUpdateCount  = 0;
  }

  static String getAttributeValue(String line, String attribute)
  {
    if (line == null)
      return null;
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

  static String findLastCSUpdateBetween(List<String> csUpdateLines, long start, long end)
  {
    long lastCSUpdateTimestamp = Long.MIN_VALUE;
    String lastCSUpdateLine = null;
    for (String line : csUpdateLines)
    {
      // ZNRecord record = getZNRecord(line);
      long timestamp = Long.parseLong(getAttributeValue(line, "time:"));
      if (timestamp >= start && timestamp <= end && timestamp > lastCSUpdateTimestamp)
      {
        lastCSUpdateTimestamp = timestamp;
        lastCSUpdateLine = line;
      }
    }
    assert (lastCSUpdateLine != null) : "No CS update between " + start + " - " + end;
    return lastCSUpdateLine;
  }

  static ZNRecord getZNRecord(String line)
  {
    ZNRecord record = null;
    String value = getAttributeValue(line, "data:");
    if (value != null)
    {
      record = (ZNRecord) _deserializer.deserialize(value.getBytes());
      // if (record == null)
      // {
      // System.out.println(line);
      // }
    }
    return record;
  }

  public static void main(String[] args) throws Exception
  {
    if (args.length != 3)
    {
      System.err.println("USAGE: ZkLogAnalyzer zkLogDir clusterName zkAddr");
      System.exit(1);
    }

    // get create-timestamp of "/" + clusterName
    // find all zk logs after that create-timestamp and parse them
    // save parsed log in /tmp/zkLogAnalyzor_zklog.parsed0,1,2...

    String zkLogDir = args[0];
    String clusterName = args[1];
    String zkAddr = args[2];
    // long startDisable = Long.parseLong(args[3]);
    long startDisable = Long.parseLong("1339789709024");
    CurrentState cs1=null;
    CurrentState cs2=null;
    InstanceConfig ic1=null;
    InstanceConfig ic2=null;
    ZkClient zkClient = new ZkClient(zkAddr);
    Stat clusterCreateStat = zkClient.getStat("/" + clusterName);
    System.out.println(clusterName + " created at " + clusterCreateStat.getCtime());
    while (zkLogDir.endsWith("/"))
    {
      zkLogDir = zkLogDir.substring(0, zkLogDir.length() - 1);
    }
    if (!zkLogDir.endsWith("/version-2"))
    {
      zkLogDir = zkLogDir + "/version-2";
    }
    File dir = new File(zkLogDir);
    File[] zkLogs = dir.listFiles(new FileFilter()
    {

      @Override
      public boolean accept(File file)
      {
        return file.isFile() && (file.getName().indexOf("log") != -1);
      }
    });

    // lastModified time -> zkLog
    TreeMap<Long, String> lastZkLogs = new TreeMap<Long, String>();
    for (File file : zkLogs)
    {
      if (file.lastModified() > clusterCreateStat.getCtime())
      {
        lastZkLogs.put(file.lastModified(), file.getAbsolutePath());
      }
    }

    List<String> parsedZkLogs = new ArrayList<String>();
    int i = 0;
    System.out.println("zk logs last modified later than " + clusterCreateStat.getCtime());
    for (Long lastModified : lastZkLogs.keySet())
    {
      String fileName = lastZkLogs.get(lastModified);
      System.out.println(lastModified + ": "
          + (fileName.substring(fileName.lastIndexOf('/') + 1)));

      String parsedFileName = "zkLogAnalyzor_zklog.parsed" + i;
      i++;
      ZKLogFormatter.main(new String[] { "log", fileName, parsedFileName });
      parsedZkLogs.add(parsedFileName);
    }

    // sessionId -> create liveInstance line
    Map<String, String> sessionMap = new HashMap<String, String>();

    // message send lines in time order
    // List<String> sendMessageLines = new ArrayList<String>();

    // CS update lines in time order
    List<String> csUpdateLines = new ArrayList<String>();

    String leaderSession = null;

    System.out.println();
    Stats stats = new Stats();
    long lastTestStartTimestamp = Long.MAX_VALUE;
    long controllerStartTime = 0;
    for (String parsedZkLog : parsedZkLogs)
    {

      FileInputStream fis = new FileInputStream(parsedZkLog);
      BufferedReader br = new BufferedReader(new InputStreamReader(fis));

      String inputLine;
      ZNRecordSerializer serializer = new  ZNRecordSerializer();
      while ((inputLine = br.readLine()) != null)
      {
        if (dump == true)
        {
        //  System.err.println(inputLine);
        }
        String timestamp = getAttributeValue(inputLine, "time:");
        if (timestamp == null)
        {
          continue;
        }
        long timestampVal = Long.parseLong(timestamp);
        if (timestampVal < clusterCreateStat.getCtime())
        {
          continue;
        }
      
        if (inputLine.indexOf("/" + clusterName + "/CONFIGS/PARTICIPANT") != -1)
        {
          dump = true;
          String type = getAttributeValue(inputLine, "type:");
          if (type.equals("setData") || type.equals("create") )
          {
            String data = getAttributeValue(inputLine, "data:");
            if (inputLine.indexOf("localhost_8901") > -1)
            {
              ic1 = new InstanceConfig((ZNRecord)serializer.deserialize(data.getBytes()));
            }
            if (inputLine.indexOf("localhost_8902") > -1)
            {
              ic2 = new InstanceConfig((ZNRecord)serializer.deserialize(data.getBytes()));
            }
            
          }else{
            System.out.println(type);
          }
        }
        if (inputLine.indexOf("CURRENTSTATES") != -1)
        {
          String type = getAttributeValue(inputLine, "type:");
          if (type.equals("setData"))
          {
            String data = getAttributeValue(inputLine, "data:");
            if (inputLine.indexOf("localhost_8901") > -1)
            {
              cs1 = new CurrentState((ZNRecord)serializer.deserialize(data.getBytes()));
            }
            if (inputLine.indexOf("localhost_8902") > -1)
            {
              cs2 = new CurrentState((ZNRecord)serializer.deserialize(data.getBytes()));
            }
            print(timestamp,cs1,cs2, ic1,ic2);
          }
        }
      } // end of [br.readLine()) != null]
    }
  }

  private static void print(String timestamp,CurrentState cs1, CurrentState cs2, InstanceConfig ic1, InstanceConfig ic2)
  {
    int onlineCount =0;
    if(cs1!=null){
      Map<String, String> partitionStateMap = cs1.getPartitionStateMap();
      for(String partition:partitionStateMap.keySet()){
        if(ic1.getInstanceEnabledForPartition(partition) && "MASTER".equalsIgnoreCase(partitionStateMap.get(partition))){
          onlineCount++;
        }
      }
    }
    if(cs2!=null){
      Map<String, String> partitionStateMap = cs2.getPartitionStateMap();
      for(String partition:partitionStateMap.keySet()){
        if("MASTER".equalsIgnoreCase(partitionStateMap.get(partition))){
          onlineCount++;
        }
      }
    }
    System.out.println("ONLINE COUNT "+ timestamp + " \t "+ onlineCount);
    // TODO Auto-generated method stub
    
  }
}
