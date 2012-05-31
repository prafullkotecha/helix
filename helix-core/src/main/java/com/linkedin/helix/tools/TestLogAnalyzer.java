package com.linkedin.helix.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TestLogAnalyzer
{
  static int getTime(String line)
  {
    int ret = -1;
    int idx = line.indexOf("time: ");
    if (idx != -1)
    {
      ret = Integer.parseInt(line.substring(idx + 6));
      // System.out.println("time: " + ret);
    }

    return ret;
  }

  public static void main(String[] args) throws Exception
  {
    if (args.length != 1)
    {
      System.err.println("USAGE: TestLogAnalyzer testLogDir");
      System.exit(1);
    }

    // find the latest test logs
    String testLogDir = args[0];
    File dir = new File(testLogDir);
    File[] testLogs = dir.listFiles(new FileFilter()
    {

      @Override
      public boolean accept(File file)
      {
        return file.isFile() 
            && (file.getName().indexOf("log") != -1)
            && (file.getName().indexOf("swp") == -1);
      }
    });

    long lastModTimeParticipant = Long.MIN_VALUE;
    long lastModTimeController = Long.MIN_VALUE;
    long lastModTime = Long.MIN_VALUE;

    String lastParticipantLog = null;
    String lastControllerLog = null;
    String lastTimestampLog = null;

    for (File file : testLogs)
    {
      if (file.getName().indexOf("process_start") != -1)
      {
        if (file.lastModified() > lastModTimeParticipant)
        {
          lastParticipantLog = file.getAbsolutePath();
          lastModTimeParticipant = file.lastModified();
        }
      }
      else if (file.getName().indexOf("manager_start") != -1)
      {
        if (file.lastModified() > lastModTimeController)
        {
          lastControllerLog = file.getAbsolutePath();
          lastModTimeController = file.lastModified();
        }
      }
      else if (file.getName().indexOf("test_timestamps") != -1)
      {
        if (file.lastModified() > lastModTime)
        {
          lastTimestampLog = file.getAbsolutePath();
          lastModTime = file.lastModified();
        }
      }
    }

    System.out.println("Use latest participant Log: " + lastParticipantLog.substring(lastControllerLog.lastIndexOf('/') + 1));
    System.out.println("Use latest controller Log: " + lastControllerLog.substring(lastControllerLog.lastIndexOf('/') + 1));
    System.out.println("Use latest timestamp Log: " + lastTimestampLog.substring(lastTimestampLog.lastIndexOf('/') + 1));

    List<Date> testDates = new ArrayList<Date>();
    FileInputStream fis = new FileInputStream(lastTimestampLog);
    BufferedReader br = new BufferedReader(new InputStreamReader(fis));
    String inputLine;
    while ((inputLine = br.readLine()) != null)
    {
      // System.out.println(inputLine);
      int idx = inputLine.lastIndexOf('_');
      String dateStr = inputLine.substring(0, idx + 4);
      // System.out.println(dateStr);
      SimpleDateFormat sdformat = new SimpleDateFormat("yyMMdd_HHmmss_SSS");
      Date date = sdformat.parse(dateStr);
      testDates.add(date);
      // System.out.println(date);
    }
    System.out.println(testDates);

    System.out.println("\ncontroller stats:");
    calStats(lastControllerLog, testDates);
    
    System.out.println("\nparticipant stats:");
    calStats(lastParticipantLog, testDates);

  }

  private static void calStats(String logFile,
                               List<Date> testDates) throws Exception  {
    SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
    FileInputStream fis;
    BufferedReader br;
    String inputLine;
    int readCnt = 0;
    int writeCnt = 0;
//    int updateCnt = 0;
    int deleteCnt = 0;
    int createCnt = 0;
    float totalReadTime = 0;
    float totalWriteTime = 0;
//    float totalUpdateTime = 0;
    float totalDeleteTime = 0;
    float totalCreateTime = 0;

    fis = new FileInputStream(logFile);
    br = new BufferedReader(new InputStreamReader(fis));

    for (Date testDate : testDates)
    {
      while ((inputLine = br.readLine()) != null)
      {
        if (inputLine.indexOf("ZkClient.java") != -1)
        {
          String dateStr = inputLine.substring(0, inputLine.indexOf('+'));
          Date date = sdformat.parse(dateStr);
          if (date.after(testDate))
          {
            System.out.println("getData:\t" + readCnt + "\t" + (totalReadTime / readCnt));
//            System.out.println("update:\t" + updateCnt + "\t"
//                + (totalUpdateTime / updateCnt));
            System.out.println("setData:\t" + writeCnt + "\t"
                + (totalWriteTime / writeCnt));
            System.out.println("delete:\t" + deleteCnt + "\t"
                + (totalDeleteTime / deleteCnt));
            System.out.println("create:\t" + createCnt + "\t"
                + (totalCreateTime / createCnt));

            
            System.out.println();
            System.out.println(testDate);

            readCnt = 0;
            writeCnt = 0;
//            updateCnt = 0;
            deleteCnt = 0;
            totalReadTime = 0;
            totalWriteTime = 0;
//            totalUpdateTime = 0;
            totalDeleteTime = 0;
            break;
          }

          if (inputLine.indexOf("getData.") != -1)
          {
            int time = getTime(inputLine);
            if (time >= 0)
            {
              readCnt++;
              totalReadTime += time;
            }
          }
//          else if (inputLine.indexOf("update.") != -1)
//          {
//            int time = getTime(inputLine);
//            if (time >= 0)
//            {
//              updateCnt++;
//              totalUpdateTime += time;
//            }
//          } 
          else if (inputLine.indexOf("setData.") != -1)
          {
            int time = getTime(inputLine);
            if (time >= 0)
            {
              writeCnt++;
              totalWriteTime += time;
            }
          } else if (inputLine.indexOf("delete.") != -1)
          {
            int time = getTime(inputLine);
            if (time >= 0)
            {
              deleteCnt++;
              totalDeleteTime += time;
            }
          } else if (inputLine.indexOf("asyncCreate.") != -1)
          {
            int time = getTime(inputLine);
            if (time >= 0)
            {
              createCnt++;
              totalCreateTime += time;
            }
          }
        }
      }
    } // end of for testDate

    // last run
    readCnt = 0;
    writeCnt = 0;
//    updateCnt = 0;
    deleteCnt = 0;
    createCnt = 0;
    totalReadTime = 0;
    totalWriteTime = 0;
//    totalUpdateTime = 0;
    totalDeleteTime = 0;
    totalCreateTime = 0;

    while ((inputLine = br.readLine()) != null)
    {
      if (inputLine.indexOf("ZkClient.java") != -1)
      {
        if (inputLine.indexOf("getData.") != -1)
        {
          int time = getTime(inputLine);
          if (time >= 0)
          {
            readCnt++;
            totalReadTime += time;
          }
        }
//        else if (inputLine.indexOf("update.") != -1)
//        {
//          int time = getTime(inputLine);
//          if (time >= 0)
//          {
//            updateCnt++;
//            totalUpdateTime += time;
//          }
//        } 
        else if (inputLine.indexOf("setData.") != -1)
        {
          int time = getTime(inputLine);
          if (time >= 0)
          {
            writeCnt++;
            totalWriteTime += time;
          }
        } else if (inputLine.indexOf("delete.") != -1)
        {
          int time = getTime(inputLine);
          if (time >= 0)
          {
            deleteCnt++;
            totalDeleteTime += time;
          }
        }  else if (inputLine.indexOf("asyncCreate.") != -1)
        {
          int time = getTime(inputLine);
          if (time >= 0)
          {
            createCnt++;
            totalCreateTime += time;
          }
        }
      }
    }
    System.out.println("getData:\t" + readCnt + "\t" + (totalReadTime / readCnt));
//    System.out.println("update:\t" + updateCnt + "\t" + (totalUpdateTime / updateCnt));
    System.out.println("setData:\t" + writeCnt + "\t"
        + (totalWriteTime / writeCnt));
    System.out.println("delete:\t" + deleteCnt + "\t"
        + (totalDeleteTime / deleteCnt));
    System.out.println("create:\t" + createCnt + "\t"
        + (totalCreateTime / createCnt));
  }
}

