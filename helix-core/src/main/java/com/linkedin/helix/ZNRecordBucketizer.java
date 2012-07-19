package com.linkedin.helix;

import java.util.Map;

import org.apache.log4j.Logger;

public class ZNRecordBucketizer implements Bucketizer<ZNRecord>
{
  private static Logger LOG = Logger.getLogger(ZNRecordBucketizer.class);
  final int             _bucketSize;

  public ZNRecordBucketizer(int bucketSize)
  {
    if (bucketSize <= 0)
    {
      throw new IllegalArgumentException("bucketSize should be > 0 (was " + bucketSize
          + ")");
    }

    _bucketSize = bucketSize;
  }

  @Override
  public String getBucketName(String key)
  {
    int idx = key.lastIndexOf('_');
    if (idx < 0)
    {
      throw new IllegalArgumentException("Could NOT find partition# in " + key
          + ". partitionName should be in format of resourceName_partition#");
    }

    try
    {
      int partitionNb = Integer.parseInt(key.substring(idx + 1));
      int bucketNb = partitionNb / _bucketSize;
      return key.substring(0, idx) + "_bucket" + bucketNb;
    }
    catch (NumberFormatException e)
    {
      throw new IllegalArgumentException("Could NOT parse partition# ("
          + key.substring(idx + 1) + ") in " + key);
    }
  }

  @Override
  public Map<String, ZNRecord> bucketize(ZNRecord value)
  {
    // TODO Auto-generated method stub
    return null;
  }

  // @Override
  // public ZNRecord unbucketize(Map<String, ZNRecord> bucketizedValue)
  // {
  // ZNRecord record = null;
  // if (bucketizedValue != null && bucketizedValue.size() > 0)
  // {
  // for (String key : bucketizedValue.keySet())
  // {
  // ZNRecord bucketizedRecord = bucketizedValue.get(key);
  // if (bucketizedRecord == null)
  // {
  // continue;
  // }
  //
  // if (record == null)
  // {
  // record = new ZNRecord(bucketizedRecord.getId());
  // }
  //
  // record.merge(bucketizedRecord);
  // }
  // }
  // return record;
  // }

  public static void main(String[] args)
  {
    ZNRecordBucketizer bucketizer = new ZNRecordBucketizer(3);
    String[] partitionNames =
        { "TestDB_0", "TestDB_1", "TestDB_2", "TestDB_3", "TestBB_4" };
    for (String partitionName : partitionNames)
    {
      System.out.println(bucketizer.getBucketName(partitionName));
    }
  }
}
