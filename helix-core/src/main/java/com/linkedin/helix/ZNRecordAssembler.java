package com.linkedin.helix;

import java.util.Map;

public class ZNRecordAssembler implements Assembler<ZNRecord>
{

  @Override
  public ZNRecord assemble(Map<String, ZNRecord> values)
  {
    ZNRecord record = null;
    if (values != null && values.size() > 0)
    {
      for (String key : values.keySet())
      {
        ZNRecord bucketizedRecord = values.get(key);
        if (bucketizedRecord == null)
        {
          continue;
        }

        if (record == null)
        {
          record = new ZNRecord(bucketizedRecord.getId());
        }

        record.merge(bucketizedRecord);
      }
    }
    return record;
  }

}
