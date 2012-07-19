package com.linkedin.helix.manager.zk;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

import com.linkedin.helix.BaseDataAccessor;
import com.linkedin.helix.PropertyKey;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordBucketizer;
import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.model.CurrentState;

public class TestZKHelixDataAccessor extends ZkUnitTestBase
{

  @Test
  public void testBucketize()
  {
    System.out.println("START " + getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));

    String clusterName = getTestClassName() + "_" + getTestMethodName();
    BaseDataAccessor<ZNRecord> baseAccessor =
        new ZkBaseDataAccessor<ZNRecord>(_gZkClient);
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);

    Builder keyBuilder = new Builder(clusterName);
    ZNRecordBucketizer bucketizer = new ZNRecordBucketizer(1);
//    ZNRecordAssembler assembler = new ZNRecordAssembler();

    // set TestDB cs meta data
    CurrentState csMeta = new CurrentState("TestDB");
    csMeta.setStateModelDefRef("MasterSlave");
    csMeta.setBucketSize(1);
    csMeta.setSessionId("session_0");


    accessor.setProperty(keyBuilder.currentState("host_0",
                                                 "session_0",
                                                 "TestDB"),
                         csMeta);


    // set TestDB_0 in bucket 0
    CurrentState cs = new CurrentState("TestDB");
    cs.setStateModelDefRef("MasterSlave");
    cs.setSessionId("session_0");
    cs.setState("TestDB_0", "SLAVE");

    accessor.setProperty(keyBuilder.currentState("host_0",
                                                 "session_0",
                                                 "TestDB",
                                                 bucketizer.getBucketName("TestDB_0")),
                         cs);

    // set TestDB_1 in bucket 1
    cs = new CurrentState("TestDB");
    cs.setStateModelDefRef("MasterSlave");
    cs.setSessionId("session_0");
    cs.setState("TestDB_1", "MASTER");

    accessor.setProperty(keyBuilder.currentState("host_0",
                                                 "session_0",
                                                 "TestDB",
                                                 bucketizer.getBucketName("TestDB_1")),
                         cs);

    // get cs bucket
    cs =
        accessor.getProperty(keyBuilder.currentState("host_0",
                                                     "session_0",
                                                     "TestDB",
                                                     bucketizer.getBucketName("TestDB_0")));
    System.out.println(cs);

    // get cs
    cs =
        accessor.getProperty(keyBuilder.currentState("host_0", "session_0", "TestDB"));
    System.out.println(cs);
    List<PropertyKey> keys = new ArrayList<PropertyKey>();
    keys.add(keyBuilder.currentState("host_0",
                                     "session_0",
                                     "TestDB",
                                     bucketizer.getBucketName("TestDB_0")));
    keys.add(keyBuilder.currentState("host_0",
                                     "session_0",
                                     "TestDB",
                                     bucketizer.getBucketName("TestDB_1")));
    List<CurrentState> csList = accessor.getProperty(keys);
    System.out.println(csList);


    // set cs meta data
    csMeta = new CurrentState("MyDB");
    csMeta.setStateModelDefRef("MasterSlave");
    csMeta.setBucketSize(1);
    csMeta.setSessionId("session_0");


    accessor.setProperty(keyBuilder.currentState("host_0",
                                                 "session_0",
                                                 "MyDB"),
                                                 csMeta);
    // set MyDB_0 in bucket 0
    cs = new CurrentState("MyDB");
    cs.setStateModelDefRef("MasterSlave");
    cs.setSessionId("session_0");
    cs.setState("MyDB_0", "MASTER");

    accessor.setProperty(keyBuilder.currentState("host_0",
                                                 "session_0",
                                                 "MyDB",
                                                 bucketizer.getBucketName("MyDB_0")),
                         cs);

    // set TestDB_1 in bucket 1
    cs = new CurrentState("MyDB");
    cs.setStateModelDefRef("MasterSlave");
    cs.setSessionId("session_0");
    cs.setState("MyDB_1", "SLAVE");

    accessor.setProperty(keyBuilder.currentState("host_0",
                                                 "session_0",
                                                 "MyDB",
                                                 bucketizer.getBucketName("MyDB_1")),
                         cs);


    keys.clear();
    keys.add(keyBuilder.currentState("host_0",
                                     "session_0",
                                     "TestDB"));
    keys.add(keyBuilder.currentState("host_0",
                                     "session_0",
                                     "MyDB"));
//    List<Assembler<ZNRecord>> assemblers = new ArrayList<Assembler<ZNRecord>>(2);
//    assemblers.add(assembler);
//    assemblers.add(assembler);

    csList = accessor.getProperty(keys);
    System.out.println(csList);
    
    Map<String, CurrentState> csMap = accessor.getChildValuesMap(keyBuilder.currentStates("host_0", "session_0"));
    System.out.println(csMap);


    System.out.println("END " + getTestMethodName() + " at "
            + new Date(System.currentTimeMillis()));
  }
}
