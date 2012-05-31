package com.linkedin.helix.manager.zk;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import com.linkedin.helix.ZNRecord;

public class ZNRecordStreamingSerializer implements ZkSerializer
{

  @Override
  public byte[] serialize(Object data) throws ZkMarshallingError
  {
    StringWriter sw = new StringWriter();
    ZNRecord record = (ZNRecord) data;
    try
    {

      JsonFactory f = new JsonFactory();
      JsonGenerator g = f.createJsonGenerator(sw);

      g.writeStartObject();
      
      // write id field
      g.writeStringField("id", record.getId());

      // write simepleFields
      g.writeObjectFieldStart("simpleFields");
      for (String key : record.getSimpleFields().keySet())
      {
        g.writeStringField(key, record.getSimpleField(key));
      }
      g.writeEndObject(); // for simpleFields
      
      // write listFields
      g.writeObjectFieldStart("listFields");
      for (String key : record.getListFields().keySet())
      {
//        g.writeStringField(key, record.getListField(key).toString());

//        g.writeObjectFieldStart(key);
        g.writeArrayFieldStart(key);
        List<String> list = record.getListField(key);
        for (String listValue : list)
        {
          g.writeString(listValue);          
        }
//        g.writeEndObject();
        g.writeEndArray();

      }
      g.writeEndObject(); // for listFields

      
      
      // write mapFields
      g.writeObjectFieldStart("mapFields");
      for (String key : record.getMapFields().keySet())
      {
//        g.writeStringField(key, record.getMapField(key).toString());
        g.writeObjectFieldStart(key);
        Map<String, String> map = record.getMapField(key);
        for (String mapKey : map.keySet())
        {
          g.writeStringField(mapKey, map.get(mapKey));          
        }
        g.writeEndObject();

      }
      g.writeEndObject(); // for mapFields
      
      
      
//      g.writeObjectFieldStart("name");
//      g.writeStringField("first", "Joe");
//      g.writeStringField("last", "Sixpack");
//      g.writeEndObject(); // for field 'name'
//      g.writeStringField("gender", "Gender.MALE");
//      g.writeBooleanField("verified", false);
//      g.writeFieldName("userImage"); // no 'writeBinaryField' (yet?)
//      // byte[] binaryData = ...;
//      // g.writeBinary(binaryData);
      
      
      
      g.writeEndObject();
      g.close(); // important: will force flushing of output, close underlying output
                 // stream
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    return sw.toString().getBytes();
  }

  @Override
  public Object deserialize(byte[] bytes) throws ZkMarshallingError
  {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    ZNRecord record = null;

    try
    {
    JsonFactory f = new JsonFactory();
    JsonParser jp = f.createJsonParser(bais);

    
    jp.nextToken(); // will return JsonToken.START_OBJECT (verify?)
    while (jp.nextToken() != JsonToken.END_OBJECT) {
      String fieldname = jp.getCurrentName();
      jp.nextToken(); // move to value, or START_OBJECT/START_ARRAY
      if ("id".equals(fieldname)) { // contains an object
//        Name name = new Name();
        record = new ZNRecord(jp.getText());
//        while (jp.nextToken() != JsonToken.END_OBJECT) {
//          String namefield = jp.getCurrentName();
//          jp.nextToken(); // move to value
//          if ("first".equals(namefield)) {
//            name.setFirst(jp.getText());
//          } else if ("last".equals(namefield)) {
//            name.setLast(jp.getText());
//          } else {
//            throw new IllegalStateException("Unrecognized field '"+fieldname+"'!");
//          }
//        }
//        user.setName(name);
      } else if ("simpleFields".equals(fieldname)) {
        while (jp.nextToken() != JsonToken.END_OBJECT) {
          String key = jp.getCurrentName();
          jp.nextToken(); // move to value
          record.setSimpleField(key, jp.getText());

        }
//        user.setGender(Gender.valueOf(jp.getText()));
      } else if ("mapFields".equals(fieldname)) {
//        user.setVerified(jp.getCurrentToken() == JsonToken.VALUE_TRUE);
        while (jp.nextToken() != JsonToken.END_OBJECT) {
          String key = jp.getCurrentName();
          record.setMapField(key, new TreeMap<String, String>());
          jp.nextToken(); // move to value

          while (jp.nextToken() != JsonToken.END_OBJECT) {
            String mapKey = jp.getCurrentName();
            jp.nextToken(); // move to value
            record.getMapField(key).put(mapKey, jp.getText());
          }
        }

      } else if ("listFields".equals(fieldname)) {
//        user.setUserImage(jp.getBinaryValue());
        while (jp.nextToken() != JsonToken.END_OBJECT) {
          String key = jp.getCurrentName();
          record.setListField(key, new ArrayList<String>());
          jp.nextToken(); // move to value
          while (jp.nextToken() != JsonToken.END_ARRAY) {
            record.getListField(key).add(jp.getText());
          }

        }

      } else {
        throw new IllegalStateException("Unrecognized field '"+fieldname+"'!");
      }
    }
    jp.close(); // ensure resources get cleaned up timely and properly
    } catch (Exception e)
    {
      e.printStackTrace();
    }
    
    return record;
  }

  public static void main(String[] args)
  {
    ZNRecord record = new ZNRecord("record");
    for (int i = 0; i < 100; i++)
    {
      record.setSimpleField("" + i, "" + i);
      record.setListField("" + i, new ArrayList<String>());
      for (int j = 0; j < 100; j++)
      {
        record.getListField("" + i).add("" + j);
      }

      
      record.setMapField("" + i, new TreeMap<String, String>());
      for (int j = 0; j < 100; j++)
      {
        record.getMapField("" + i).put("" + j, "" + j);
      }
    }

    ZNRecordStreamingSerializer serializer = new ZNRecordStreamingSerializer();
    long start = System.currentTimeMillis();
    for (int i = 0; i < 100; i++)
    {
      byte[] bytes = serializer.serialize(record);
//      System.out.println(new String(bytes));
      ZNRecord record2 = (ZNRecord) serializer.deserialize(bytes);
//      System.out.println(record2);
    }
    long end = System.currentTimeMillis();
    System.out.println("time used: " + (end - start));
  }
}
