package com.linkedin.helix.manager.zk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.log4j.Logger;

import com.linkedin.helix.Assembler;
import com.linkedin.helix.BaseDataAccessor;
import com.linkedin.helix.GroupCommit;
import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixException;
import com.linkedin.helix.HelixProperty;
import com.linkedin.helix.PropertyKey;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordUpdater;

public class ZKHelixDataAccessor implements HelixDataAccessor
{
  private static Logger                    LOG          =
                                                            Logger.getLogger(ZKHelixDataAccessor.class);
  private final BaseDataAccessor<ZNRecord> _baseDataAccessor;
  private final String                     _clusterName;
  private final Builder                    _propertyKeyBuilder;
  private final GroupCommit                _groupCommit = new GroupCommit();

  public ZKHelixDataAccessor(String clusterName,
                             BaseDataAccessor<ZNRecord> baseDataAccessor)
  {
    _clusterName = clusterName;
    _baseDataAccessor = baseDataAccessor;
    _propertyKeyBuilder = new PropertyKey.Builder(_clusterName);
  }

  @Override
  public <T extends HelixProperty> boolean createProperty(PropertyKey key, T value)
  {
    PropertyType type = key.getType();
    String path = key.getPath();
    int options = constructOptions(type);
    return _baseDataAccessor.create(path, value.getRecord(), options);
  }

  @Override
  public <T extends HelixProperty> boolean setProperty(PropertyKey key, T value)
  {
    PropertyType type = key.getType();
    if (!value.isValid())
    {
      throw new HelixException("The ZNRecord for " + type + " is not valid.");
    }

    String path = key.getPath();
    int options = constructOptions(type);
    return _baseDataAccessor.set(path, value.getRecord(), options);
  }

  @Override
  public <T extends HelixProperty> boolean updateProperty(PropertyKey key, T value)
  {
    PropertyType type = key.getType();
    String path = key.getPath();
    int options = constructOptions(type);

    boolean success;
    switch (type)
    {
    case CURRENTSTATES:
      success = _groupCommit.commit(_baseDataAccessor, path, value.getRecord());
      break;
    default:
      success =
          _baseDataAccessor.update(path, new ZNRecordUpdater(value.getRecord()), options);
      break;
    }
    return success;
  }

  @Override
  public <T extends HelixProperty> T getProperty(PropertyKey key)
  {
    PropertyType type = key.getType();
    String path = key.getPath();
    int options = constructOptions(type);

    ZNRecord record = null;
    try
    {
      record = _baseDataAccessor.get(path, null, options);
    }
    catch (ZkNoNodeException e)
    {
      // OK
    }

    @SuppressWarnings("unchecked")
    T t = (T) HelixProperty.convertToTypedInstance(key.getTypeClass(), record);
    return t;
  }

  @Override
  public <T extends HelixProperty> List<T> getProperty(List<PropertyKey> keys)
  {
    if (keys == null || keys.size() == 0)
    {
      return Collections.emptyList();
    }

    List<T> childValues = new ArrayList<T>();

    List<String> paths = new ArrayList<String>();
    for (PropertyKey key : keys)
    {
      paths.add(key.getPath());
    }

    List<ZNRecord> children = _baseDataAccessor.get(paths, null, 0);

    for (int i = 0; i < keys.size(); i++)
    {
      PropertyKey key = keys.get(i);
      ZNRecord record = children.get(i);
      if (record != null)
      {
        @SuppressWarnings("unchecked")
        T t = (T) HelixProperty.convertToTypedInstance(key.getTypeClass(), record);
        childValues.add(t);
      }
      else
      {
        childValues.add(null);
      }
    }

    return childValues;
  }

  @Override
  public <T extends HelixProperty> Map<String, T> getPropertyMap(List<PropertyKey> keys,
                                                          List<Assembler<ZNRecord>> assemblers)
  {
    if (keys == null || keys.size() == 0)
    {
      return Collections.emptyMap();
    }

    List<T> children = getProperty(keys, assemblers);
    Map<String, T> childValuesMap = new HashMap<String, T>();
    for (T t : children)
    {
      if (t != null)
      {
        childValuesMap.put(t.getRecord().getId(), t);
      }
    }

    return childValuesMap;
  }

  @Override
  public <T extends HelixProperty> T getProperty(PropertyKey key,
                                                 Assembler<ZNRecord> assembler)
  {
    PropertyType type = key.getType();
    String path = key.getPath();
    int options = constructOptions(type);
    ZNRecord record = null;

    if (assembler != null)
    {
      List<String> childNames = _baseDataAccessor.getChildNames(path, options);
      List<String> paths = new ArrayList<String>();
      for (String childName : childNames)
      {
        String childPath = path + "/" + childName;
        paths.add(childPath);
      }

      List<ZNRecord> records = _baseDataAccessor.get(paths, null, options);
      Map<String, ZNRecord> recordsMap = new HashMap<String, ZNRecord>();
      for (int i = 0; i < childNames.size(); i++)
      {
        ZNRecord bucketizedRecord = records.get(i);
        if (bucketizedRecord != null)
        {
          recordsMap.put(childNames.get(i), bucketizedRecord);
        }
      }

      record = assembler.assemble(recordsMap);

      @SuppressWarnings("unchecked")
      T t = (T) HelixProperty.convertToTypedInstance(key.getTypeClass(), record);
      return t;
    }
    else
    {
      return getProperty(key);
    }
  }

  @Override
  public <T extends HelixProperty> List<T> getProperty(List<PropertyKey> keys,
                                                       List<Assembler<ZNRecord>> assemblers)
  {
    if (keys == null || keys.size() == 0)
    {
      return Collections.emptyList();
    }

    List<T> childValues = new ArrayList<T>();

    if (assemblers != null)
    {
      for (int i = 0; i < keys.size(); i++)
      {
        PropertyKey key = keys.get(i);
        Assembler<ZNRecord> assembler = assemblers.get(i);

        T t = getProperty(key, assembler);
        childValues.add(t);
      }
      return childValues;
    }
    else
    {
      return getProperty(keys);
    }
  }

  @Override
  public boolean removeProperty(PropertyKey key)
  {
    PropertyType type = key.getType();
    String path = key.getPath();
    return _baseDataAccessor.remove(path);
  }

  @Override
  public List<String> getChildNames(PropertyKey key)
  {
    PropertyType type = key.getType();
    String parentPath = key.getPath();
    int options = constructOptions(type);
    return _baseDataAccessor.getChildNames(parentPath, options);
  }

  @Override
  public <T extends HelixProperty> List<T> getChildValues(PropertyKey key)
  {
    PropertyType type = key.getType();
    String parentPath = key.getPath();
    int options = constructOptions(type);
    List<T> childValues = new ArrayList<T>();

    List<ZNRecord> children = _baseDataAccessor.getChildren(parentPath, null, options);

    for (ZNRecord record : children)
    {
      // HelixProperty typedInstance =
      @SuppressWarnings("unchecked")
      T t = (T) HelixProperty.convertToTypedInstance(key.getTypeClass(), record);
      childValues.add(t);
    }
    return childValues;
  }

  @Override
  public <T extends HelixProperty> Map<String, T> getChildValuesMap(PropertyKey key)
  {
    PropertyType type = key.getType();
    String parentPath = key.getPath();
    int options = constructOptions(type);
    // List<ZNRecord> children = _baseDataAccessor.getChildren(parentPath, null, options);
    List<T> children = getChildValues(key);
    Map<String, T> childValuesMap = new HashMap<String, T>();
    // for (ZNRecord record : children)
    for (T t : children)
    {
      // @SuppressWarnings("unchecked")
      // T t = (T) HelixProperty.convertToTypedInstance(key.getTypeClass(), record);
      childValuesMap.put(t.getRecord().getId(), t);
    }
    return childValuesMap;
  }

  @Override
  public Builder keyBuilder()
  {
    return _propertyKeyBuilder;
  }

  private int constructOptions(PropertyType type)
  {
    int options = 0;
    if (type.isPersistent())
    {
      options = options | BaseDataAccessor.Option.PERSISTENT;
    }
    else
    {
      options = options | BaseDataAccessor.Option.EPHEMERAL;
    }
    return options;
  }

  @Override
  public <T extends HelixProperty> boolean[] createChildren(List<PropertyKey> keys,
                                                            List<T> children)
  {
    // TODO: add validation
    int options = -1;
    List<String> paths = new ArrayList<String>();
    List<ZNRecord> records = new ArrayList<ZNRecord>();
    for (int i = 0; i < keys.size(); i++)
    {
      PropertyKey key = keys.get(i);
      PropertyType type = key.getType();
      String path = key.getPath();
      paths.add(path);
      HelixProperty value = children.get(i);
      records.add(value.getRecord());
      options = constructOptions(type);
    }
    return _baseDataAccessor.createChildren(paths, records, options);
  }

  @Override
  public <T extends HelixProperty> boolean[] setChildren(List<PropertyKey> keys,
                                                         List<T> children)
  {
    int options = -1;
    List<String> paths = new ArrayList<String>();
    List<ZNRecord> records = new ArrayList<ZNRecord>();
    for (int i = 0; i < keys.size(); i++)
    {
      PropertyKey key = keys.get(i);
      PropertyType type = key.getType();
      String path = key.getPath();
      paths.add(path);
      HelixProperty value = children.get(i);
      records.add(value.getRecord());
      options = constructOptions(type);
    }
    return _baseDataAccessor.setChildren(paths, records, options);

  }

  @Override
  public BaseDataAccessor<ZNRecord> getBaseDataAccessor()
  {
    return _baseDataAccessor;
  }

  @Override
  public <T extends HelixProperty> boolean[] updateChildren(List<String> paths,
                                                            List<DataUpdater<ZNRecord>> updaters,
                                                            int options)
  {
    return _baseDataAccessor.updateChildren(paths, updaters, options);
  }

}
