package com.linkedin.helix.manager.zk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.PropertyPathConfig;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordDecorator;
import com.linkedin.helix.store.zk.ZNode;

public class ZkCachedDataAccessor implements DataAccessor
{
  private static Logger              LOG                    =
                                                                Logger.getLogger(ZkCachedDataAccessor.class);

  final String                       _clusterName;
  final ZkClient                     _zkClient;
  final InstanceType                 _instanceType;

  final ConcurrentMap<String, ZNode> _writeThroughCache     =
                                                                new ConcurrentHashMap<String, ZNode>();
  final ReadWriteLock                _writeThroughCacheLock =
                                                                new ReentrantReadWriteLock();

  // final ZkCache _cache;

  public ZkCachedDataAccessor(String clusterName,
                              ZkClient zkClient,
                              InstanceType instanceType)
  {
    _clusterName = clusterName;
    _zkClient = zkClient;
    _instanceType = instanceType;
    // _cache = new ZkCache("/" + clusterName, zkClient, null);
    //
    // _cache.updateCache(PropertyPathConfig.getPath(PropertyType.STATEMODELDEFS,
    // clusterName));
    // _cache.updateCache(PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName));
    // _cache.updateCache(PropertyPathConfig.getPath(PropertyType.IDEALSTATES,
    // clusterName));
    // _cache.updateCache(PropertyPathConfig.getPath(PropertyType.LIVEINSTANCES,
    // clusterName));
    // if (type == InstanceType.CONTROLLER)
    // {
    // _cache.updateCache(PropertyPathConfig.getPath(PropertyType.CURRENTSTATES,
    // clusterName));
    // }
  }

  @Override
  public boolean setProperty(PropertyType type, ZNRecord record, String... keys)
  {
    String path = PropertyPathConfig.getPath(type, _clusterName, keys);

    CreateMode mode =
        (type.isPersistent()) ? CreateMode.PERSISTENT : CreateMode.EPHEMERAL;
    ZNRecord mergedRecord = record;
    if (record.getDeltaList().size() > 0)
    {
      mergedRecord = new ZNRecord(record.getId());
      mergedRecord.merge(record);
    }

    switch (type)
    {
    case LEADER:
    case LIVEINSTANCES:
    case CONFIGS:
      // create only if absent, no merge
      try
      {
        _zkClient.create(path, mergedRecord, mode);
      }
      catch (ZkNoNodeException e)
      {
        String parentDir = path.substring(0, path.lastIndexOf('/'));
        _zkClient.createPersistent(parentDir, true);
        _zkClient.create(path, mergedRecord, mode);
      }
      catch (ZkNodeExistsException e)
      {
        // OK
      }
      break;
    case CURRENTSTATES:
    case EXTERNALVIEW:
      // TODO: if we need merge, then use update
      // single writer, merge on set
      // cache CURRENTSTATE on participant side
      // cache EXTERNALVIEW on controller side
      try
      {
        _writeThroughCacheLock.writeLock().lock();
        ZNRecord setRecord = mergedRecord;
        if (_writeThroughCache.containsKey(path))
        {
          // no merge for ExternalView
          if (type == PropertyType.CURRENTSTATES)
          {
            ZNRecord curRecord = (ZNRecord) _writeThroughCache.get(path).getData();
            if (curRecord != null)
            {
              curRecord.merge(record);
              setRecord = curRecord;
            }
          }
        }
        // TODO get stat from write
        _writeThroughCache.put(path, new ZNode(null, setRecord, null));
        
        try
        {
          _zkClient.writeData(path, setRecord);
        }
        catch (ZkNoNodeException e)
        {
          String parentDir = path.substring(0, path.lastIndexOf('/'));
          _zkClient.createPersistent(parentDir, true);
          _zkClient.create(path, setRecord, mode);
        }

      }
      finally
      {
        _writeThroughCacheLock.writeLock().unlock();
      }

      break;
    case MESSAGES:
      // TODO: add create() interface to DataAccessor
      if (_instanceType == InstanceType.CONTROLLER)
      {
        // create message
        try
        {
//          _zkClient.create(path, mergedRecord, mode);
          _zkClient.asyncCreate(path, mergedRecord, mode);
        }
        catch (ZkNoNodeException e)
        {
          String parentDir = path.substring(0, path.lastIndexOf('/'));
          _zkClient.createPersistent(parentDir, true);
//          _zkClient.create(path, mergedRecord, mode);
          _zkClient.asyncCreate(path, mergedRecord, mode);
        }
      }
      else if (_instanceType == InstanceType.PARTICIPANT)
      {
        // TODO: add CONTROLLER_PARTICIPANT
        // cache message on participant side only
        try
        {
          _writeThroughCacheLock.writeLock().lock();
          _writeThroughCache.put(path, new ZNode(null, mergedRecord, null));
//          _zkClient.writeData(path, mergedRecord);
          _zkClient.asyncWriteData(path, mergedRecord);
        }
        catch (ZkNoNodeException e)
        {
          String parentDir = path.substring(0, path.lastIndexOf('/'));
          _zkClient.createPersistent(parentDir, true);
          _zkClient.create(path, mergedRecord, mode);
        }
        finally
        {
          _writeThroughCacheLock.writeLock().unlock();
        }
      }
      break;
    default:
      try
      {
        _zkClient.writeData(path, mergedRecord);
      }
      catch (ZkNoNodeException e)
      {
        String parentDir = path.substring(0, path.lastIndexOf('/'));
        _zkClient.createPersistent(parentDir, true);
        _zkClient.create(path, mergedRecord, mode);
      }
      break;
    }

    return true;
  }

  @Override
  public boolean setProperty(PropertyType type, ZNRecordDecorator value, String... keys)
  {
    return setProperty(type, value.getRecord(), keys);
  }

  @Override
  public boolean updateProperty(PropertyType type, ZNRecord value, String... keys)
  {
    return setProperty(type, value, keys);
  }

  @Override
  public boolean updateProperty(PropertyType type,
                                ZNRecordDecorator value,
                                String... keys)
  {
    return updateProperty(type, value.getRecord(), keys);
  }

  @Override
  public ZNRecord getProperty(PropertyType type, String... keys)
  {
    String path = PropertyPathConfig.getPath(type, _clusterName, keys);
    ZNRecord getRecord = null;
    switch (type)
    {
    case MESSAGES:
      // need to cache MESSAGES on reading from participant side
      if (_instanceType == InstanceType.PARTICIPANT)
      {
        try
        {
          _writeThroughCacheLock.writeLock().lock();
          if (_writeThroughCache.containsKey(path))
          {
            getRecord = (ZNRecord) _writeThroughCache.get(path).getData();
          }
          else
          {
            getRecord = _zkClient.readData(path, true);
            _writeThroughCache.put(path, new ZNode(null, getRecord, null));
          }

        }
        finally
        {
          _writeThroughCacheLock.writeLock().unlock();
        }

      }
      else if (_instanceType == InstanceType.CONTROLLER)
      {
        getRecord = _zkClient.readData(path, true);
      }
      break;
    default:
      boolean isInCache = false;
      try
      {
        _writeThroughCacheLock.readLock().lock();
        if (_writeThroughCache.containsKey(path))
        {
          getRecord = (ZNRecord) _writeThroughCache.get(path).getData();
          isInCache = true;
        }
      }
      finally
      {
        _writeThroughCacheLock.readLock().unlock();
      }

      if (!isInCache)
      {
        getRecord = _zkClient.readData(path, true);
      }
      break;
    }

//    getRecord = _zkClient.readData(path, true);
    return getRecord;
  }

  @Override
  public <T extends ZNRecordDecorator> T getProperty(Class<T> clazz,
                                                     PropertyType type,
                                                     String... keys)
  {
    return ZNRecordDecorator.convertToTypedInstance(clazz, getProperty(type, keys));
  }

  @Override
  public boolean removeProperty(PropertyType type, String... keys)
  {
    String path = PropertyPathConfig.getPath(type, _clusterName, keys);
    switch (type)
    {
    case MESSAGES:
      // need to cache MESSAGES on reading from participant side
      if (_instanceType == InstanceType.PARTICIPANT)
      {
        try
        {
          _writeThroughCacheLock.writeLock().lock();
          _writeThroughCache.remove(path);
          _zkClient.delete(path);
        }
        finally
        {
          _writeThroughCacheLock.writeLock().unlock();
        }
      }
      break;
    default:
      _zkClient.delete(path);
      break;
    }

//    _zkClient.delete(path);
    return true;
  }

  @Override
  public List<String> getChildNames(PropertyType type, String... keys)
  {
    String path = PropertyPathConfig.getPath(type, _clusterName, keys);
    try
    {
      return _zkClient.getChildren(path);
    }
    catch (ZkNoNodeException e)
    {
      return Collections.emptyList();
    }
  }

  @Override
  public List<ZNRecord> getChildValues(PropertyType type, String... keys)
  {
    String path = PropertyPathConfig.getPath(type, _clusterName, keys);

    List<String> children = getChildNames(type, keys);
    if (children == null || children.size() == 0)
    {
      return Collections.emptyList();
    }

    List<ZNRecord> childRecords = new ArrayList<ZNRecord>();
    Stat stat = new Stat();
    ZNRecord childRecord;
    for (String child : children)
    {
      String childPath = path + "/" + child;
      switch (type)
      {
      case MESSAGES:
      case CURRENTSTATES:
        if (_instanceType == InstanceType.PARTICIPANT
            && _writeThroughCache.containsKey(childPath))
        {
          // TODO read lock
          childRecord = (ZNRecord) _writeThroughCache.get(childPath).getData();
          childRecords.add(childRecord);
        }
        else
        {
          childRecord = _zkClient.readDataAndStat(childPath, stat, true);
          if (childRecord != null)
          {
            childRecord.setVersion(stat.getVersion());
            childRecord.setCreationTime(stat.getCtime());
            childRecord.setModifiedTime(stat.getMtime());
            childRecords.add(childRecord);
          }
        }
        break;
      case EXTERNALVIEW:
        if (_instanceType == InstanceType.CONTROLLER
            && _writeThroughCache.containsKey(childPath))
        {
          // TODO read lock
          childRecord = (ZNRecord) _writeThroughCache.get(childPath).getData();
          childRecords.add(childRecord);
        }
        else
        {
          childRecord = _zkClient.readDataAndStat(childPath, stat, true);
          if (childRecord != null)
          {
            childRecord.setVersion(stat.getVersion());
            childRecord.setCreationTime(stat.getCtime());
            childRecord.setModifiedTime(stat.getMtime());
            childRecords.add(childRecord);
          }
        }
        break;
      default:
        childRecord = _zkClient.readDataAndStat(childPath, stat, true);
        if (childRecord != null)
        {
          childRecord.setVersion(stat.getVersion());
          childRecord.setCreationTime(stat.getCtime());
          childRecord.setModifiedTime(stat.getMtime());
          childRecords.add(childRecord);
        }
        break;
      }
      
//      childRecord = _zkClient.readDataAndStat(childPath, stat, true);
//      if (childRecord != null)
//      {
//        childRecord.setVersion(stat.getVersion());
//        childRecord.setCreationTime(stat.getCtime());
//        childRecord.setModifiedTime(stat.getMtime());
//        childRecords.add(childRecord);
//      }

    }

    return childRecords;
  }

  @Override
  public <T extends ZNRecordDecorator> List<T> getChildValues(Class<T> clazz,
                                                              PropertyType type,
                                                              String... keys)
  {
    List<ZNRecord> childs = getChildValues(type, keys);
    if (childs.size() > 0)
    {
      return ZNRecordDecorator.convertToTypedList(clazz, childs);
    }
    return Collections.emptyList();
  }

  @Override
  public <T extends ZNRecordDecorator> Map<String, T> getChildValuesMap(Class<T> clazz,
                                                                        PropertyType type,
                                                                        String... keys)
  {
    List<T> childs = getChildValues(clazz, type, keys);
    return Collections.unmodifiableMap(ZNRecordDecorator.convertListToMap(childs));
  }

}
