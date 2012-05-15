package com.linkedin.helix.store.zk;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.log4j.Logger;

import com.linkedin.helix.store.PropertySerializer;
import com.linkedin.helix.store.PropertyStoreException;

public class ByteArrayUpdater<T> implements DataUpdater<byte[]>
{
  private static final Logger LOG = Logger.getLogger(ByteArrayUpdater.class);

  final DataUpdater<T>        _updater;
  final PropertySerializer<T> _serializer;

  ByteArrayUpdater(DataUpdater<T> updater, PropertySerializer<T> serializer)
  {
    _updater = updater;
    _serializer = serializer;
  }

  @Override
  public byte[] update(byte[] current)
  {
    T currentValue = null;
    try
    {
      currentValue = _serializer.deserialize(current);
      T updateValue = _updater.update(currentValue);
      return _serializer.serialize(updateValue);
    }
    catch (PropertyStoreException e)
    {
      LOG.error("Exception in update " + currentValue + ". Updater: " + _updater, e);
    }
    return null;
  }
}
