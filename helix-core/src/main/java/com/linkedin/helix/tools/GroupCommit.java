package com.linkedin.helix.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import com.linkedin.helix.ZNRecord;

public class GroupCommit
{
  private static Logger                    logger      =
                                                           Logger.getLogger(GroupCommit.class);
  ConcurrentHashMap<String, AtomicInteger> inFlight    =
                                                           new ConcurrentHashMap<String, AtomicInteger>();
  // ConcurrentHashMap<String, Object> mergeLocks =
  // new ConcurrentHashMap<String, Object>();
  // ConcurrentHashMap<String, Object> writeLocks =
  // new ConcurrentHashMap<String, Object>();

  ConcurrentHashMap<String, ZNRecord>      dataMap     =
                                                           new ConcurrentHashMap<String, ZNRecord>();
  Map<String, ZNRecord>                    cache       =
                                                           new ConcurrentHashMap<String, ZNRecord>();
  ReadWriteLock[]                          _writeLocks = new ReadWriteLock[100];
  Lock[]                                   _mergeLocks = new Lock[100];

  {
    Arrays.fill(_mergeLocks, new ReentrantLock());
    Arrays.fill(_writeLocks, new ReentrantReadWriteLock());
  }
  AtomicInteger                            writeDelay  = new AtomicInteger();

  private static class Queue
  {
    final AtomicReference<Thread>      _running = new AtomicReference<Thread>();
    final ConcurrentLinkedQueue<Entry> _pending = new ConcurrentLinkedQueue<Entry>();
  }

  private static class Entry
  {
    final String   _key;
    final ZNRecord _record;
    AtomicBoolean  _sent = new AtomicBoolean(false);

    Entry(String key, ZNRecord record)
    {
      _key = key;
      _record = record;
    }
  }

  private final Queue[]                          _queues   = new Queue[100];
  private final ConcurrentHashMap<String, Entry> _inFlight =
                                                               new ConcurrentHashMap<String, Entry>();

  public GroupCommit()
  {
    // Don't use Arrays.fill();
    for (int i = 0; i < _queues.length; ++i)
    {
      _queues[i] = new Queue();
    }
  }

  private Queue getQueue(String key)
  {
    return _queues[(key.hashCode() & Integer.MAX_VALUE) % _queues.length];
  }

  public void commit3(IStore store, String key, ZNRecord record)
  {
    
    Queue queue = getQueue(key);
    Entry entry = new Entry(key, record);

    queue._pending.add(entry);

    while (!entry._sent.get())
    {
      if (queue._running.compareAndSet(null, Thread.currentThread()))
      {
        ArrayList<Entry> processed = new ArrayList<Entry>();
        try
        {
          if (queue._pending.peek() == null)
            return;

          // remove from queue.
          Entry first = queue._pending.poll();
          processed.add(first);

          String mergedKey = first._key;
          ZNRecord merged = cache.get(key);
          if (merged == null)
          {
            merged = new ZNRecord(first._record);
          }
          else
          {
            merged.merge(first._record);
          }
          Iterator<Entry> it = queue._pending.iterator();
          while (it.hasNext())
          {
            Entry ent = it.next();
            if (!ent._key.equals(mergedKey))
              continue;
            processed.add(ent);
            merged.merge(ent._record);
            //System.out.println("After merging:" + merged);
            it.remove();
          }
          store.write(mergedKey, merged);
          cache.put(mergedKey, merged);
        }
        finally
        {
          queue._running.set(null);
          for (Entry e : processed)
          {
            synchronized (e)
            {
              e._sent.set(true);
              e.notify();
            }
          }
        }
      }
      else
        synchronized (entry)
        {
          try
          {
            entry.wait(10);
          }
          catch (InterruptedException e)
          {
            e.printStackTrace();
          }
        }
    }
  }

  public boolean commit2(IStore store, String key, ZNRecord record)
  {

    // I like using the real Lock objects rather than just blank objects - it gives more
    // flexibility
    Lock ml = getMergeLock(key);
    ReadWriteLock wl = getWriteLock(key); // This does actually have to be a RWLock

    try
    {
      wl.readLock().lock();
      if (dataMap.containsKey(key)
          || (dataMap.putIfAbsent(key, new ZNRecord(record))) != null) // merge needed
      {
        ml.lock(); // prevent concurrent calls to merge()
        try
        {
          dataMap.get(key).merge(record);
        }
        finally
        {
          ml.unlock();
        }
      }
    }
    finally
    {
      wl.readLock().unlock();
    }
    // At this point, the merge is done. Try to commit
    if (wl.writeLock().tryLock())
    {
      try
      { // I'm just assuming that all of this logic is correct
        ZNRecord mergedRecord = null;
        mergedRecord = dataMap.get(key);
        dataMap.remove(key);
        if (mergedRecord != null)
        {
          ZNRecord cachedRecord = cache.get(key);
          if (cachedRecord != null)
          {
            cachedRecord.merge(mergedRecord);
            store.write(key, cachedRecord);
            cache.put(key, cachedRecord);
          }
          else
          {
            store.write(key, mergedRecord);
            cache.put(key, mergedRecord);
          }
        }
        writeDelay.set(0);
      }
      finally
      {
        wl.writeLock().unlock();
      }
    }
    else
    {
      int delayed = writeDelay.incrementAndGet();
      if (delayed == 32)
      { // ensure that only one thread does the write
        wl.writeLock().lock();
        try
        {
          // Write logic here (not duplicated for brevity and potential refactoring into a
          // separate method
          ZNRecord mergedRecord = null;
          mergedRecord = dataMap.get(key);
          dataMap.remove(key);
          if (mergedRecord != null)
          {
            ZNRecord cachedRecord = cache.get(key);
            if (cachedRecord != null)
            {
              cachedRecord.merge(mergedRecord);
              store.write(key, cachedRecord);
              cache.put(key, cachedRecord);
            }
            else
            {
              store.write(key, mergedRecord);
              cache.put(key, mergedRecord);
            }
          }
          writeDelay.set(0);
        }
        finally
        {
          wl.writeLock().unlock();
        }
      }
      return true;
    }

    return true;
  }

  public boolean commit(IStore store, String key, ZNRecord record)
  {
    long start = System.nanoTime();

    if (false)
    {
      logger.info("start time:" + start);
      return true;
    }
    Lock ml = getMergeLock(key);
    ReadWriteLock wl = getWriteLock(key);

    synchronized (ml)
    {
      if (!dataMap.containsKey(key))
      {
        inFlight.put(key, new AtomicInteger(1));
        dataMap.put(key, new ZNRecord(record));
      }
      else
      {
        inFlight.get(key).incrementAndGet();
        dataMap.get(key).merge(record);
      }
    }
    long mergeEnd = System.nanoTime();
    synchronized (wl)
    {
      ZNRecord mergedRecord = null;
      synchronized (ml)
      {
        mergedRecord = dataMap.get(key);
        if (mergedRecord != null)
          logger.info("Merged count:" + inFlight.get(key).get());
        dataMap.remove(key);
        inFlight.remove(key);
      }
      if (mergedRecord != null)
      {
        ZNRecord cachedRecord = cache.get(key);
        if (cachedRecord != null)
        {
          cachedRecord.merge(mergedRecord);
          store.write(key, cachedRecord);
          cache.put(key, cachedRecord);
        }
        else
        {
          store.write(key, mergedRecord);
          cache.put(key, mergedRecord);
        }
      }
    }
    long writeEnd = System.nanoTime();
    logger.info("commit time:" + (writeEnd - start) + " merge time:" + (mergeEnd - start)
        + " start time:" + start);

    return true;
  }

  private ReadWriteLock getWriteLock(String key)
  {
    return _writeLocks[(key.hashCode() & Integer.MAX_VALUE) % _writeLocks.length];
  }

  private Lock getMergeLock(String key)
  {
    return _mergeLocks[(key.hashCode() & Integer.MAX_VALUE) % _mergeLocks.length];
  }

  public static void main(String[] args) throws InterruptedException
  {
    final IStore store = new Store();
    final GroupCommit commit = new GroupCommit();
    ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(40);
    for (int i = 0; i < 2400; i++)
    {
      Runnable runnable = new MyClass(store, commit, i);
      newFixedThreadPool.submit(runnable);
    }
    Thread.sleep(10000);
    System.out.println(store.get("test"));
    System.out.println(store.get("test").getSimpleFields().size());
  }
}

class MyClass implements Runnable
{
  private final IStore      store;
  private final GroupCommit commit;
  private final int         i;

  public MyClass(IStore store, GroupCommit commit, int i)
  {
    this.store = store;
    this.commit = commit;
    this.i = i;
  }

  @Override
  public void run()
  {
    // System.out.println("START " + System.currentTimeMillis() + " --"
    // + Thread.currentThread().getId());
    ZNRecord znRecord = new ZNRecord("test");
    znRecord.setSimpleField("test_id" + i, "" + i);
    commit.commit3(store, "test", znRecord);
    store.get("test").getSimpleField("");
    // System.out.println("END " + System.currentTimeMillis() + " --"
    // + Thread.currentThread().getId());
  }

}

class Store implements IStore
{
  Map<String, ZNRecord> map = new HashMap<String, ZNRecord>();

  /*
   * (non-Javadoc)
   * 
   * @see com.linkedin.helix.tools.IStore#write(java.lang.String,
   * com.linkedin.helix.ZNRecord)
   */
  @Override
  public void write(String key, ZNRecord value)
  {
    System.err.println("Store.write()" + System.currentTimeMillis());
    map.put(key, value);
    try
    {
      Thread.sleep(50);
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.linkedin.helix.tools.IStore#get(java.lang.String)
   */
  @Override
  public ZNRecord get(String key)
  {
    return map.get(key);
  }

}
