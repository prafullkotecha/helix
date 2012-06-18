package com.linkedin.helix.tools;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.linkedin.helix.ZNRecord;

public class GroupCommit2
{

  private static class Queue
  {
    final AtomicReference<Thread>      _running = new AtomicReference<Thread>();
    final ConcurrentLinkedQueue<Entry> _pending = new ConcurrentLinkedQueue<Entry>();
  }

  private static class Entry
  {
    final String   _key;
    final ZNRecord _record;
    AtomicBoolean        _sent = new AtomicBoolean(false);

    Entry(String key, ZNRecord record)
    {
      _key = key;
      _record = record;
    }
  }

  private final Queue[]                          _queues   = new Queue[100];
  private final ConcurrentHashMap<String, Entry> _inFlight =
                                                               new ConcurrentHashMap<String, Entry>();

  public GroupCommit2()
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

  public void commit(IStore store, String key, ZNRecord record) 
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
          processed.add(queue._pending.poll());

          Iterator<Entry> it = queue._pending.iterator();
          while (it.hasNext())
          {
            Entry ent = it.next();
            if (!ent._key.equals(processed.get(0)._key))
              continue;
            processed.add(ent);
            processed.get(0)._record.merge(ent._record);
            it.remove();
          }
          store.write(processed.get(0)._key, processed.get(0)._record);
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
}
