package com.linkedin.helix.store.zk;

import java.util.HashSet;
import java.util.Set;

public class ZNode
{
  private final String _name;

  public ZNode(String name, Object data)
  {
    _name = name;
    _childSet = new HashSet<String>();
    _data = data;
  }

  Object _data;
  Set<String> _childSet;

  public void addChild(String child)
  {
    _childSet.add(child);
  }

  public boolean hasChild(String child)
  {
    return _childSet.contains(child);
  }

  public void setData(Object data)
  {
    _data= data;    
  }
}
