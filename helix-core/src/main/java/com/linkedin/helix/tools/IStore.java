package com.linkedin.helix.tools;

import com.linkedin.helix.ZNRecord;

public interface IStore
{

  public abstract void write(String key, ZNRecord value);

  public abstract ZNRecord get(String key);

}
