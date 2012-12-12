package com.linkedin.helix;

public class ZkItem<T>
{
  public final String _path;
  public final T _data;

  public ZkItem(String path, T data)
  {
    _path = path;
    _data = data;
  }
}
