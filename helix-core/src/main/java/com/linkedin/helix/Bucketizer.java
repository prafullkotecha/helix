package com.linkedin.helix;

import java.util.Map;

public interface Bucketizer<T>
{
  String getBucketName(String key);
  
  Map<String, T> bucketize(T value);
  
//  T unbucketize(Map<String, T> bucketizedValue);
}
