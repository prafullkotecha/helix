package com.linkedin.helix;

import java.util.Map;

public interface Assembler<T>
{
  T assemble(Map<String, T> values);
}
