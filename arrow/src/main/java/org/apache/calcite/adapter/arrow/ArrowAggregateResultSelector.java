package org.apache.calcite.adapter.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

public interface ArrowAggregateResultSelector<T0, T1> {
  void init(BufferAllocator allocator);
  void apply(int idx, T0 key, T1 value);
  VectorSchemaRoot build();
}
