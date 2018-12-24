package org.apache.calcite.adapter.arrow;

public interface ArrowAggregateKeySelector<T> {
  <TKey> TKey getKey(VectorSchemaRootContainer input, int i, int j);
}
