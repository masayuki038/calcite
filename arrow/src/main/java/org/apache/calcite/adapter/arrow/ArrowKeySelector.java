package org.apache.calcite.adapter.arrow;

public interface ArrowKeySelector<T> {
  <TKey> TKey getKey(VectorSchemaRootContainer input, int i, int j);
}
