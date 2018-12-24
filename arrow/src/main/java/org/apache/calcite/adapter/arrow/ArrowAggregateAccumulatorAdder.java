package org.apache.calcite.adapter.arrow;

public interface ArrowAggregateAccumulatorAdder<T> {
  T add(T acc, VectorSchemaRootContainer input, int i, int j);
}