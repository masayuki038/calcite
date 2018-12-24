package org.apache.calcite.adapter.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;

import java.util.HashMap;
import java.util.Map;

abstract public class AbstractArrowProcedure<T> implements ArrowProcedure<T> {

  protected VectorSchemaRootContainer input;

  protected AbstractArrowProcedure(VectorSchemaRootContainer input) {
    this.input = input;
  }

  public String getFieldVectorName(int i) {
    assert(input.getVectorSchemaRootCount() > 0);
    assert(input.getFieldCount(0) > i);
    return input.getFieldVector(0, i).getField().getName();
  }

  public <TKey, TAccumulate> VectorSchemaRootContainer groupBy(
      final ArrowAggregateKeySelector<TKey> keySelector,
      final Function0<TAccumulate> accumulatorInitializer,
      final ArrowAggregateAccumulatorAdder<TAccumulate> accumulatorAdder,
      final ArrowAggregateResultSelector resultSelector) {
    return groupBy_(
      new HashMap<TKey, TAccumulate>(),
      keySelector,
      accumulatorInitializer,
      accumulatorAdder,
      resultSelector);
  }

  private  <TKey, TAccumulate> VectorSchemaRootContainer groupBy_(
      final Map<TKey, TAccumulate> map,
      final ArrowAggregateKeySelector<TKey> keySelector,
      final Function0<TAccumulate> accumulatorInitializer,
      final ArrowAggregateAccumulatorAdder<TAccumulate> accumulatorAdder,
      final ArrowAggregateResultSelector resultSelector) {
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    for (int i = 0; i < this.input.getVectorSchemaRootCount(); i++) {
      for (int j = 0; j < this.input.getRowCount(i); j++) {
        TKey key = keySelector.getKey(this.input, i, j);
        TAccumulate accumulator = map.get(key);
        if (accumulator == null) {
          accumulator = accumulatorInitializer.apply();
          accumulator = accumulatorAdder.add(accumulator, this.input, i, j);
          map.put(key, accumulator);
        } else {
          TAccumulate accumulator0 = accumulator;
          accumulator = accumulatorAdder.add(accumulator, this.input, i, j);
          if (accumulator != accumulator0) {
            map.put(key, accumulator);
          }
        }
      }
    }
    return createVectorSchemaContainer(map, allocator, resultSelector);
  }

  private <TKey, TAccumulate> VectorSchemaRootContainer createVectorSchemaContainer(
    final Map<TKey, TAccumulate> map,
    BufferAllocator allocator,
    ArrowAggregateResultSelector resultSelector) {

    resultSelector.init(allocator);

    int idx = 0;
    for (Map.Entry<TKey, TAccumulate> e: map.entrySet()) {
      resultSelector.apply(idx++, e.getKey(), e.getValue());
    }
    VectorSchemaRoot vectorSchemaRoot = resultSelector.build(idx);
    UInt4Vector selectionVector = new UInt4Vector("selectionVector", allocator);
    return new VectorSchemaRootContainerImpl(new VectorSchemaRoot[]{vectorSchemaRoot}, selectionVector);
  }
}
