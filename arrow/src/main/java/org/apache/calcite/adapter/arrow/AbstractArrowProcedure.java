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

abstract public class AbstractArrowProcedure<T> implements ArrowProcedure {

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
      final Function1<T, TKey> keySelector,
      final Function0<TAccumulate> accumulatorInitializer,
      final Function2<TAccumulate, T, TAccumulate> accumulatorAdder,
      final ArrowAggregateResultSelector resultSelector) {
    return groupBy_(
      new HashMap<TKey, TAccumulate>(),
      null,
      keySelector,
      accumulatorInitializer,
      accumulatorAdder,
      resultSelector);
  }

  private  <TKey, TAccumulate> VectorSchemaRootContainer groupBy_(
      final Map<TKey, TAccumulate> map,
      final Enumerable<T> enumerable,
      final Function1<T, TKey> keySelector,
      final Function0<TAccumulate> accumulatorInitializer,
      final Function2<TAccumulate, T, TAccumulate> accumulatorAdder,
      final ArrowAggregateResultSelector resultSelector) {
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    try (Enumerator<T> os = enumerable.enumerator()) {
      while (os.moveNext()) {
        T o = os.current();
        TKey key = keySelector.apply(o);
        TAccumulate accumulator = map.get(key);
        if (accumulator == null) {
          accumulator = accumulatorInitializer.apply();
          accumulator = accumulatorAdder.apply(accumulator, o);
          map.put(key, accumulator);
        } else {
          TAccumulate accumulator0 = accumulator;
          accumulator = accumulatorAdder.apply(accumulator, o);
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
    int idx = 0;
    for (Map.Entry<TKey, TAccumulate> e: map.entrySet()) {
      resultSelector.apply(idx++, e.getKey(), e.getValue());
    }
    VectorSchemaRoot vectorSchemaRoot = resultSelector.build();
    UInt4Vector selectionVector = new UInt4Vector("selectionVector", allocator);
    return new VectorSchemaRootContainerImpl(new VectorSchemaRoot[]{vectorSchemaRoot}, selectionVector);
  }
}
