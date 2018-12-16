package org.apache.calcite.adapter.arrow;

import com.google.common.collect.Lists;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.adapter.arrow.utils.Function4;

import java.util.HashMap;
import java.util.Map;

abstract public class AbstractArrowProcedure<T> implements ArrowProcedure {

  protected VectorSchemaRootContainer input;

  protected AbstractArrowProcedure(VectorSchemaRootContainer input) {
    this.input = input;
  }

  protected String getFieldVectorName(int i) {
    assert(input.getVectorSchemaRootCount() > 0);
    assert(input.getFieldCount(0) > i);
    return input.getFieldVector(0, i).getField().getName();
  }

  public <TKey, TAccumulate> VectorSchemaRootContainer groupBy(
      final Function1<T, TKey> keySelector,
      final Function0<TAccumulate> accumulatorInitializer,
      final Function1<BufferAllocator, FieldVector[]> fieldVectorsInitializer,
      final Function2<TAccumulate, T, TAccumulate> accumulatorAdder,
      final Function4<Integer, FieldVector[], TKey, TAccumulate, Void> resultSelector) {
    return groupBy_(
      new HashMap<TKey, TAccumulate>(),
      null,
      keySelector,
      accumulatorInitializer,
      fieldVectorsInitializer,
      accumulatorAdder,
      resultSelector);
  }

  private  <TKey, TAccumulate> VectorSchemaRootContainer groupBy_(
      final Map<TKey, TAccumulate> map,
      final Enumerable<T> enumerable,
      final Function1<T, TKey> keySelector,
      final Function0<TAccumulate> accumulatorInitializer,
      final Function1<BufferAllocator, FieldVector[]> fieldVectorsInitializer,
      final Function2<TAccumulate, T, TAccumulate> accumulatorAdder,
      final Function4<Integer, FieldVector[], TKey, TAccumulate, Void> resultSelector) {
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    FieldVector[] fieldVectors = fieldVectorsInitializer.apply(allocator);
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
    return createVectorSchemaContainer(map, fieldVectors, allocator, resultSelector);
  }

  private <TKey, TAccumulate> VectorSchemaRootContainer createVectorSchemaContainer(
    final Map<TKey, TAccumulate> map,
    FieldVector[] fieldVectors,
    BufferAllocator allocator,
    Function4<Integer, FieldVector[], TKey, TAccumulate, Void> resultSelector) {
    int idx = 0;
    for (Map.Entry<TKey, TAccumulate> e: map.entrySet()) {
      resultSelector.apply(idx++, fieldVectors, e.getKey(), e.getValue());
    }
    UInt4Vector selectionVector = new UInt4Vector("selectionVector", allocator);
    VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(Lists.newArrayList(fieldVectors));
    return new VectorSchemaRootContainerImpl(new VectorSchemaRoot[]{vectorSchemaRoot}, selectionVector);
  }
}
