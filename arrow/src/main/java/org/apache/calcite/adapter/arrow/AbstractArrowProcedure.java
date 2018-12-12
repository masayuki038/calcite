package org.apache.calcite.adapter.arrow;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
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

  protected String getFieldVectorName(int i) {
    assert(input.getVectorSchemaRootCount() > 0);
    assert(input.getFieldCount(0) > i);
    return input.getFieldVector(0, i).getField().getName();
  }

  public <TKey, TAccumulate> VectorSchemaRootContainer groupBy(
      Function1<T, TKey> keySelector,
      Function0<TAccumulate> accumulatorInitializer,
      Function2<TAccumulate, T, TAccumulate> accumulatorAdder,
      Function2<TKey, TAccumulate, VectorSchemaRootContainer> resultSelector) {
    return groupBy_(new HashMap<TKey, TAccumulate>(), null, keySelector, accumulatorInitializer, accumulatorAdder, resultSelector);
  }

  private  <TKey, TAccumulate> VectorSchemaRootContainer groupBy_(
      final Map<TKey, TAccumulate> map, Enumerable<T> enumerable,
      Function1<T, TKey> keySelector,
      Function0<TAccumulate> accumulatorInitializer,
      Function2<TAccumulate, T, TAccumulate> accumulatorAdder,
      final Function2<TKey, TAccumulate, VectorSchemaRootContainer> resultSelector) {
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
    //return new LookupResultEnumerable<>(map, resultSelector);
    return null;
  }
}
