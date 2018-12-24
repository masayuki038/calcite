package org.apache.calcite.adapter.arrow;

import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;

public interface ArrowProcedure<T> {
  VectorSchemaRootContainer execute();
  <TKey, TAccumulate> VectorSchemaRootContainer groupBy(
    final ArrowAggregateKeySelector<TKey> keySelector,
    final Function0<TAccumulate> accumulatorInitializer,
    final ArrowAggregateAccumulatorAdder<TAccumulate> accumulatorAdder,
    final ArrowAggregateResultSelector resultSelector);
}
