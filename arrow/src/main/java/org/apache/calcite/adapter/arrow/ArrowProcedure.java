package org.apache.calcite.adapter.arrow;

import org.apache.calcite.linq4j.function.Function0;

public interface ArrowProcedure<T> {
  VectorSchemaRootContainer execute();
  <TKey, TAccumulate> VectorSchemaRootContainer groupBy(
    final ArrowKeySelector<TKey> keySelector,
    final Function0<TAccumulate> accumulatorInitializer,
    final ArrowAggregateAccumulatorAdder<TAccumulate> accumulatorAdder,
    final ArrowAggregateResultSelector resultSelector);
}
