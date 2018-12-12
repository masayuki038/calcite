package org.apache.calcite.adapter.arrow;

public abstract class ArrowAggregateProcedure extends AbstractArrowProcedure {

  public ArrowAggregateProcedure(VectorSchemaRootContainer input) {
    super(input);
  }
}
