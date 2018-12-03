package org.apache.calcite.adapter.arrow;

abstract public class AbstractArrowProcedure implements ArrowProcedure {

  protected VectorSchemaRootContainer input;

  protected AbstractArrowProcedure(VectorSchemaRootContainer input) {
    this.input = input;
  }
}
