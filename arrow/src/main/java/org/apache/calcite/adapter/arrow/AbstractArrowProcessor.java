package org.apache.calcite.adapter.arrow;

abstract public class AbstractArrowProcessor implements ArrowProcessor {

  protected VectorSchemaRootContainer input;

  protected AbstractArrowProcessor(VectorSchemaRootContainer input) {
    this.input = input;
  }
}
