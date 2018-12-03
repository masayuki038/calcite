package org.apache.calcite.adapter.arrow;

/**
 * Enumerator for Filter
 */
public abstract class ArrowFilterProcedure extends AbstractArrowProcedure {

  public ArrowFilterProcedure(VectorSchemaRootContainer input) {
    super(input);
  }
}
