package org.apache.calcite.adapter.arrow;

/**
 * Enumerator for Filter
 */
public abstract class ArrowFilterProcessor extends AbstractArrowProcessor {

  public ArrowFilterProcessor(VectorSchemaRootContainer input) {
    super(input);
  }
}
