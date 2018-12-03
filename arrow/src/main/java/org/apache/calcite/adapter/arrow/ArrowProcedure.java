package org.apache.calcite.adapter.arrow;

public interface ArrowProcedure {
  VectorSchemaRootContainer execute();
}
