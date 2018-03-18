package org.apache.calcite.adapter.arrow;

import org.apache.arrow.vector.FieldVector;

/**
 * A container for VectorSchemaRoot
 */
public interface VectorSchemaRootContainer {

    int getVectorSchemaRootCount();

    int getRowCount(int vectorSchemaRootIndex);

    int getFieldCount(int vectorSchemaRootIndex);

    FieldVector getFieldVector(int vectorSchemaRootIndex, int fieldIndex);

}
