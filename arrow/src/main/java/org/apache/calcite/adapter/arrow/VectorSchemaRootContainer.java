package org.apache.calcite.adapter.arrow;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;

/**
 * A container for VectorSchemaRoot
 */
public interface VectorSchemaRootContainer {

    int getVectorSchemaRootCount();

    int getRowCount(int vectorSchemaRootIndex);

    int getFieldCount(int vectorSchemaRootIndex);

    FieldVector getFieldVector(int vectorSchemaRootIndex, int fieldIndex);

    UInt4Vector selectionVector();
}
