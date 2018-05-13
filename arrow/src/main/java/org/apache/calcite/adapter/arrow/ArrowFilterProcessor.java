package org.apache.calcite.adapter.arrow;

import org.apache.arrow.vector.UInt4Vector;

/**
 * Enumerator for Filter
 */
public abstract class ArrowFilterProcessor extends AbstractArrowProcessor {

    public ArrowFilterProcessor(VectorSchemaRootContainer input) {
        super(input);
    }

    @Override
    public VectorSchemaRootContainer run() {
        UInt4Vector selectionVector = input.selectionVector();
        selectionVector.clear();
        for (int i = 0; i < input.getVectorSchemaRootCount(); i++) {
            for (int j = 0; j < input.getRowCount(i); j++) {

            }
        }
        return input;
    }
}
