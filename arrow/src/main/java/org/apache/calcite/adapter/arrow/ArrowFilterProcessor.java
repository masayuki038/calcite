package org.apache.calcite.adapter.arrow;

import org.apache.arrow.vector.UInt4Vector;

/**
 * Enumerator for Filter
 */
public abstract class ArrowFilterProcessor extends AbstractArrowProcessor {

    public ArrowFilterProcessor(VectorSchemaRootContainer input) {
        super(input);
    }
}
