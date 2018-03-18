package org.apache.calcite.adapter.arrow;

import org.apache.arrow.vector.FieldVector;
import org.apache.calcite.linq4j.Enumerator;

/**
 * Enumerator for Filter
 */
public abstract class ArrowFilterEnumerator implements Enumerator {

    private int[][] indexes;
    private int rootIndex = 0;
    private int vectorIndex = 0;
    private Enumerator inputEnumerator;

    public ArrowFilterEnumerator(Enumerator inputEnumerator) {
        this.inputEnumerator = inputEnumerator;
    }

    @Override
    public Object current() {
        VectorSchemaRootContainer container = (VectorSchemaRootContainer)inputEnumerator;
        int[] fieldIndexes = getProjectedIndexes();
        Object[] current = new Object[fieldIndexes.length];
        for (int i = 0; i < fieldIndexes.length; i++) {
            FieldVector vector = container.getFieldVector(rootIndex, fieldIndexes[i]);
            current[i] = vector.getAccessor().getObject(vectorIndex);
        }
        return current;
    }

    @Override
    public boolean moveNext() {
        if (this.indexes == null) {
            this.indexes = filter();
            for (int rootIndex = 0; rootIndex < this.indexes.length; rootIndex++) {
                if (this.indexes[rootIndex].length > 0) {
                    return true;
                }
            }
            return false;
        }

        if (vectorIndex >= (indexes[rootIndex].length - 1)) {
            if (rootIndex >= (indexes.length - 1)) {
                return false;
            }
            rootIndex ++;
            vectorIndex = 0;
        } else {
            vectorIndex ++;
        }
        return true;
    }

    @Override
    public void reset() { inputEnumerator.reset(); }

    @Override
    public void close() { inputEnumerator.close(); }

    protected int[][] filter() {
        VectorSchemaRootContainer container = (VectorSchemaRootContainer)inputEnumerator;
        int size = container.getVectorSchemaRootCount();
        int index[][] = new int[size][];
        for (int i = 0; i < size; i++) {
            index[i] = filter(container, i);
        }
        return index;
    }

    abstract public int[] filter(VectorSchemaRootContainer container, int i);
    abstract public int[] getProjectedIndexes();
}
