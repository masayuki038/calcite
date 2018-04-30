package org.apache.calcite.adapter.arrow;

import org.apache.arrow.vector.FieldVector;
import org.apache.calcite.linq4j.Enumerator;

import java.util.List;

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
        int fieldSize = container.getFieldCount(rootIndex);
        Object[] current = new Object[fieldSize];
        for (int i = 0; i < fieldSize; i++) {
            FieldVector vector = container.getFieldVector(rootIndex, i);
            current[i] = vector.getAccessor().getObject(indexes[rootIndex][vectorIndex]);
        }
        return current;
    }

    @Override
    public boolean moveNext() {
        if (indexes == null) {
            indexes = filter();
            for (rootIndex = 0; rootIndex < indexes.length; rootIndex++) {
                if (indexes[rootIndex].length > 0) {
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
            index[i] = toArray(filter(container, i));
        }
        return index;
    }

    private int[] toArray(List<Integer> list) {
        return list.stream().mapToInt(Integer::intValue).toArray();
    }

    abstract public List<Integer> filter(VectorSchemaRootContainer container, int i);

//    public List<Integer> filter(VectorSchemaRootContainer container, int i) {
//        List<Integer> list = new ArrayList<>();
//        for (int j = 0; j < container.getRowCount(i); j++) {
//            FieldVector fieldVector =  container.getFieldVector(i, 2);
//            final Long inp2_ = (Long)fieldVector.getAccessor().getObject(j);
//            if (inp2_ != null && (Long) root.get("?0") != null && inp2_.longValue() == ((Long) root.get("?0")).longValue()) {
//                list.add(j);
//            }
//        }
//        return list;
//    }
}
