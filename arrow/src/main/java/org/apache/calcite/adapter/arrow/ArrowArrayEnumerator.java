package org.apache.calcite.adapter.arrow;

import org.apache.calcite.linq4j.Enumerator;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.FieldVector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Created by masayuki on 2017/12/24.
 */
public class ArrowArrayEnumerator implements Enumerator<Object[]> {

    private Logger logger = LoggerFactory.getLogger(ArrowArrayEnumerator.class);

    private VectorSchemaRoot[] vectorSchemaRoots;
    private int[] fields;

    private int index = 0;
    private int currentPos = 0;

    public ArrowArrayEnumerator(VectorSchemaRoot[] vectorSchemaRoots, int[] fields) {
        this.vectorSchemaRoots = vectorSchemaRoots;
        this.fields = fields;
    }

    public ArrowArrayEnumerator(VectorSchemaRoot[] vectorSchemaRoots) {
        this(vectorSchemaRoots, EnumerableUtils.identityList(vectorSchemaRoots[0].getFieldVectors().size()));
    }

    @Override
    public void close() {}

    @Override
    public void reset() {
        this.index = 0;
        this.currentPos = 0;
    }

    @Override
    public boolean moveNext() {
        if (this.currentPos < (this.vectorSchemaRoots[this.index].getRowCount() - 1)) {
            this.currentPos ++;
            return true;
        } else if (this.index < (this.vectorSchemaRoots.length - 1)) {
            this.index ++;
            this.currentPos = 0;
        }
        return false;
    }

    @Override
    public Object[] current() {
        return Arrays.stream(this.fields).mapToObj(fieldIndex -> {
            FieldVector fieldVector  = vectorSchemaRoots[this.index].getFieldVectors().get(fieldIndex);
            if (fieldVector.getAccessor().getValueCount() < this.currentPos) {
                return "NULL";
            } else {
                return fieldVector.getAccessor().getObject(this.currentPos);
            }
        }).toArray();
    }
}
