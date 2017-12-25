package org.apache.calcite.adapter.arrow;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerator;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.FieldVector;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Created by masayuki on 2017/12/24.
 */
public class ArrowEnumerator implements Enumerator<Object> {

    private Logger logger = LoggerFactory.getLogger(ArrowEnumerator.class);

    private VectorSchemaRoot[] vectorSchemaRoots;
    private int[] fields;

    private int index = 0;
    private int currentPos = 0;

    public ArrowEnumerator(VectorSchemaRoot[] vectorSchemaRoots, int[] fields) {
        this.vectorSchemaRoots = vectorSchemaRoots;
        this.fields = fields;
    }

    public ArrowEnumerator(VectorSchemaRoot[] vectorSchemaRoots) {
        this(vectorSchemaRoots, EnumerableUtils.identityList(vectorSchemaRoots[0].getFieldVectors().size()));
    }

    public static RelDataType deduceRowType(VectorSchemaRoot vectorSchemaRoot, JavaTypeFactory typeFactory) {
        List<Pair<String, RelDataType>> ret = vectorSchemaRoot.getFieldVectors().stream().map(fieldVector -> {
            RelDataType relDataType = ArrowFieldType.of(fieldVector.getField().getType()).toType(typeFactory);
            return new Pair<String, RelDataType>(fieldVector.getField().getName(), relDataType);
        }).collect(Collectors.toList());
        return typeFactory.createStructType(ret);
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
            return true;
        }
        return false;
    }

    @Override
    public Object current() {
        if (fields.length == 1) {
            return getObject(fields[0]);
        }
        return Arrays.stream(fields).mapToObj(field -> getObject(field)).toArray();
    }

    private Object getObject(int fieldIndex) {
        FieldVector fieldVector = this.vectorSchemaRoots[this.index].getFieldVectors().get(fieldIndex);
        if (fieldVector.getAccessor().getValueCount() <= this.currentPos) {
            return "NULL";
        }
        return fieldVector.getAccessor().getObject(this.currentPos);
    }
}
