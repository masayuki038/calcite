package org.apache.calcite.adapter.arrow;

import org.apache.arrow.vector.FieldVector;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.runtime.Bindable;

import java.util.Collections;
import java.util.List;

/**
 * Created by masayuki on 2018/01/28.
 */
public class Baz2 implements Bindable {

    @Override
    public Enumerable bind(DataContext dataContext) {
        final Enumerator inputEnumerator = ((ArrowTable) dataContext
                .getRootSchema()
                .getSubSchema(getSchemaName())
                .getTable(getTableName()))
                .project(dataContext, getFieldIndexes()).enumerator();

        return new AbstractEnumerable() {
            @Override
            public Enumerator enumerator() {
                return new ArrowFilterEnumerator(inputEnumerator) {
                    @Override
                    public List<Integer> filter(VectorSchemaRootContainer container, int i) {
                        FieldVector field1 = container.getFieldVector(i, 0);
                        List<Integer> selected = Collections.emptyList();
                        for (int j = 0; j < container.getRowCount(i); j++) {
                            final Long inp2_ = (Long) field1.getAccessor().getObject(j);
                            if (inp2_ != null && (Long) dataContext.get("?0") != null && inp2_.longValue() == ((Long) dataContext.get("?0")).longValue()) {
                                selected.add(j);
                            }
                        }
                        return selected;
                    }
                };
            }
        };
    }

    protected int[] getFieldIndexes() { return new int[]{0, 1, 2, 3}; }
    protected String getSchemaName() { return "SAMPLES"; }
    protected String getTableName() { return "NATIONSSF"; }
}
