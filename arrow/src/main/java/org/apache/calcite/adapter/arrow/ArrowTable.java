package org.apache.calcite.adapter.arrow;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import java.lang.reflect.Type;

/**
 * Table for Apache Arrow
 */
public class ArrowTable extends AbstractTable implements QueryableTable, TranslatableTable {

    private VectorSchemaRoot[] vectorSchemaRoots;
    private RelProtoDataType tProtoRowType;

    public ArrowTable(VectorSchemaRoot[] vectorSchemaRoots, RelProtoDataType tProtoRowType) {
        this.vectorSchemaRoots = vectorSchemaRoots;
        this.tProtoRowType = tProtoRowType;
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (this.tProtoRowType != null) {
            return this.tProtoRowType.apply(typeFactory);
        }
        return ArrowEmumerator.deduceRowType(this.vectorSchemaRoots, (JavaTypeFactory)typeFactory);
    }

    public Enumerable<Object> project(DataContext root, final int[] fields) {
        return new AbstractEnumerable<Object>() {
            @Override
            public Enumerable<Object> enumerator() {
                return new ArrowEnumerator(vectorSchemaRoots, fields);
            }
        };
    }

    public Expression getExpression(SchemaPlus schema, String tableName, Class<?> clazz) {
       return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
    }

    public Type getElementType() {
        return Object[].class;
    }

    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        throw new UnsupportedOperationException();
    }

    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        int fieldCount = relOptTable.getRowType().getFieldCount();
        int[] fields = EnumerableUtils.identityList(fieldCount);
        return new ArrowTableScan(context.getCluster(), relOptTable, this, fields);
    }
}
