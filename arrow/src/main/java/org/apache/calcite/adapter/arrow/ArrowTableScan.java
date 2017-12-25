package org.apache.calcite.adapter.arrow;

import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.Arrays;
import java.util.List;

/**
 * Created by masayuki on 2017/12/25.
 */
public class ArrowTableScan extends TableScan implements EnumerableRel {

    private RelOptTable relOptTable;
    private ArrowTable arrowTable;
    private int[] fields;

    public ArrowTable getArrowTable() {
        return this.arrowTable;
    }

    public ArrowTableScan(RelOptCluster cluster, RelOptTable relOptTable, ArrowTable arrowTable, int[] fields) {
        super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), relOptTable);
        this.relOptTable = relOptTable;
        this.arrowTable = arrowTable;
        this.fields = fields;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new ArrowTableScan(getCluster(), this.relOptTable, this.arrowTable, this.fields);
    }

    @Override
    public RelWriter explainTerms(RelWriter rw) {
        return super.explainTerms(rw).item("fields", Primitive.asList(this.fields));
    }

    @Override
    public RelDataType deriveRowType() {
        List<RelDataTypeField> fieldList = this.relOptTable.getRowType().getFieldList();
        RelDataTypeFactory.FieldInfoBuilder builder = getCluster().getTypeFactory().builder();
        Arrays.stream(this.fields).forEach(i -> builder.add(fieldList.get(i)));
        return builder.build();
    }

    @Override
    public void register(RelOptPlanner planner) {
        planner.addRule(ArrowProjectTableScanRule.INSTANCE);
    }

    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray());
        return implementor.result(physType, Blocks.toBlock(
                Expressions.call(
                        table.getExpression(ArrowTable.class),
                        "project",
                        implementor.getRootExpression(),
                        Expressions.constant(this.fields))
                )
        );
    }
}
