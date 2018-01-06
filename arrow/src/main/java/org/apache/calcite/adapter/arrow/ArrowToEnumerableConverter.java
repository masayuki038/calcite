package org.apache.calcite.adapter.arrow;

import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.List;

/**
 * Relational expression representing a scan of a table in a Arrow data source.
 */
public class ArrowToEnumerableConverter
extends ConverterImpl
implements EnumerableRel {
    protected ArrowToEnumerableConverter(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input) {
        super(cluster, ConventionTraitDef.INSTANCE, traits, input);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new ArrowToEnumerableConverter(getCluster(), traitSet, sole(inputs));
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(.1);
    }

    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        final BlockBuilder builder = new BlockBuilder();
        final ArrowRel.Implementor arrowImplementor = new ArrowRel.Implementor(implementor, pref);
        final Result result = arrowImplementor.visitChild(0, getInput());
        Expression childExp =
                builder.append(
                        "child",
                        result.block);
        builder.add(childExp);

        final PhysType physType = PhysTypeImpl.of(
                implementor.getTypeFactory(), rowType, pref.prefer(JavaRowFormat.ARRAY));
        return implementor.result(physType, builder.toBlock());
    }
}
