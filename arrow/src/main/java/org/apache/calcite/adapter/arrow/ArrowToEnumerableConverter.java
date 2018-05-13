package org.apache.calcite.adapter.arrow;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

import java.lang.reflect.Modifier;
import java.util.List;

import static org.apache.calcite.adapter.enumerable.EnumUtils.NO_EXPRS;
import static org.apache.calcite.adapter.enumerable.EnumUtils.NO_PARAMS;

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

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        final BlockBuilder builder = new BlockBuilder();
        final ArrowRel.ArrowImplementor arrowArrowImplementor = new ArrowRel.ArrowImplementor(implementor, pref);
        final ArrowRel.Result result = arrowArrowImplementor.visitChild(0, getInput());
        Expression childExp =
                builder.append(
                        "child",
                        result.block);

        GotoStatement g = Expressions.return_(
                null, Expressions.new_(
                        ArrowToEnumerableEnumerator.class,
                        Expressions.list(Expressions.call(childExp, "run", NO_PARAMS))));

        builder.add(
                Expressions.return_(
                        null,
                        Expressions.new_(
                                BuiltInMethod.ABSTRACT_ENUMERABLE_CTOR.constructor,
                                // TODO: generics
                                //   Collections.singletonList(inputRowType),
                                NO_EXPRS,
                                ImmutableList.<MemberDeclaration>of(
                                        Expressions.methodDecl(
                                                Modifier.PUBLIC,
                                                BuiltInMethod.ENUMERABLE_ENUMERATOR.method.getReturnType(),
                                                BuiltInMethod.ENUMERABLE_ENUMERATOR.method.getName(),
                                                NO_PARAMS,
                                                Blocks.toFunctionBlock(g))))));

        final PhysType physType = PhysTypeImpl.of(
                implementor.getTypeFactory(), rowType, pref.prefer(JavaRowFormat.ARRAY));
        return implementor.result(physType, builder.toBlock());
    }
}
