package org.apache.calcite.adapter.arrow;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;

import static org.apache.calcite.adapter.enumerable.EnumUtils.NO_PARAMS;

/**
 * Projection for Apache Arrow
 */
public class ArrowProject extends Project implements ArrowRel {

    private List<String> inputFieldNames;

    public ArrowProject(RelOptCluster cluster,
                        RelTraitSet traitSet,
                        RelNode input,
                        List<? extends RexNode> projects, RelDataType rowType) {
        super(cluster, traitSet, input, projects, rowType);
        this.inputFieldNames = input.getRowType().getFieldNames();
    }

    @Override
    public ArrowProject copy(RelTraitSet traitSet, RelNode input,
                                      List<RexNode> projects, RelDataType rowType) {
        return new ArrowProject(getCluster(), traitSet, input, projects,
                rowType);
    }

    @Override
    public ArrowRel.Result implement(ArrowImplementor arrowImplementor, EnumerableRel.Prefer pref) {
        final JavaTypeFactory typeFactory = arrowImplementor.getTypeFactory();
        final BlockBuilder builder = new BlockBuilder();
        final ArrowRel child = (ArrowRel)this.input;
        final ArrowRel.Result result = arrowImplementor.visitChild(0, child);
        final PhysType physType = PhysTypeImpl.of(typeFactory, getRowType(), pref.prefer(result.format));

        List<String> fieldNames = getRowType().getFieldNames();
        BlockBuilder projectedIndexesBody = new BlockBuilder();

        ConstantExpression[] constants = new ConstantExpression[fieldNames.size()];
        for(int i = 0; i < fieldNames.size(); i ++) {
            String fieldName = fieldNames.get(i);
            for(int j = 0; j < this.inputFieldNames.size(); j++) {
                if (fieldName.equals(this.inputFieldNames.get(j))) {
                    constants[i] = Expressions.constant(j);
                    break;
                }
            }
        }

        GotoStatement returnIndexes = Expressions.return_(
                null, Expressions.newArrayInit(int.class, 1, constants));
        projectedIndexesBody.add(returnIndexes);

        MethodDeclaration m = Expressions.methodDecl(
                Modifier.PUBLIC,
                int[].class,
                "getProjectedIndexes",
                NO_PARAMS,
                Blocks.toFunctionBlock(returnIndexes));

        final Expression inputEnumerable =
                builder.append(
                        "inputEnumerable", result.block, false);

        ParameterExpression test = new ParameterExpression(
                0, BuiltInMethod.ENUMERABLE_ENUMERATOR.getDeclaringClass(), result.variableName);
        Expression arrowProjectEnumerator = Expressions.new_(
                ArrowProjectEnumerator.class,
                Arrays.asList(test),
                Expressions.list(m));

        String variableName = "e" + arrowImplementor.getAndIncrementSuffix();
        builder.append(variableName, arrowProjectEnumerator);

        return arrowImplementor.result(variableName, physType, builder.toBlock());
    }
}