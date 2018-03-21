package org.apache.calcite.adapter.arrow;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.BuiltInMethod;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;

import static org.apache.calcite.adapter.enumerable.EnumUtils.NO_PARAMS;

/**
 * Projection for Apache Arrow
 */
public class ArrowProject extends SingleRel implements ArrowRel {

    private int[] fields;
    private  List<RelDataTypeField> fieldList;

    public ArrowProject(RelOptCluster cluster,
                        RelTraitSet traitSet,
                        RelNode input,
                        List<RelDataTypeField> fieldList,
                        int[] fields) {
        super(cluster, traitSet, input);
        this.fieldList = fieldList;
        this.fields = fields;
    }

    public static ArrowProject create(final RelTraitSet traitSet, final List<RelDataTypeField> fieldList, final RelNode input, int[] fields) {
        final RelOptCluster cluster = input.getCluster();
        return new ArrowProject(cluster, traitSet, input, fieldList, fields);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new ArrowProject(getCluster(), traitSet, sole(inputs), this.fieldList, this.fields);
    }

    @Override
    public RelDataType deriveRowType() {
        RelDataTypeFactory.FieldInfoBuilder builder = getCluster().getTypeFactory().builder();
        Arrays.stream(this.fields).forEach(i -> builder.add(this.fieldList.get(i)));
        return builder.build();
    }

    @Override
    public ArrowRel.Result implement(ArrowImplementor arrowImplementor, EnumerableRel.Prefer pref) {
        final JavaTypeFactory typeFactory = arrowImplementor.getTypeFactory();
        final BlockBuilder builder = new BlockBuilder();
        final ArrowRel child = (ArrowRel)this.input;
        final ArrowRel.Result result = arrowImplementor.visitChild(0, child);
        final PhysType physType = PhysTypeImpl.of(typeFactory, getRowType(), pref.prefer(result.format));

        BlockBuilder projectedIndexesBody = new BlockBuilder();
        ConstantExpression[] constants = new ConstantExpression[this.fields.length];
        for(int i = 0; i < this.fields.length; i ++) {
            constants[i] = Expressions.constant(this.fields[i]);
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

//        ParameterExpression inputEnumerator =
//                Expressions.parameter(
//                        Types.of(
//                                Enumerator.class, inputJavaType),
//                        "inputEnumerator");
//
//        FieldDeclaration f = Expressions.fieldDecl(
//                Modifier.PUBLIC
//                        | Modifier.FINAL,
//                inputEnumerator,
//                Expressions.call(
//                        inputEnumerable,
//                        BuiltInMethod.ENUMERABLE_ENUMERATOR.method));

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