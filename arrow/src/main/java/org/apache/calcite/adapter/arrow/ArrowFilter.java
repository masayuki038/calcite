package org.apache.calcite.adapter.arrow;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import static org.apache.calcite.adapter.enumerable.EnumUtils.BRIDGE_METHODS;
import static org.apache.calcite.adapter.enumerable.EnumUtils.NO_EXPRS;
import static org.apache.calcite.adapter.enumerable.EnumUtils.NO_PARAMS;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Filter for Apache Arrow
 */
public class ArrowFilter extends Calc implements ArrowRel {

    private int[] fields;

    public ArrowFilter(RelOptCluster cluster,
                       RelTraitSet traitSet,
                       RelNode input,
                       RexProgram program) {
        super(cluster, traitSet, input, program);
        assert !program.containsAggs();
    }

    public static ArrowFilter create(final RelTraitSet traitSet, final RelNode input, final RexProgram program) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        return new ArrowFilter(cluster, traitSet, input, program);
    }

    @Override
    public Calc copy(RelTraitSet traitSet, RelNode child, RexProgram program) {
        return new ArrowFilter(getCluster(), traitSet, child, program);
    }

    @Override
    public EnumerableRel.Result implement(Implementor implementor, EnumerableRel.Prefer pref) {
        final JavaTypeFactory typeFactory = implementor.getTypeFactory();
        final BlockBuilder builder = new BlockBuilder();
        final ArrowRel child = (ArrowRel)getInput();

        final EnumerableRel.Result result = implementor.visitChild(0, child);

        final PhysType physType = PhysTypeImpl.of(typeFactory, getRowType(), pref.prefer(result.format));

        Type outputJavaType = physType.getJavaRowType();

        final Type enumeratorType =
                Types.of(
                        ArrowFilterEnumerator.class, outputJavaType);
        Type inputJavaType = result.physType.getJavaRowType();

        final Expression inputEnumerable = builder.append(
                "inputEnumerable", result.block, false);

        Expression inputEnumerator =
                Expressions.call(inputEnumerable, "enumerator", NO_PARAMS);
//                Expressions.parameter(
//                        Types.of(
//                                Enumerator.class, inputJavaType),
//                        "inputEnumerator");
        Expression input =
                RexToLixTranslator.convert(
                        Expressions.call(
                                inputEnumerator,
                                BuiltInMethod.ENUMERATOR_CURRENT.method),
                        inputJavaType);

        final RexBuilder rexBuilder = getCluster().getRexBuilder();
        final RelMetadataQuery mq = RelMetadataQuery.instance();
        final RelOptPredicateList predicates = mq.getPulledUpPredicates(child);
        final RexSimplify simplify =
                new RexSimplify(rexBuilder, predicates, false, RexUtil.EXECUTOR);
        final RexProgram program = this.program.normalize(rexBuilder, simplify);

        Method filterMethod = Types.lookupMethod(ArrowFilterEnumerator.class, "filter", VectorSchemaRootContainer.class, int.class);
        ParameterExpression container = Expressions.parameter(0, VectorSchemaRootContainer.class, "container");
        ParameterExpression i = Expressions.parameter(0, int.class, "i");

        // TODO
        BlockBuilder filterBody = new BlockBuilder();
        filterBody.add(
                Expressions.return_(null, Expressions.newArrayInit(int.class, 1, Expressions.constant(1))));
//        ArrowRexToLixTranslator.translateCondition(
//                program,
//                typeFactory,
//                filterBody,
//                new RexToLixTranslator.InputGetterImpl(
//                        Collections.singletonList(
//                                Pair.of(input, result.physType))),
//                implementor.allCorrelateVariables);

        final Expression body =
                Expressions.new_(
                        enumeratorType,
                        Arrays.asList(inputEnumerator),
                        Expressions.list(
                                EnumUtils.overridingMethodDecl(
                                        filterMethod,
                                        Arrays.asList(container, i),
                                        filterBody.toBlock())
                        )
                );

//        BlockStatement moveNextBody;
//        if (program.getCondition() == null) {
//            moveNextBody =
//                    Blocks.toFunctionBlock(
//                            Expressions.call(
//                                    inputEnumerator,
//                                    BuiltInMethod.ENUMERATOR_MOVE_NEXT.method));
//        } else {
//            final BlockBuilder builder2 = new BlockBuilder();
//            Expression condition =
//                    RexToLixTranslator.translateCondition(
//                            program,
//                            typeFactory,
//                            builder2,
//                            new RexToLixTranslator.InputGetterImpl(
//                                    Collections.singletonList(
//                                            Pair.of(input, result.physType))),
//                            implementor.allCorrelateVariables);
//            builder2.add(
//                    Expressions.ifThen(
//                            condition,
//                            Expressions.return_(
//                                    null, Expressions.constant(true))));
//            moveNextBody =
//                    Expressions.block(
//                            Expressions.while_(
//                                    Expressions.call(
//                                            inputEnumerator,
//                                            BuiltInMethod.ENUMERATOR_MOVE_NEXT.method),
//                                    builder2.toBlock()),
//                            Expressions.return_(
//                                    null,
//                                    Expressions.constant(false)));
//        }
//
//        final BlockBuilder builder3 = new BlockBuilder();
//        List<Expression> expressions =
//                RexToLixTranslator.translateProjects(
//                        program,
//                        typeFactory,
//                        builder3,
//                        physType,
//                        DataContext.ROOT,
//                        new RexToLixTranslator.InputGetterImpl(
//                                Collections.singletonList(
//                                        Pair.of(input, result.physType))),
//                        implementor.allCorrelateVariables);
//        builder3.add(
//                Expressions.return_(
//                        null, physType.record(expressions)));
//        BlockStatement currentBody =
//                builder3.toBlock();
//
//        final Expression inputEnumerable =
//                builder.append(
//                        "inputEnumerable", result.block, false);
//        final Expression body =
//                Expressions.new_(
//                        enumeratorType,
//                        NO_EXPRS,
//                        Expressions.list(
//                                Expressions.fieldDecl(
//                                        Modifier.PUBLIC
//                                                | Modifier.FINAL,
//                                        inputEnumerator,
//                                        Expressions.call(
//                                                inputEnumerable,
//                                                BuiltInMethod.ENUMERABLE_ENUMERATOR.method)),
//                                EnumUtils.overridingMethodDecl(
//                                        BuiltInMethod.ENUMERATOR_RESET.method,
//                                        NO_PARAMS,
//                                        Blocks.toFunctionBlock(
//                                                Expressions.call(
//                                                        inputEnumerator,
//                                                        BuiltInMethod.ENUMERATOR_RESET.method))),
//                                EnumUtils.overridingMethodDecl(
//                                        BuiltInMethod.ENUMERATOR_MOVE_NEXT.method,
//                                        NO_PARAMS,
//                                        moveNextBody),
//                                EnumUtils.overridingMethodDecl(
//                                        BuiltInMethod.ENUMERATOR_CLOSE.method,
//                                        NO_PARAMS,
//                                        Blocks.toFunctionBlock(
//                                                Expressions.call(
//                                                        inputEnumerator,
//                                                        BuiltInMethod.ENUMERATOR_CLOSE.method))),
//                                Expressions.methodDecl(
//                                        Modifier.PUBLIC,
//                                        BRIDGE_METHODS
//                                                ? Object.class
//                                                : outputJavaType,
//                                        "current",
//                                        NO_PARAMS,
//                                        currentBody)));
        builder.append("filter", body);
        return implementor.result(physType, builder.toBlock());
    }
}
