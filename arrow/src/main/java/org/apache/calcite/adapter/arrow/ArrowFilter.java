package org.apache.calcite.adapter.arrow;

import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import static org.apache.calcite.adapter.enumerable.EnumUtils.NO_PARAMS;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

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
    public ArrowRel.Result implement(ArrowImplementor arrowImplementor, EnumerableRel.Prefer pref) {
        final JavaTypeFactory typeFactory = arrowImplementor.getTypeFactory();
        final BlockBuilder builder = new BlockBuilder();
        final ArrowRel child = (ArrowRel)getInput();

        final ArrowRel.Result result = arrowImplementor.visitChild(0, child);
        final PhysType physType = PhysTypeImpl.of(typeFactory, getRowType(), pref.prefer(result.format));
        Type outputJavaType = physType.getJavaRowType();

        final Type enumeratorType =
                Types.of(
                        ArrowFilterEnumerator.class, outputJavaType);

        final Expression inputEnumerable = builder.append(
                "inputEnumerable", result.block, false);

        Expression inputEnumerator =
                Expressions.call(inputEnumerable, "enumerator", NO_PARAMS);

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

        Expression list = filterBody.append("list", Expressions.new_(ArrayList.class));
        Expression getRowCountCall = Expressions.call(container, "getRowCount", Arrays.asList(i));
        Expression rowCount = filterBody.append("rowCount", getRowCountCall);
        DeclarationStatement j = Expressions.declare(0, Expressions.parameter(int.class, "j"), Expressions.constant(0));
        ParameterExpression j_ = Expressions.parameter(int.class, "j");
        BinaryExpression test = Expressions.lessThan(j_, rowCount);

        Expression where = ArrowRexToLixTranslator.translateCondition(
                program,
                typeFactory,
                filterBody,
                new ArrowRexToLixTranslator.InputGetterImpl(result.physType),
                arrowImplementor.allCorrelateVariables);

        Expression valueOf = RexToLixTranslator.convert(
                Expressions.call(Integer.class, "valueOf", j_), Integer.class, Object.class);
        Node listAdd = Expressions.block(Expressions.statement(Expressions.call(list, "add", valueOf)));
        ConditionalStatement found = Expressions.ifThen(where, listAdd);

        filterBody.add(Expressions.for_(j, test, Expressions.preIncrementAssign(j_), found));
        filterBody.add(Expressions.return_(null, list));

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
//                            arrowImplementor.allCorrelateVariables);
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
//                        arrowImplementor.allCorrelateVariables);
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
        String variableName = "e" + arrowImplementor.getAndIncrementSuffix();
        builder.append(variableName, body);
        return arrowImplementor.result(variableName, physType, builder.toBlock());
    }
}
