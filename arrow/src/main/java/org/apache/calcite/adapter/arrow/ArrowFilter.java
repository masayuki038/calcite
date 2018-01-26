package org.apache.calcite.adapter.arrow;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import static org.apache.calcite.adapter.enumerable.EnumUtils.BRIDGE_METHODS;
import static org.apache.calcite.adapter.enumerable.EnumUtils.NO_EXPRS;
import static org.apache.calcite.adapter.enumerable.EnumUtils.NO_PARAMS;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;

/**
 * Filter for Apache Arrow
 */
public class ArrowFilter extends Calc implements ArrowRel {

    public ArrowFilter(RelOptCluster cluster,
                          RelTraitSet traitSet,
                          RelNode input,
                          RexProgram program) {
        super(cluster, traitSet, input, program);
        assert !program.containsAggs();
    }

    public static ArrowFilter create(final RelNode input, final RexProgram program) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet = cluster.traitSet();
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
        final Type enumeratorType = Types.of(Enumerator.class, outputJavaType);
        Type inputJavaType = result.physType.getJavaRowType();
        ParameterExpression inputEnumerator = Expressions.parameter(
                Types.of(Enumerator.class, inputJavaType),
                "inputEnumerator"
        );
        Expression input = RexToLixTranslator.convert(
                Expressions.call(inputEnumerator, BuiltInMethod.ENUMERATOR_CURRENT.method),
                inputJavaType);
        final RexBuilder rexBuilder = getCluster().getRexBuilder();
        final RelMetadataQuery mq = RelMetadataQuery.instance();
        final RelOptPredicateList predicates = mq.getPulledUpPredicates(child);
        final RexProgram program = this.program.normalize(rexBuilder, null);

        BlockStatement moveNextBody;
        final ParameterExpression i = Expressions.parameter(int.class, "i");

        ParameterExpression indexes = Expressions.parameter(Types.of(int[][].class, inputJavaType), "indexes");
        if (program.getCondition() == null) {
            moveNextBody = Blocks.toFunctionBlock(
                    Expressions.call(inputEnumerator, BuiltInMethod.ENUMERATOR_MOVE_NEXT.method));
        } else {
            final BlockBuilder builder2 = new BlockBuilder();

            BlockBuilder initializeIndexes = new BlockBuilder();
            initializeIndexes.add(Expressions.assign(indexes, Expressions.call(ArrowFilter.class, "filter")));

            Statement loopBody = Expressions.ifThen(
                    Expressions.lessThan(
                            Expressions.field(Expressions.arrayIndex(indexes, i), "length"),
                            Expressions.constant(0)),
                    Expressions.return_(null, Expressions.constant(true)));

            DeclarationStatement rootIndexDeclaration = Expressions.declare(0, "i",  Expressions.constant(0));
            Expression loopCondition = Expressions.lessThan(
                        i,
                        Expressions.field(indexes, "length"));

            ForStatement loop = Expressions.for_(
                    rootIndexDeclaration,
                    loopCondition,
                    Expressions.increment(i),
                    loopBody);
            initializeIndexes.add(loop);
            initializeIndexes.add(Expressions.return_(null, Expressions.constant(false)));

            Statement filterCall = Expressions.ifThen(
                    Expressions.equal(indexes, null),
                    initializeIndexes.toBlock()
            );

            ParameterExpression vectorIndex = Expressions.parameter(int.class, "vectorIndex");
            ParameterExpression rootIndex = Expressions.parameter(int.class, "rootIndex");
            Expression indexesRootIndexLength = Expressions.field(Expressions.arrayIndex(indexes, rootIndex), "length");
            Expression indexLength = Expressions.field(indexes, "length");

            final BlockBuilder builder3 = new BlockBuilder();
            builder3.add(
                 Expressions.ifThen(
                        Expressions.greaterThan(rootIndex, indexLength),
                        Expressions.return_(null, Expressions.constant(false))));
            builder3.add(Expressions.increment(rootIndex));
            builder3.add(Expressions.assign(vectorIndex, Expressions.constant(0)));

            ConditionalStatement nextIndex = Expressions.ifThenElse(
                    Expressions.greaterThan(vectorIndex, indexesRootIndexLength),
                    builder3.toBlock(),
                    Expressions.increment(vectorIndex));

            builder2.add(filterCall);
            builder2.add(nextIndex);
            builder2.add(Expressions.return_(null, Expressions.constant(true)));

            moveNextBody = builder2.toBlock();
        }

        // private int[][] filter()
        ParameterExpression container = Expressions.declare(0, "container",
                Expressions.typeAs(inputEnumerator, VectorSchemaRootContainer.class)).parameter;
        ParameterExpression size = Expressions.declare(0, "size",
                Expressions.call(container, "getVectorSchemaRootCount")).parameter;
        ParameterExpression index = Expressions.declare(0, "index",
                Expressions.newArrayInit(int.class, 2, size)).parameter;

        BlockBuilder filterBody = new BlockBuilder();
        ParameterExpression selected = Expressions.declare(0, "selected",
                Expressions.call(Collections.class, "emptyList")).parameter;
        final ParameterExpression j = Expressions.parameter(int.class, "j");
//        Statement loopBody = Expressions.ifThen(
//                Expressions.lessThan(
//                        j,
//                        Expressions.call(container, "getRowCount", i)),

        Expression condition = RexToLixTranslator.translateCondition(
                program,
                typeFactory,
                filterBody,
                new RexToLixTranslator.InputGetterImpl(
                        Collections.singletonList(Pair.of(input, result.physType))),
                implementor.allCorrelateVariables);
        filterBody.add(Expressions.ifThen(condition, Expressions.call(
              selected, "add", j
        )));

        final BlockBuilder builder4 = new BlockBuilder();
        List<Expression> expressions = RexToLixTranslator.translateProjects(
                program,
                typeFactory,
                builder4,
                physType,
                DataContext.ROOT,
                new RexToLixTranslator.InputGetterImpl(
                        Collections.singletonList(Pair.of(input, result.physType))),
                implementor.allCorrelateVariables);
        builder4.add(
                Expressions.return_(null, physType.record(expressions)));
        BlockStatement currentBody = builder4.toBlock();

        final Expression inputEnumerable = builder.append("inputEnumerable", result.block, false);
        final Expression body = Expressions.new_(enumeratorType, NO_EXPRS, Expressions.list(
                Expressions.fieldDecl(
                        Modifier.PUBLIC | Modifier.FINAL, inputEnumerator,
                        Expressions.call(inputEnumerable, BuiltInMethod.ENUMERABLE_ENUMERATOR.method)),
                Expressions.fieldDecl(
                        Modifier.PRIVATE | Modifier.FINAL,
                        indexes, null),
                EnumUtils.overridingMethodDecl(
                        BuiltInMethod.ENUMERATOR_RESET.method,
                        NO_PARAMS,
                        Blocks.toFunctionBlock(
                                Expressions.call(inputEnumerator, BuiltInMethod.ENUMERATOR_RESET.method))),
                EnumUtils.overridingMethodDecl(
                        BuiltInMethod.ENUMERATOR_MOVE_NEXT.method, NO_PARAMS, moveNextBody),
                EnumUtils.overridingMethodDecl(
                        BuiltInMethod.ENUMERATOR_CLOSE.method,
                        NO_PARAMS,
                        Blocks.toFunctionBlock(
                                Expressions.call(inputEnumerator, BuiltInMethod.ENUMERATOR_CLOSE.method))),
                Expressions.methodDecl(
                        Modifier.PUBLIC,
                        BRIDGE_METHODS ? Object.class : outputJavaType,
                        "current",
                        NO_PARAMS,
                        currentBody)));
        builder.add(
                Expressions.return_(
                        null,
                        Expressions.new_(
                                BuiltInMethod.ABSTRACT_ENUMERABLE_CTOR.constructor,
                                NO_EXPRS,
                                ImmutableList.<MemberDeclaration>of(
                                        Expressions.methodDecl(
                                                Modifier.PUBLIC,
                                                enumeratorType,
                                                BuiltInMethod.ENUMERABLE_ENUMERATOR.method.getName(),
                                                NO_PARAMS,
                                                Blocks.toFunctionBlock(body))))));
        return implementor.result(physType, builder.toBlock());
    }


}
