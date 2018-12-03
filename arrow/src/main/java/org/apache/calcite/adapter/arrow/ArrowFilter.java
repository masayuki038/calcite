package org.apache.calcite.adapter.arrow;

import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Arrays;

import static org.apache.calcite.adapter.arrow.EnumerableUtils.NO_PARAMS;

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
  public Result implement(ArrowImplementor arrowImplementor, EnumerableRel.Prefer pref) {
    final JavaTypeFactory typeFactory = arrowImplementor.getTypeFactory();
    final BlockBuilder builder = new BlockBuilder();
    final ArrowRel child = (ArrowRel) getInput();

    final Result result = arrowImplementor.visitChild(0, child);
    final PhysType physType = PhysTypeImpl.of(typeFactory, getRowType(), pref.prefer(result.format));
    Type outputJavaType = physType.getJavaRowType();

    final Type arrowFilterProcessor =
      Types.of(
        ArrowFilterProcedure.class, outputJavaType);

    final Expression inputEnumerator = builder.append(
      "inputEnumerator", result.block, false);

    final RexBuilder rexBuilder = getCluster().getRexBuilder();
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    final RelOptPredicateList predicates = mq.getPulledUpPredicates(child);
    final RexSimplify simplify =
      new RexSimplify(rexBuilder, predicates, false, RexUtil.EXECUTOR);
    final RexProgram program = this.program.normalize(rexBuilder, simplify);

    Method runMethod = Types.lookupMethod(ArrowFilterProcedure.class, "execute");
    ParameterExpression input = Expressions.parameter(0, VectorSchemaRootContainer.class, "input");

    BlockBuilder filterBody = new BlockBuilder();

    ParameterExpression index = Expressions.parameter(int.class, "index");
    filterBody.add(Expressions.declare(0, index, Expressions.constant(0)));
    Expression selectionVector = filterBody.append(
      "selectionVector", Expressions.call(input, "selectionVector", NO_PARAMS));
    filterBody.add(Expressions.statement(Expressions.call(selectionVector, "clear", NO_PARAMS)));
    filterBody.add(Expressions.statement(Expressions.call(selectionVector, "allocateNew", NO_PARAMS)));

    Expression getVectorSchemaRootCount = Expressions.call(input, "getVectorSchemaRootCount", NO_PARAMS);
    ParameterExpression i = Expressions.parameter(int.class, "i");
    DeclarationStatement declareI = Expressions.declare(0, i, Expressions.constant(0));
    BinaryExpression iLessThan = Expressions.lessThan(i, getVectorSchemaRootCount);

    Expression getRowCountCall = Expressions.call(input, "getRowCount", Arrays.asList(i));
    Expression rowCount = filterBody.append("rowCount", getRowCountCall);
    ParameterExpression j = Expressions.parameter(int.class, "j");
    DeclarationStatement declareJ = Expressions.declare(0, j, Expressions.constant(0));
    BinaryExpression jLessThan = Expressions.lessThan(j, rowCount);

    Expression where = RexToLixTranslator.translateCondition(
      program,
      typeFactory,
      filterBody,
      new ArrowInputGetterImpl(result.physType),
      arrowImplementor.allCorrelateVariables);

    Expression upper = Expressions.leftShift(i, Expressions.constant(16));
    Expression lower = Expressions.and(j, Expressions.constant(65535));
    Expression value = Expressions.or(upper, lower);

    Node selectionVectorSet = Expressions.block(
      Expressions.statement(
        Expressions.call(selectionVector, "set", Expressions.postIncrementAssign(index), value)));
    ConditionalStatement found = Expressions.ifThen(where, selectionVectorSet);

    Statement jFor = Expressions.for_(declareJ, jLessThan, Expressions.preIncrementAssign(j), found);
    filterBody.add(Expressions.for_(declareI, iLessThan, Expressions.preIncrementAssign(i), jFor));
    filterBody.add(Expressions.statement(Expressions.call(selectionVector, "setValueCount", index)));
    filterBody.add(Expressions.return_(null, input));

    final Expression body =
      Expressions.new_(
        arrowFilterProcessor,
        Arrays.asList(Expressions.call(inputEnumerator, "execute", NO_PARAMS)),
        Expressions.list(
          EnumUtils.overridingMethodDecl(
            runMethod,
            NO_PARAMS,
            filterBody.toBlock())));

    String variableName = "e" + arrowImplementor.getAndIncrementSuffix();
    builder.append(variableName, body);
    return arrowImplementor.result(variableName, physType, builder.toBlock());
  }
}
