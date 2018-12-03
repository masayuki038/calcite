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

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;

import static org.apache.calcite.adapter.arrow.EnumerableUtils.NO_PARAMS;

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
  public Result implement(ArrowImplementor arrowImplementor, EnumerableRel.Prefer pref) {
    final JavaTypeFactory typeFactory = arrowImplementor.getTypeFactory();
    final BlockBuilder builder = new BlockBuilder();
    final ArrowRel child = (ArrowRel) this.input;
    final Result result = arrowImplementor.visitChild(0, child);
    final PhysType physType = PhysTypeImpl.of(typeFactory, getRowType(), pref.prefer(result.format));

    List<String> fieldNames = getRowType().getFieldNames();
    BlockBuilder projectedIndexesBody = new BlockBuilder();

    ConstantExpression[] constants = new ConstantExpression[fieldNames.size()];
    for (int i = 0; i < fieldNames.size(); i++) {
      String fieldName = fieldNames.get(i);
      for (int j = 0; j < this.inputFieldNames.size(); j++) {
        if (fieldName.equals(this.inputFieldNames.get(j))) {
          constants[i] = Expressions.constant(j);
          break;
        }
      }
    }

    GotoStatement returnIndexes = Expressions.return_(
      null, Expressions.newArrayInit(int.class, 1, constants));
    projectedIndexesBody.add(returnIndexes);

    MethodDeclaration getProjectedIndexes = Expressions.methodDecl(
      Modifier.PUBLIC,
      int[].class,
      "getProjectedIndexes",
      NO_PARAMS,
      Blocks.toFunctionBlock(returnIndexes));

    final Expression inputEnumerable =
      builder.append(
        "inputEnumerable", result.block, false);

    ParameterExpression previous = new ParameterExpression(0, ArrowProcessor.class, result.variableName);
    Expression arrowProjectEnumerator = Expressions.new_(
      ArrowProjectProcessor.class,
      Arrays.asList(Expressions.call(previous, "run", NO_PARAMS)),
      Expressions.list(getProjectedIndexes));

    String variableName = "e" + arrowImplementor.getAndIncrementSuffix();
    builder.append(variableName, arrowProjectEnumerator);

    return arrowImplementor.result(variableName, physType, builder.toBlock());
  }
}