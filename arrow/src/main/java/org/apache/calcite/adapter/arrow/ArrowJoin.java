package org.apache.calcite.adapter.arrow;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelNodes;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.EquiJoin;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import java.util.Set;

public class ArrowJoin extends EquiJoin implements ArrowRel {
  public ArrowJoin(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode left,
      RelNode right,
      RexNode condition,
      ImmutableIntList leftKeys,
      ImmutableIntList rightKeys,
      Set<CorrelationId> variablesSet,
      JoinRelType joinType) throws InvalidRelException {
    super(
      cluster,
      traits,
      left,
      right,
      condition,
      leftKeys,
      rightKeys,
      variablesSet,
      joinType);
  }

  @Override
  public ArrowJoin copy(RelTraitSet traitSet, RexNode condition, RelNode left,
    RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
    assert joinInfo.isEqui();
    try {
      return new ArrowJoin(getCluster(), traitSet, left, right, condition,
                            joinInfo.leftKeys, joinInfo.rightKeys, variablesSet, joinType);
    } catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public Result implement(ArrowImplementor implementor, EnumerableRel.Prefer pref) {
    BlockBuilder builder = new BlockBuilder();
    final Result leftResult = implementor.visitChild(0, left);
    Expression leftExpression = builder.append("left", leftResult.block);
    final Result rightResult = implementor.visitChild(0, right);
    Expression rightExpression = builder.append("right", rightResult.block);

    final PhysType physType = PhysTypeImpl.of(
      implementor.getTypeFactory(), getRowType(), pref.preferArray());
    final PhysType keyPhysType = leftResult.physType.project(
      leftKeys, JavaRowFormat.LIST);

    String variableName = "e" + implementor.getAndIncrementSuffix();
    BlockBuilder tmp = new BlockBuilder();
    BlockBuilder body = tmp.append(
      Expressions.call(
        leftExpression,
        BuiltInMethod.JOIN.method,
        Expressions.list(
          rightExpression,
          leftResult.physType.generateAccessor(leftKeys),
          rightResult.physType.generateAccessor(rightKeys),
          EnumUtils.joinSelector(joinType,
            physType,
            ImmutableList.of(
              leftResult.physType, rightResult.physType)))
          .append(
            Util.first(keyPhysType.comparer(),
              Expressions.constant(null)))
          .append(
            Expressions.constant(joinType.generatesNullsOnLeft()))
          .append(
            Expressions.constant(
              joinType.generatesNullsOnRight()))));

    builder.append(variableName, body.toBlock());
    return implementor.result(variableName, physType, builder.toBlock());
  }
}
