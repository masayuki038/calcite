package org.apache.calcite.adapter.arrow;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;

import java.util.ArrayList;
import java.util.List;

public class ArrowJoinRule  extends ConverterRule {

  public static ArrowJoinRule INSTANCE = new ArrowJoinRule();

  public ArrowJoinRule() {
    super(LogicalJoin.class, Convention.NONE, ArrowRel.CONVENTION, "ArrowJoinRule");
  }

  public RelNode convert(RelNode rel) {
    LogicalJoin join = (LogicalJoin) rel;
    List<RelNode> newInputs = new ArrayList<>();
    for (RelNode input: join.getInputs()) {
      if (input.getConvention() != ArrowRel.CONVENTION) {
        input = convert(input, input.getTraitSet().replace(ArrowRel.CONVENTION));
      }
      newInputs.add(input);
    }

    final RelOptCluster cluster = join.getCluster();
    final RelTraitSet traitSet = join.getTraitSet().replace(ArrowRel.CONVENTION);
    final RelNode left = newInputs.get(0);
    final RelNode right = newInputs.get(1);
    final JoinInfo info = JoinInfo.of(left, right, join.getCondition());
    if (!info.isEqui() || join.getJoinType() != JoinRelType.INNER) {
      throw new UnsupportedOperationException("Only supported: INNER JOIN and equals condition");
    }

    RelNode newRel;
    try {
      newRel = new ArrowJoin(
                              cluster,
                              join.getTraitSet().replace(ArrowRel.CONVENTION),
                              left,
                              right,
                              info.getEquiCondition(left, right, cluster.getRexBuilder()),
                              info.leftKeys,
                              info.rightKeys,
                              join.getVariablesSet(),
                              join.getJoinType());
      return newRel;
    } catch (InvalidRelException e) {
      throw new IllegalStateException(e);
    }
  }
}
