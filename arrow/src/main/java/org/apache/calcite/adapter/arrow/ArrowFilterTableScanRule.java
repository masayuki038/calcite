package org.apache.calcite.adapter.arrow;

import org.apache.calcite.adapter.enumerable.EnumerableCalc;
import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;

/**
 * Filter scan rule for Apache Arrow
 */
public class ArrowFilterTableScanRule extends RelOptRule {

    public static ArrowFilterTableScanRule INSTANCE = new ArrowFilterTableScanRule();

    public ArrowFilterTableScanRule() {
        super(operand(LogicalFilter.class, operand(ArrowTableScan.class, none())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalFilter filter = call.rel(0);
        final RelNode input = filter.getInput();

        final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        final RelDataType inputRowType = input.getRowType();
        final RexProgramBuilder programBuilder = new RexProgramBuilder(inputRowType, rexBuilder);
        programBuilder.addIdentity();
        programBuilder.addCondition(filter.getCondition());
        final RexProgram program = programBuilder.getProgram();

        final ArrowFilter arrowFilter = ArrowFilter.create(input, program);
        call.transformTo(arrowFilter);
    }
}
