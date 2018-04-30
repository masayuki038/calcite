package org.apache.calcite.adapter.arrow;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;

/**
 * Project scan rule for Apache Arrow
 */
public class ArrowProjectTableScanRule2 extends RelOptRule {

    public static ArrowProjectTableScanRule2 INSTANCE = new ArrowProjectTableScanRule2(RelFactories.LOGICAL_BUILDER);

    public ArrowProjectTableScanRule2(RelBuilderFactory relBuilderFactory) {
        //super(operand(EnumerableProject.class, operand(ArrowToEnumerableConverter.class, operand(ArrowFilter.class, operand(ArrowTableScan.class, none())))), relBuilderFactory, null);
        //super(operand(EnumerableProject.class, operand(ArrowToEnumerableConverter.class, operand(ArrowFilter.class, any()))));
        //super(operand(LogicalProject.class, operand(LogicalFilter.class, none())));
        super(operand(LogicalProject.class, operand(LogicalFilter.class, none())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalProject project = call.rel(0);

        final RexBuilder rexBuilder = project.getCluster().getRexBuilder();
        final RelDataType inputRowType = project.getRowType();
        final RexProgramBuilder programBuilder = new RexProgramBuilder(inputRowType, rexBuilder);

        programBuilder.addIdentity();
        int[] fields = getProjectFields(project.getProjects());
        if (fields == null) {
            return;
        }
        final RelTraitSet traitSet = project.getTraitSet().replace(ArrowRel.CONVENTION);
        ArrowProject newArrowProject = new ArrowProject(
                project.getCluster(),
                traitSet,
                convert(project.getInput(), ArrowRel.CONVENTION),
                project.getProjects(),
                project.getRowType());
        call.transformTo(newArrowProject);
    }

    private int[] getProjectFields(List<RexNode> exps) {
        final int[] fields = new int[exps.size()];
        for (int i = 0; i < exps.size(); i++) {
            final RexNode exp = exps.get(i);
            if (exp instanceof RexInputRef) {
                fields[i] = ((RexInputRef) exp).getIndex();
            } else {
                return null; // not a simple projection
            }
        }
        return fields;
    }
}
