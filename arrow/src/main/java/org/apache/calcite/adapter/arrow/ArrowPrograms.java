package org.apache.calcite.adapter.arrow;

import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;

import java.util.List;

/**
 * To provide programs for Arrow
 */
public class ArrowPrograms {

    /** Returns the standard program used by Prepare. */
    public static Program standard() {
        return standard(DefaultRelMetadataProvider.INSTANCE);
    }

    /** Returns the standard program with user metadata provider. */
    public static Program standard(RelMetadataProvider metadataProvider) {

        final Program program1 =
                new Program() {
                    public RelNode run(RelOptPlanner planner, RelNode rel,
                                       RelTraitSet requiredOutputTraits,
                                       List<RelOptMaterialization> materializations,
                                       List<RelOptLattice> lattices) {
                        planner.setRoot(rel);

                        for (RelOptMaterialization materialization : materializations) {
                            planner.addMaterialization(materialization);
                        }
                        for (RelOptLattice lattice : lattices) {
                            planner.addLattice(lattice);
                        }

                        final RelNode rootRel2 =
                                rel.getTraitSet().equals(requiredOutputTraits)
                                        ? rel
                                        : planner.changeTraits(rel, requiredOutputTraits);
                        assert rootRel2 != null;

                        planner.setRoot(rootRel2);
                        final RelOptPlanner planner2 = planner.chooseDelegate();
                        final RelNode rootRel3 = planner2.findBestExp();
                        assert rootRel3 != null : "could not implement exp";
                        return rootRel3;
                    }
                };

        return Programs.sequence(Programs.subQuery(metadataProvider),
                new Programs.DecorrelateProgram(),
                new Programs.TrimFieldsProgram(),
                program1);
    }
}
