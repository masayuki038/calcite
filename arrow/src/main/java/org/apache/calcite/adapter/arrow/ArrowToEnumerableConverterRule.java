package org.apache.calcite.adapter.arrow;

import com.google.common.base.Predicates;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Rule to convert a relational expression from
 * {@link ArrpwRel#CONVENTION} to {@link EnumerableConvention}.
 */
public class ArrowToEnumerableConverterRule extends ConverterRule {
    public static final ConverterRule INSTANCE =
            new ArrowToEnumerableConverterRule(RelFactories.LOGICAL_BUILDER);

    public ArrowToEnumerableConverterRule(RelBuilderFactory relBuilderFactory) {
        super(RelNode.class, Predicates.<RelNode>alwaysTrue(),
                ArrowRel.CONVENTION, EnumerableConvention.INSTANCE,
                relBuilderFactory, "ArrowToEnumerableConverterRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
        return new ArrowToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
    }
}
