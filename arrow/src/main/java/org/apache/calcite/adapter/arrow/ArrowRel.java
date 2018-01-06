package org.apache.calcite.adapter.arrow;

import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

/**
 * Relational expression that uses Arrow calling convention.
 */
public interface  ArrowRel extends RelNode {
    EnumerableRel.Result implement(Implementor implementor, EnumerableRel.Prefer pref);
    Convention CONVENTION = new Convention.Impl("ARROW", ArrowRel.class);

    /** Callback for the implementation process that converts a tree of
     * {@link ArrowRel} nodes. */
    class Implementor extends JavaRelImplementor {

        private EnumerableRelImplementor enumerableRelImplementor;
        private EnumerableRel.Prefer pref;

        public Implementor(EnumerableRelImplementor enumerableRelImplementor, EnumerableRel.Prefer pref) {
            super(enumerableRelImplementor.getRexBuilder());
            this.enumerableRelImplementor = enumerableRelImplementor;
            this.pref = pref;
        }

        public EnumerableRel.Result visitChild(int ordinal, RelNode input) {
            assert ordinal == 0;
            return ((ArrowRel) input).implement(this, pref);
        }

        public EnumerableRel.Result result(PhysType physType, BlockStatement block) {
            return this.enumerableRelImplementor.result(physType, block);
        }
    }
}
