package org.apache.calcite.adapter.arrow;

import com.google.common.collect.Maps;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

import java.util.Map;

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

        // TODO corrVars is not referenced now. It will be fix when implemented Correlate for Arrow.
        private final Map<String, RexToLixTranslator.InputGetter> corrVars =
                Maps.newHashMap();

        protected final Function1<String, RexToLixTranslator.InputGetter> allCorrelateVariables =
                new Function1<String, RexToLixTranslator.InputGetter>() {
                    public RexToLixTranslator.InputGetter apply(String name) {
                        return getCorrelVariableGetter(name);
                    }
                };

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

        public RexToLixTranslator.InputGetter getCorrelVariableGetter(String name) {
            assert corrVars.containsKey(name) : "Correlation variable " + name
                    + " should be defined";
            return corrVars.get(name);
        }
    }
}
