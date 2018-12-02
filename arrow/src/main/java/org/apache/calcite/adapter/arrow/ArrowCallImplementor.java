package org.apache.calcite.adapter.arrow;

import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rex.RexCall;

interface ArrowCallImplementor {
    Expression implement(
            ArrowRexToLixTranslator translator,
            RexCall call,
            RexImpTable.NullAs nullAs);
}
