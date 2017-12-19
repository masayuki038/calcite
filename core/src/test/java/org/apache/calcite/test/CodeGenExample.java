package org.apache.calcite.test;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.test.SqlToRelTestBase;
import org.junit.Test;

/**
 * Created by masayuki on 2017/12/20.
 */
public class CodeGenExample {
    @Test
    public void testCodeGen() {
        String sql = "select * from emp a INNER JOIN dept b ON CAST(a.empno AS int) <> b.deptno|";
        SingleRel relNode = (SingleRel)toRel(sql);

        final RexBuilder rexBuilder = relNode.getCluster().getRexBuilder();
        final RelMetadataQuery mq = RelMetadataQuery.instance();
        final RelOptPredicateList predicates = mq.getPulledUpPredicates((EnumerableRel)relNode.getInput());
        final RexSimplify simplify =
                new RexSimplify(rexBuilder, predicates, false, RexUtil.EXECUTOR);
        final RexProgram org = ((Calc)relNode).getProgram();
        final RexProgram program = org.normalize(rexBuilder, simplify);
    }

    private static RelNode toRel(String sql) {
        final SqlToRelTestBase test = new SqlToRelTestBase() {
        };
        return test.createTester().convertSqlToRel(sql).rel;
    }
}
