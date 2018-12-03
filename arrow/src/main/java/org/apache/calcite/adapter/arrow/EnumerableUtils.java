package org.apache.calcite.adapter.arrow;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.ParameterExpression;

import java.util.List;

/**
 * Utilities for Enumerable
 */
public class EnumerableUtils {

  static List<ParameterExpression> NO_PARAMS = ImmutableList.of();
  static List<Expression> NO_EXPRS = ImmutableList.of();

  public static int[] identityList(int n) {
    int[] ret = new int[n];
    for (int i = 0; i < n; i++) {
      ret[i] = i;
    }
    return ret;
  }
}
