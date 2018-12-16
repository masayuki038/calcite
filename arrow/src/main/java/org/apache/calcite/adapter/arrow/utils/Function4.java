package org.apache.calcite.adapter.arrow.utils;

import org.apache.calcite.linq4j.function.Function;

/**
 * Function with four parameters.
 *
 * @param <R> Result type
 * @param <T0> Type of argument #0
 * @param <T1> Type of argument #1
 * @param <T2> Type of argument #2
 * @param <T3> Type of argument #3
 */
public interface Function4<T0, T1, T2, T3, R> extends Function<R> {
  R apply(T0 v0, T1 v1, T2 v2, T3 v3);
}