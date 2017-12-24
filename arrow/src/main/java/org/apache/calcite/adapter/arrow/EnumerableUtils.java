package org.apache.calcite.adapter.arrow;

/**
 * Utilities for Enumerable
 */
public class EnumerableUtils {

    public static int[] identityList(int n) {
        int[] ret = new int[n];
        for (int i = 0; i < n; i++) {
            ret[i] = i;
        }
        return ret;
    }
}
