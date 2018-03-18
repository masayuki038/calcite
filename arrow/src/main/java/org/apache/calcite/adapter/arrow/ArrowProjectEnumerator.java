package org.apache.calcite.adapter.arrow;

import org.apache.calcite.linq4j.Enumerator;

/**
 * Enumerator for Projection
 */
public abstract class ArrowProjectEnumerator implements Enumerator {
    private Enumerator inputEnumerator;

    public ArrowProjectEnumerator(Enumerator inputEnumerator) {
        this.inputEnumerator = inputEnumerator;
    }

    @Override
    public Object current() {
        Object[] inner = (Object[])this.inputEnumerator.current();
        int[] fields = getProjectedIndexes();
        // 必要なカラムを取り出す
        Object[] ret = new Object[fields.length];
        for(int i = 0; i < fields.length; i++) {
            ret[i] = inner[fields[i]];
        }
        return ret;
    }

    @Override
    public boolean moveNext() { return this.inputEnumerator.moveNext(); }

    @Override
    public void reset() { this.inputEnumerator.reset(); }

    @Override
    public void close() { this.inputEnumerator.close(); }

    abstract public int[] getProjectedIndexes();
}