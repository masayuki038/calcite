package org.apache.calcite.adapter.arrow;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.calcite.linq4j.Enumerator;

/**
 * Enumerator for Filter
 */
public class ArrowToEnumerableEnumerator implements Enumerator {

  private int rootIndex = 0;
  private int vectorIndex = -1;
  private int selectionVectorIndex = 0;
  private VectorSchemaRootContainer container;

  public ArrowToEnumerableEnumerator(VectorSchemaRootContainer container) {
    this.container = container;
  }

  @Override
  public Object current() {
    int fieldSize = container.getFieldCount(rootIndex);
    Object[] current = new Object[fieldSize];
    for (int i = 0; i < fieldSize; i++) {
      FieldVector vector = container.getFieldVector(rootIndex, i);
      current[i] = vector.getObject(vectorIndex);
    }
    return current;
  }

  @Override
  public boolean moveNext() {
    UInt4Vector selectionVector = container.selectionVector();
    if (selectionVector.getValueCount() > 0) {
      if (selectionVectorIndex >= selectionVector.getValueCount()) {
        return false;
      }

      int index = (int) selectionVector.getObject(selectionVectorIndex++);
      rootIndex = index & 0xffff0000;
      vectorIndex = index & 0x0000ffff;

      return true;
    } else {
      if (vectorIndex >= (container.getRowCount(rootIndex) - 1)) {
        if (rootIndex >= (container.getVectorSchemaRootCount() - 1)) {
          return false;
        }
        rootIndex++;
        vectorIndex = 0;
      } else {
        vectorIndex++;
      }
      return true;
    }
  }

  @Override
  public void reset() {
  }

  @Override
  public void close() {
  }
}
