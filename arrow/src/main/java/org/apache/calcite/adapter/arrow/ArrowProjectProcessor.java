package org.apache.calcite.adapter.arrow;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;

/**
 * Enumerator for Projection
 */
public abstract class ArrowProjectProcessor extends AbstractArrowProcessor {

  public ArrowProjectProcessor(VectorSchemaRootContainer input) {
    super(input);
  }

  @Override
  public VectorSchemaRootContainer run() {
    return new VectorSchemaRootContainerImpl(getVectorSchemaRoots(), input.selectionVector());
  }

  private VectorSchemaRoot[] getVectorSchemaRoots() {
    final int[] projected = getProjectedIndexes();
    int rootSize = input.getVectorSchemaRootCount();
    VectorSchemaRoot[] vectorSchemaRoots = new VectorSchemaRoot[rootSize];

    for (int i = 0; i < input.getVectorSchemaRootCount(); i++) {
      List<FieldVector> fieldVectors = new ArrayList<>();
      List<Field> fields = new ArrayList<>();
      for (int j = 0; j < projected.length; j++) {
        FieldVector fieldVector = input.getFieldVector(i, projected[j]);
        fieldVectors.add(fieldVector);
        fields.add(fieldVector.getField());
      }
      vectorSchemaRoots[i] = new VectorSchemaRoot(fields, fieldVectors, input.getRowCount(i));
    }
    return vectorSchemaRoots;
  }

  abstract public int[] getProjectedIndexes();
}