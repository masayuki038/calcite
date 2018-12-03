package org.apache.calcite.adapter.arrow;

import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Implementation of InputGetter for Arrow
 */
public class ArrowInputGetterImpl implements RexToLixTranslator.InputGetter {

  private PhysType physType;

  public ArrowInputGetterImpl(PhysType physType) {
    this.physType = physType;
  }

  public Expression field(BlockBuilder list, int index, Type storageType) {
    List<Expression> paramList = new ArrayList<>();
    paramList.add(Expressions.parameter(int.class, "i"));
    paramList.add(Expressions.constant(index));

    Expression container = Expressions.parameter(VectorSchemaRootContainer.class, "input");
    Expression call1 = Expressions.call(container, "getFieldVector", paramList);
    final Expression fieldVector = list.append("fieldVector", call1);

    Expression call3 = Expressions.call(
      fieldVector, "getObject", Arrays.asList(Expressions.parameter(int.class, "j")));
    final Expression value = list.append("value", call3);
    Type fieldType = physType.fieldClass(index);
    return RexToLixTranslator.convert(value, value.getType(), fieldType);
  }
}
