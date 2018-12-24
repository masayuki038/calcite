package org.apache.calcite.adapter.arrow;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.adapter.enumerable.impl.AggAddContextImpl;
import org.apache.calcite.adapter.enumerable.impl.AggResultContextImpl;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.*;

import static org.apache.calcite.adapter.arrow.EnumerableUtils.NO_PARAMS;

public class ArrowAggregate extends Aggregate implements ArrowRel {

  private static final ImmutableMap<Type, Type> VECTOR_TYPE_MAP = ImmutableMap.of(
    int.class, org.apache.arrow.vector.IntVector.class,
    Integer.class, org.apache.arrow.vector.IntVector.class,
    long.class, org.apache.arrow.vector.BigIntVector.class,
    Long.class, org.apache.arrow.vector.BigIntVector.class,
    String.class, org.apache.arrow.vector.VarCharVector.class
  );

  public ArrowAggregate(
                         RelOptCluster cluster,
                         RelTraitSet traits,
                         RelNode child,
                         boolean indicator,
                         ImmutableBitSet groupSet,
                         List<ImmutableBitSet> groupSets,
                         List<AggregateCall> aggCalls) throws InvalidRelException {
    super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
    Preconditions.checkArgument(!indicator,
      "ArrowAggregate no longer supports indicator fields");

    for (AggregateCall aggCall: aggCalls) {
      if (aggCall.isDistinct()) {
        throw new InvalidRelException("distinct aggregation not supported");
      }
      AggImplementor implementor2 = RexImpTable.INSTANCE.get(aggCall.getAggregation(), false);
      if (implementor2 == null) {
        throw new InvalidRelException("aggregation " + aggCall.getAggregation() + " not supported");
      }
    }
  }
  @Override
  public ArrowAggregate copy(
                                RelTraitSet traitSet,
                                RelNode input,
                                boolean indicator,
                                ImmutableBitSet groupSet,
                                List<ImmutableBitSet> groupSets,
                                List<AggregateCall> aggCalls) {
    try {
      return new ArrowAggregate(getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls);
    } catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  public Result implement(ArrowImplementor implementor, EnumerableRel.Prefer pref) {
    final JavaTypeFactory typeFactory = implementor.getTypeFactory();
    final BlockBuilder builder = new BlockBuilder();
    final BlockBuilder aggregateBuilder = new BlockBuilder();
    final ArrowRel child = (ArrowRel) getInput();
    final Result result = implementor.visitChild(0, child);
    Expression childExp = builder.append("inputEnumerator", result.block, false);

    final PhysType physType = PhysType.of(typeFactory, getRowType(), pref.preferCustom());
    final PhysType inputPhysType = result.physType;

    ParameterExpression parameter = Expressions.parameter(inputPhysType.getJavaRowType(), "a0");

    final PhysType keyPhysType =
      inputPhysType.project(groupSet.asList(), true, JavaRowFormat.LIST);
    final int groupCount = getGroupCount();

    final List<AggImpState> aggs = new ArrayList<>(aggCalls.size());
    for (Ord<AggregateCall> call: Ord.zip(aggCalls)) {
      aggs.add(new AggImpState(call.i, call.e, false));
    }

    final List<Expression> initExpressions = new ArrayList<>();
    final BlockBuilder initBlock = new BlockBuilder();

    final List<Type>aggStateTypes = new ArrayList<>();
    for (AggImpState agg: aggs) {
      agg.context = new AggContextImpl(agg, typeFactory);
      final List<Type> state = agg.implementor.getStateType(agg.context);

      if (state.isEmpty()) {
        agg.state = ImmutableList.of();
        continue;
      }

      aggStateTypes.addAll(state);

      final List<Expression> decls = new ArrayList(state.size());
      for (int i = 0; i < state.size(); i++) {
        String aggName = "a" + agg.aggIdx;
        if (CalcitePrepareImpl.DEBUG) {
          aggName = Util.toJavaId(agg.call.getAggregation().getName(), 0).substring("ID$0$".length()) + aggName;
        }
        Type type = state.get(i);
        ParameterExpression pe =
          Expressions.parameter(type,
            initBlock.newName(aggName + "s" + i));
        initBlock.add(Expressions.declare(0, pe, null));
        decls.add(pe);
      }
      agg.state = decls;
      initExpressions.addAll(decls);
      agg.implementor.implementReset(agg.context,
        new AggResultContextImpl(initBlock, agg.call, decls, null, null));
    }

    final PhysType accPhysType =
      PhysType.of(typeFactory,
        typeFactory.createSyntheticType(aggStateTypes));


    if (accPhysType.getJavaRowType() instanceof JavaTypeFactoryImpl.SyntheticRecordType) {
      // We have to initialize the SyntheticRecordType instance this way, to avoid using
      // class constructor with too many parameters.
      JavaTypeFactoryImpl.SyntheticRecordType synType =
        (JavaTypeFactoryImpl.SyntheticRecordType)
          accPhysType.getJavaRowType();
      final ParameterExpression record0_ =
        Expressions.parameter(accPhysType.getJavaRowType(), "record0");
      initBlock.add(Expressions.declare(0, record0_, null));
      initBlock.add(
        Expressions.statement(
          Expressions.assign(record0_,
            Expressions.new_(accPhysType.getJavaRowType()))));
      List<Types.RecordField> fieldList = synType.getRecordFields();
      for (int i = 0; i < initExpressions.size(); i++) {
        Expression right = initExpressions.get(i);
        initBlock.add(
          Expressions.statement(
            Expressions.assign(
              Expressions.field(record0_, fieldList.get(i)),
              right)));
      }
      initBlock.add(record0_);
    } else {
      initBlock.add(accPhysType.record(initExpressions));
    }

    final Expression accumulatorInitializer =
      aggregateBuilder.append(
        "accumulatorInitializer",
        Expressions.lambda(
          Function0.class,
          initBlock.toBlock()));

    // Function2<Object[], Employee, Object[]> accumulatorAdder =
    //     new Function2<Object[], Employee, Object[]>() {
    //         public Object[] apply(Object[] acc, Employee in) {
    //              acc[0] = ((Integer) acc[0]) + 1;
    //              acc[1] = ((Integer) acc[1]) + in.salary;
    //             return acc;
    //         }
    //     };
    final BlockBuilder builder2 = new BlockBuilder();
    final ParameterExpression acc_ =
      Expressions.parameter(accPhysType.getJavaRowType(), "acc");
    final ParameterExpression input_ =
      Expressions.parameter(VectorSchemaRootContainer.class, "input");
    final ParameterExpression i_ = Expressions.parameter(int.class, "i");
    final ParameterExpression j_ = Expressions.parameter(int.class, "j");

    ParameterExpression obj = Expressions.parameter(Object.class, "acc_");
    builder2.add(
      Expressions.declare(0, "acc",
        RexToLixTranslator.convert(obj, Object.class, accPhysType.getJavaRowType())));

    for (int i = 0, stateOffset = 0; i < aggs.size(); i++) {
      final AggImpState agg = aggs.get(i);

      final int stateSize = agg.state.size();
      final List<Expression> accumulator = new ArrayList<>(stateSize);
      for (int j = 0; j < stateSize; j++) {
        accumulator.add(accPhysType.fieldReference(acc_, j + stateOffset));
      }
      agg.state = accumulator;

      stateOffset += stateSize;

      AggAddContext addContext =
        new AggAddContextImpl(builder2, accumulator) {
          public List<RexNode> rexArguments() {
            List<RelDataTypeField> inputTypes =
              inputPhysType.getRowType().getFieldList();
            List<RexNode> args = new ArrayList<>();
            for (int index : agg.call.getArgList()) {
              args.add(RexInputRef.of(index, inputTypes));
            }
            return args;
          }

          public RexNode rexFilterArgument() {
            return agg.call.filterArg < 0
                     ? null
                     : RexInputRef.of(agg.call.filterArg,
              inputPhysType.getRowType());
          }

          public RexToLixTranslator rowTranslator() {
            return RexToLixTranslator.forAggregation(
              typeFactory,
              currentBlock(),
              new ArrowInputGetterImpl(inputPhysType)
            ).setNullable(currentNullables());
          }
        };

      agg.implementor.implementAdd(agg.context, addContext);
    }

    builder2.add(acc_);

    List<MemberDeclaration> memberDeclarations = new ArrayList<>();
    memberDeclarations.add(
      new MethodDeclaration(Modifier.PUBLIC, "add", accPhysType.getJavaRowType(),
                             Lists.newArrayList(obj, input_, i_, j_),
                             builder2.toBlock()));

    final Expression accumulatorAdder =
      Expressions.new_(
        ArrowAggregateAccumulatorAdder.class,
        Lists.newArrayList(),
        memberDeclarations);

    // Function2<Integer, Object[], Object[]> resultSelector =
    //     new Function2<Integer, Object[], Object[]>() {
    //         public Object[] apply(Integer key, Object[] acc) {
    //             return new Object[] { key, acc[0], acc[1] };
    //         }
    //     };
//    final BlockBuilder resultBlock = new BlockBuilder();
//    final List<Expression> results = Expressions.list();
//    final ParameterExpression key_;
//    if (groupCount == 0) {
//      key_ = null;
//    } else {
//      final Type keyType = keyPhysType.getJavaRowType();
//      key_ = Expressions.parameter(keyType, "key");
//      for (int j = 0; j < groupCount; j++) {
//        final Expression ref = keyPhysType.fieldReference(key_, j);
//        if (getGroupType() == Group.SIMPLE) {
//          results.add(ref);
//        } else {
//          results.add(
//            Expressions.condition(
//              keyPhysType.fieldReference(key_, groupCount + j),
//              Expressions.constant(null),
//              Expressions.box(ref)));
//        }
//      }
//    }
//    for (final AggImpState agg : aggs) {
//      results.add(
//        agg.implementor.implementResult(agg.context,
//          new AggResultContextImpl(resultBlock, agg.call, agg.state, key_,
//                                    keyPhysType)));
//    }
//    resultBlock.add(physType.record(results));
    List<Integer> groupKeys = groupSet.asList();
    memberDeclarations = new ArrayList<MemberDeclaration>();

    // Member Definitions
    final ParameterExpression[] fieldVectors = new ParameterExpression[groupKeys.size() + aggs.size()];
    int fieldVectorIndex = 0;
    for (; fieldVectorIndex < groupKeys.size(); fieldVectorIndex++) {
      Type vectorClazz = VECTOR_TYPE_MAP.get(keyPhysType.fieldClass(fieldVectorIndex));
      assert (vectorClazz != null) : "keyPhysType.fieldClass(int): " + keyPhysType.fieldClass(fieldVectorIndex);
      ParameterExpression member =
        Expressions.parameter(Modifier.PRIVATE, vectorClazz, "v" + fieldVectorIndex);
      memberDeclarations.add(new FieldDeclaration(Modifier.PRIVATE, member, Expressions.constant(null)));
      fieldVectors[fieldVectorIndex] = member;
    }

    for (AggImpState agg : aggs) {
      Type vectorClazz = VECTOR_TYPE_MAP.get(agg.context.returnType());
      assert (vectorClazz != null): "agg.context.returnType: " + agg.context.returnType();
      ParameterExpression member =
        Expressions.parameter(Modifier.PRIVATE, vectorClazz, "v" + fieldVectorIndex);
      memberDeclarations.add(new FieldDeclaration(Modifier.PRIVATE, member, Expressions.constant(null)));
      fieldVectors[fieldVectorIndex] = member;
      fieldVectorIndex++;
    }

    // Init method
    final BlockBuilder resultInitBlock = new BlockBuilder();
    ParameterExpression rootAllocator =
      Expressions.parameter(0, BufferAllocator.class, "rootAllocator");

    List<ParameterExpression> fieldNames = new ArrayList<>();
    for (int i = 0; i < groupKeys.size(); i++) {
      DeclarationStatement fieldName =
        Expressions.declare(0, "groupKey" + i,
          Expressions.call(
            Expressions.variable(ArrowAggregateProcedure.class,
              "org.apache.calcite.adapter.arrow.ArrowAggregateProcedure.this"),
            "getFieldVectorName",
            Expressions.constant(i)));
      resultInitBlock.add(fieldName);
      fieldNames.add(fieldName.parameter);
    }

    for (int i = 0; i < aggs.size(); i++) {
      String fieldName = aggs.get(i).call.getName() + i;
      resultInitBlock.add(
        Expressions.declare(0, fieldName,
          Expressions.constant(fieldName)));
    }

    for (fieldVectorIndex = 0; fieldVectorIndex < groupKeys.size(); fieldVectorIndex++) {
      Type vectorClazz = VECTOR_TYPE_MAP.get(keyPhysType.fieldClass(fieldVectorIndex));
      assert (vectorClazz != null): "keyPhysType.fieldClass(int): " + keyPhysType.fieldClass(fieldVectorIndex);
      resultInitBlock.add(
        Expressions.statement(
          Expressions.assign(
            fieldVectors[fieldVectorIndex],
            Expressions.new_(
              vectorClazz,
              fieldNames.get(fieldVectorIndex),
              rootAllocator))));

      resultInitBlock.add(
        Expressions.statement(
          Expressions.call(fieldVectors[fieldVectorIndex], "allocateNew", Lists.newArrayList())));
    }

    for (AggImpState agg: aggs) {
      Type vectorClazz = VECTOR_TYPE_MAP.get(agg.context.returnType());
      assert (vectorClazz != null): "agg.context.returnType: " + agg.context.returnType();
      resultInitBlock.add(
        Expressions.statement(
          Expressions.assign(
            fieldVectors[fieldVectorIndex],
            Expressions.new_(
              vectorClazz,
              Expressions.constant("a_" + fieldVectorIndex),
              rootAllocator))));

      resultInitBlock.add(
        Expressions.statement(
          Expressions.call(fieldVectors[fieldVectorIndex], "allocateNew", Lists.newArrayList())));

      fieldVectorIndex++;
    }
    memberDeclarations.add(
      new MethodDeclaration(Modifier.PUBLIC, "init", void.class,
        Lists.newArrayList(rootAllocator),
        resultInitBlock.toBlock()));

    // Apply method
    final BlockBuilder resultApplyBlock = new BlockBuilder();
    ParameterExpression a1 = Expressions.parameter(Object.class, "a1");
    ParameterExpression a2 = Expressions.parameter(Object.class, "a2");

    final ParameterExpression key_;
    ParameterExpression idx = Expressions.parameter(0, int.class, "idx");
    if (groupCount == 0) {
      key_ = null;
    } else {
      final Type keyType = keyPhysType.getJavaRowType();

      DeclarationStatement castKey =
        Expressions.declare(0, "key", RexToLixTranslator.convert(a1, keyType));
      key_ = castKey.parameter;
      resultApplyBlock.add(castKey);

      for (fieldVectorIndex = 0; fieldVectorIndex < groupCount; fieldVectorIndex++) {
        Expression groupKey = null;
        if (getGroupType() == Group.SIMPLE) {
          groupKey = keyPhysType.fieldReference(key_, fieldVectorIndex);
        } else {
          groupKey = Expressions.condition(
            keyPhysType.fieldReference(key_, groupCount + fieldVectorIndex),
            Expressions.constant(null),
            Expressions.box(groupKey));
        }
        Expression value;
        if (groupKey.getType() == String.class) {
          value = Expressions.new_(Text.class, groupKey);
        } else {
          value = Expressions.unbox(groupKey);
        }

        resultApplyBlock.add(
          Expressions.statement(
            Expressions.call(fieldVectors[fieldVectorIndex], "set", idx, value)));
      }
    }

    for (final AggImpState agg : aggs) {
      DeclarationStatement castValue =
        Expressions.declare(0, "acc", RexToLixTranslator.convert(a2, acc_.getType()));
      resultApplyBlock.add(castValue);

      Expression aggRet =
        agg.implementor.implementResult(agg.context,
          new AggResultContextImpl(resultApplyBlock, agg.call, agg.state, key_, keyPhysType));
      Expression value;
      if (aggRet.getType() == String.class) {
        value = Expressions.new_(Text.class, aggRet);
      } else {
        value = Expressions.unbox(aggRet);
      }
      resultApplyBlock.add(
        Expressions.statement(
          Expressions.call(fieldVectors[fieldVectorIndex], "set", idx, value)));
      fieldVectorIndex++;
    }

    memberDeclarations.add(
      new MethodDeclaration(Modifier.PUBLIC, "apply", void.class,
      Lists.newArrayList(idx, a1, a2),
      resultApplyBlock.toBlock()));

//    BlockBuilder resultApplyBlock2 = new BlockBuilder();
//
//    resultApplyBlock2.add(
//      Expressions.call(
//        Expressions.variable(ArrowAggregateResultSelector.class, "this"),
//        "apply",
//        Lists.newArrayList(idx,
//          RexToLixTranslator.convert(a1, key_.getType()),
//          RexToLixTranslator.convert(a2, acc_.getType()))));
//
//    memberDeclarations.add(
//      new MethodDeclaration(Modifier.PUBLIC, "apply", void.class,
//        Lists.newArrayList(idx, a1, a2),
//        resultApplyBlock2.toBlock()));

    // Build method
    final BlockBuilder resultBuildBlock = new BlockBuilder();
    ParameterExpression valueCount = Expressions.parameter(int.class, "valueCount");
    for (ParameterExpression fieldVector: fieldVectors) {
      resultBuildBlock.add(
        Expressions.statement(
          Expressions.call(fieldVector, "setValueCount", valueCount)));
    }
    DeclarationStatement decl = Expressions.declare(0, "vectorSchemaRoot", Expressions.new_(
      VectorSchemaRoot.class,
      Expressions.call(Lists.class, "newArrayList", fieldVectors))
    );
    resultBuildBlock.add(decl);
    resultBuildBlock.add(
      Expressions.statement(
        Expressions.call(decl.parameter, "setRowCount", valueCount)));
    resultBuildBlock.add(
      Expressions.return_(null, decl.parameter));

    memberDeclarations.add(
      new MethodDeclaration(Modifier.PUBLIC, "build", VectorSchemaRoot.class,
        Lists.newArrayList(valueCount),
        resultBuildBlock.toBlock()));

    if (getGroupType() != Group.SIMPLE) {
      final List<Expression> list = new ArrayList<>();
      for (ImmutableBitSet set : groupSets) {
        list.add(
          inputPhysType.generateSelector(parameter, groupSet.asList(),
            set.asList(), keyPhysType.getFormat()));
      }
      final Expression keySelectors_ =
        aggregateBuilder.append("keySelectors",
          Expressions.call(BuiltInMethod.ARRAYS_AS_LIST.method,
            list));
      final Expression resultSelector =
        aggregateBuilder.append("resultSelector",
          Expressions.lambda(Function2.class,
            resultApplyBlock.toBlock(),
            key_,
            acc_));
      aggregateBuilder.add(
        Expressions.return_(null,
          Expressions.call(
            BuiltInMethod.GROUP_BY_MULTIPLE.method,
            Expressions.list(childExp,
              keySelectors_,
              accumulatorInitializer,
              accumulatorAdder,
              resultSelector)
              .appendIfNotNull(keyPhysType.comparer()))));
    } else if (groupCount == 0) {
      final Expression resultSelector =
        aggregateBuilder.append(
          "resultSelector",
          Expressions.lambda(
            Function1.class,
            resultApplyBlock.toBlock(),
            acc_));
      aggregateBuilder.add(
        Expressions.return_(
          null,
          Expressions.call(
            BuiltInMethod.SINGLETON_ENUMERABLE.method,
            Expressions.call(
              childExp,
              BuiltInMethod.AGGREGATE.method,
              Expressions.call(accumulatorInitializer, "apply"),
              accumulatorAdder,
              resultSelector))));
    } else if (aggCalls.isEmpty()
        && groupSet.equals(
        ImmutableBitSet.range(child.getRowType().getFieldCount()))) {
      aggregateBuilder.add(
        Expressions.return_(
          null,
          Expressions.call(
            inputPhysType.convertTo(childExp, physType),
            BuiltInMethod.DISTINCT.method,
            Expressions.<Expression>list()
              .appendIfNotNull(physType.comparer()))));
    } else {
      final Expression keySelector_ =
        aggregateBuilder.append("keySelector",
          generateSelector(
            inputPhysType,
            parameter,
            groupSet.asList(),
            keyPhysType.getFormat()));
//      final Expression resultSelector_ =
//        aggregateBuilder.append("resultSelector",
//          Expressions.lambda(Function2.class,
//            resultApplyBlock.toBlock(),
//            key_,
//            acc_));
      final Expression resultSelector_ =
        Expressions.new_(
          ArrowAggregateResultSelector.class,
          Lists.newArrayList(),
          memberDeclarations);
      aggregateBuilder.add(
        Expressions.return_(null,
          Expressions.call(childExp,
            BuiltInMethod.GROUP_BY2.method,
            Expressions.list(keySelector_,
              accumulatorInitializer,
              accumulatorAdder,
              resultSelector_)
              .appendIfNotNull(keyPhysType.comparer()))));
    }

    final Expression body =
      Expressions.new_(
        Types.of(ArrowAggregateProcedure.class, physType.getJavaRowType()),
        Arrays.asList(Expressions.call(childExp, "execute", NO_PARAMS)),
        Expressions.list(
          EnumUtils.overridingMethodDecl(
            Types.lookupMethod(ArrowAggregateProcedure.class, "execute"),
            NO_PARAMS,
            aggregateBuilder.toBlock())));

    String variableName = "e" + implementor.getAndIncrementSuffix();
    builder.append(variableName, body);
    return implementor.result(variableName, physType, builder.toBlock());
  }

  // TODO Move this method to PhysTypeImpl
  public Expression generateSelector(
    PhysType physType,
    ParameterExpression parameter,
    List<Integer> fields,
    JavaRowFormat targetFormat) {
    // Optimize target format
    switch (fields.size()) {
      case 0:
        targetFormat = JavaRowFormat.LIST;
        break;
      case 1:
        targetFormat = JavaRowFormat.SCALAR;
        break;
    }

    BlockBuilder keySelector = new BlockBuilder();

    RexToLixTranslator.InputGetter inputGetter = new ArrowInputGetterImpl(physType);
    final List<Expression> keys = new ArrayList<>();
    for (int field: fields) {
      keys.add(inputGetter.field(keySelector, field, null));
    }

    keySelector.add(Expressions.call(FlatLists.class, "of", keys));
    List<MemberDeclaration> memberDeclarations = new ArrayList<>();
    memberDeclarations.add(
      new MethodDeclaration(Modifier.PUBLIC, "getKey", java.util.List.class,
                             Lists.newArrayList(
                               Expressions.parameter(VectorSchemaRootContainer.class, "input"),
                               Expressions.parameter(int.class, "i"),
                               Expressions.parameter(int.class, "j")),
                             keySelector.toBlock()));

    return Expressions.new_(
      ArrowAggregateKeySelector.class, Lists.newArrayList(), memberDeclarations);
  }

  public class AggContextImpl implements AggContext {
    private final AggImpState agg;
    private final JavaTypeFactory typeFactory;

    public AggContextImpl(AggImpState agg, JavaTypeFactory typeFactory) {
      this.agg = agg;
      this.typeFactory = typeFactory;
    }

    public SqlAggFunction aggregation() {
      return agg.call.getAggregation();
    }

    public RelDataType returnRelType() {
      return agg.call.type;
    }

    public Type returnType() {
      return javaClass(typeFactory, returnRelType());
    }

    public List<? extends RelDataType> parameterRelTypes() {
      return fieldRowTypes(getInput().getRowType(), null, agg.call.getArgList());
    }

    public List<? extends Type> parameterTypes() {
      return fieldTypes(typeFactory, parameterRelTypes());
    }

    public List<ImmutableBitSet> groupSets() {
      return groupSets;
    }

    public List<Integer> keyOrdinals() {
      return groupSet.asList();
    }

    public List<? extends RelDataType> keyRelTypes() {
      return fieldRowTypes(getInput().getRowType(), null, groupSet.asList());
    }

    public List<? extends Type> keyTypes() {
      return fieldTypes(typeFactory, keyRelTypes());
    }

    private List<RelDataType> fieldRowTypes(
                                             final RelDataType inputRowTypes,
                                             final List<? extends RexNode> extraInputs,
                                             final List<Integer> argList) {
      final List<RelDataTypeField> inputFields = inputRowTypes.getFieldList();
      return new AbstractList<RelDataType>() {
        public RelDataType get(int index) {
          final int arg = argList.get(index);
          return arg < inputFields.size()
                   ? inputFields.get(arg).getType()
                   : extraInputs.get(arg - inputFields.size()).getType();
        }

        public int size() {
          return argList.size();
        }
      };
    }

    private List<Type> fieldTypes(
                                   final JavaTypeFactory typeFactory,
                                   final List<? extends RelDataType> inputTypes) {
      return new AbstractList<Type>() {
        public Type get(int index) {
          return javaClass(typeFactory, inputTypes.get(index));
        }

        public int size() {
          return inputTypes.size();
        }
      };
    }

    private Type javaClass(JavaTypeFactory typeFactory, RelDataType type) {
      final Type clazz = typeFactory.getJavaClass(type);
      return clazz instanceof Class ? clazz : Object[].class;
    }
  }
}
