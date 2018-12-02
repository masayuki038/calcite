package org.apache.calcite.adapter.arrow;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator.InputGetter;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.BuiltInMethod;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.*;

/**
 * Translates {@link org.apache.calcite.rex.RexNode REX expressions} to
 * {@link Expression linq4j expressions}.
 */
public class ArrowRexToLixTranslator {

    protected final JavaTypeFactory typeFactory;
    private final RexBuilder builder;
    private RexProgram program;
    private final Expression root;
    protected final InputGetter inputGetter;
    protected BlockBuilder list;
    private final Map<? extends RexNode, Boolean> exprNullableMap;
    private final ArrowRexToLixTranslator parent;
    private final Function1<String, InputGetter> correlates;

    private static Method findMethod(
            Class<?> clazz, String name, Class... parameterTypes) {
        try {
            return clazz.getMethod(name, parameterTypes);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private ArrowRexToLixTranslator(
            RexProgram program,
            JavaTypeFactory typeFactory,
            Expression root,
            InputGetter inputGetter,
            BlockBuilder list) {
        this(program,
                typeFactory,
                root,
                inputGetter,
                list,
                Collections.emptyMap(),
                new RexBuilder(typeFactory));
    }

    private ArrowRexToLixTranslator(
            RexProgram program,
            JavaTypeFactory typeFactory,
            Expression root,
            InputGetter inputGetter,
            BlockBuilder list,
            Map<? extends RexNode, Boolean> exprNullableMap,
            RexBuilder builder) {
        this(program,
                typeFactory,
                root,
                inputGetter,
                list,
                exprNullableMap,
                builder,
                null);
    }

    private ArrowRexToLixTranslator(
            RexProgram program,
            JavaTypeFactory typeFactory,
            Expression root,
            InputGetter inputGetter,
            BlockBuilder list,
            Map<? extends RexNode, Boolean> exprNullableMap,
            RexBuilder builder,
            ArrowRexToLixTranslator parent) {
        this(program,
                typeFactory,
                root,
                inputGetter,
                list,
                exprNullableMap,
                builder,
                parent,
                null);
    }

    private ArrowRexToLixTranslator(
            RexProgram program,
            JavaTypeFactory typeFactory,
            Expression root,
            InputGetter inputGetter,
            BlockBuilder list,
            Map<? extends RexNode, Boolean> exprNullableMap,
            RexBuilder builder,
            ArrowRexToLixTranslator parent,
            Function1<String, InputGetter> correlates) {
        this.program = program;
        this.typeFactory = typeFactory;
        this.root = root;
        this.inputGetter = inputGetter;
        this.list = list;
        this.exprNullableMap = exprNullableMap;
        this.builder = builder;
        this.parent = parent;
        this.correlates = correlates;
    }

    public static List<Expression> translateProjects(RexProgram program,
                                                     JavaTypeFactory typeFactory,
                                                     BlockBuilder list,
                                                     PhysType outputPhysType,
                                                     Expression root,
                                                     InputGetter inputGetter,
                                                     Function1<String, InputGetter> correlates) {
        List<Type> storageTypes = null;
        if (outputPhysType != null) {
            final RelDataType rowType = outputPhysType.getRowType();
            storageTypes = new ArrayList<>(rowType.getFieldCount());
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                storageTypes.add(outputPhysType.getJavaFieldType(i));
            }
        }
        return new ArrowRexToLixTranslator(program, typeFactory, root, inputGetter, list)
                .setCorrelates(correlates)
                .translateList(program.getProjectList(), storageTypes);
    }

    public ArrowRexToLixTranslator setCorrelates(Function1<String, InputGetter> correlates) {
        if (correlates != null) {
            return this;
        }
        return new ArrowRexToLixTranslator(program, typeFactory, root, inputGetter, list,
                Collections.emptyMap(), builder, this, correlates);
    }

    public List<Expression> translateList(List<? extends RexNode> operandList,
        List<? extends Type> storageTypes) {
        final List<Expression> list = new ArrayList<>(operandList.size());

        for (int i = 0; i < operandList.size(); i++) {
            RexNode rex = operandList.get(i);
            Type desiredType = null;
            if (storageTypes != null) {
                desiredType = storageTypes.get(i);
            }
            final Expression translate = translate(rex, desiredType);
            list.add(translate);

            if (desiredType == null && !isNullable(rex)) {
                assert !Primitive.isBox(translate.getType())
                        : "" + rex + ", " + translate.getType();
            }
        }
        return list;
    }

    public static Expression translateCondition(
            RexProgram program,
            JavaTypeFactory typeFactory,
            BlockBuilder list,
            InputGetter inputGetter,
            Function1<String, InputGetter> correlates) {
        if (program.getCondition() == null) {
            return RexImpTable.TRUE_EXPR;
        }
        final ParameterExpression root = DataContext.ROOT;
        ArrowRexToLixTranslator translator =
                new ArrowRexToLixTranslator(program, typeFactory, root, inputGetter, list);
        translator = translator.setCorrelates(correlates);
        return translator.translate(
                program.getCondition(),
                RexImpTable.NullAs.FALSE);
    }

    public boolean isNullable(RexNode e) {
        if (!e.getType().isNullable()) {
            return false;
        }
        final Boolean b = isKnownNullable(e);
        return b == null || b;
    }

    protected Boolean isKnownNullable(RexNode node) {
        if (!exprNullableMap.isEmpty()) {
            Boolean nullable = exprNullableMap.get(node);
            if (nullable != null) {
                return nullable;
            }
        }
        return parent == null ? null : parent.isKnownNullable(node);
    }

    Expression translate(RexNode expr, RexImpTable.NullAs nullAs) {
        return translate(expr, nullAs, null);
    }

    Expression translate(RexNode expr, Type storageType) {
        final RexImpTable.NullAs nullAs = RexImpTable.NullAs.of(isNullable(expr));
        return translate(expr, nullAs, storageType);
    }

    Expression translate(RexNode expr, RexImpTable.NullAs nullAs, Type storageType) {
        Expression expression = translate0(expr, nullAs, storageType);
        expression = EnumUtils.enforce(storageType, expression);
        assert expression != null;
        return list.append("v", expression);
    }

    private Expression translate0(RexNode expr, RexImpTable.NullAs nullAs, Type storageType) {
        if (nullAs == RexImpTable.NullAs.NULL && !expr.getType().isNullable()) {
            nullAs = RexImpTable.NullAs.NOT_POSSIBLE;
        }

        switch (expr.getKind()) {
            case INPUT_REF:
                final int index = ((RexInputRef) expr).getIndex();
                Expression x = inputGetter.field(list, index, storageType);

                Expression input = list.append("inp" + index + "_", x);
                if (nullAs == RexImpTable.NullAs.NOT_POSSIBLE && input.type.equals(storageType)) {
                    return input;
                }
                return handleNull(input, nullAs);
            case LOCAL_REF:
                return translate(deref((RexLocalRef) expr), nullAs, storageType);
            case DYNAMIC_PARAM:
                return translateParameter((RexDynamicParam) expr, nullAs, storageType);
            default:
                if (expr instanceof RexCall) {
                    return translateCall((RexCall) expr, nullAs);
                }
                throw new RuntimeException("cannot translate expression " + expr);
        }
    }

    private Expression translateParameter(RexDynamicParam expr,
                                          RexImpTable.NullAs nullAs, Type storageType) {
        if (storageType == null) {
            storageType = typeFactory.getJavaClass(expr.getType());
        }
        return nullAs.handle(
                RexToLixTranslator.convert(
                        Expressions.call(root, BuiltInMethod.DATA_CONTEXT_GET.method,
                        Expressions.constant("?" + expr.getIndex())), storageType));
    }

    private Expression translateCall(RexCall call, RexImpTable.NullAs nullAs) {
        final SqlOperator operator = call.getOperator();
        CallImplementor implementor = RexImpTable.INSTANCE.get(operator);
        if (implementor == null) {
            throw new RuntimeException("cannot translate call " + call);
        }
        return getCallImplementor(operator).implement(this, call, nullAs);
    }

    private RexNode deref(RexLocalRef ref) {
        final RexNode e2 = program.getExprList().get(ref.getIndex());
        assert ref.getType().equals(e2.getType());
        return e2;
    }

    private Expression handleNull(Expression input, RexImpTable.NullAs nullAs) {
        final Expression nullHandled = nullAs.handle(input);

        if (nullHandled instanceof ConstantExpression) {
            return nullHandled;
        }

        if (nullHandled == input) {
            return input;
        }

        String unboxVarName = "v_unboxed";
        if (input instanceof ParameterExpression) {
            unboxVarName = ((ParameterExpression) input).name + "_unboxed";
        }
        ParameterExpression unboxed =
                Expressions.parameter(nullHandled.getType(), list.newName(unboxVarName));
        list.add(Expressions.declare(Modifier.FINAL, unboxed, nullHandled));

        return unboxed;
    }

    private ArrowCallImplementor getCallImplementor(SqlOperator operator) {
        return new IsXxxImplementor(true, false);
    }

    static class InputGetterImpl implements InputGetter {
        private PhysType physType;

        public InputGetterImpl(PhysType physType) {
            this.physType = physType;
        }

        public Expression field(BlockBuilder list, int index, Type storageType) {
            List<Expression> paramList = new ArrayList<>();
            paramList.add(Expressions.parameter(int.class, "i"));
            paramList.add(Expressions.constant(index));

            Expression container =  Expressions.parameter(VectorSchemaRootContainer.class, "input");
            Expression call1 = Expressions.call(container, "getFieldVector", paramList);
            final Expression fieldVector = list.append("fieldVector", call1);

            Expression call3 = Expressions.call(fieldVector, "getObject", Arrays.asList(Expressions.parameter(int.class, "j")));
            final Expression value = list.append("value", call3);
            Type fieldType = physType.fieldClass(index);
            return RexToLixTranslator.convert(value, value.getType(), fieldType);
        }
    }

    static private class IsXxxImplementor implements ArrowCallImplementor {

        private Boolean seek;
        private boolean negate;

        IsXxxImplementor(Boolean seek, boolean negate) {
            this.seek = seek;
            this.negate = negate;
        }

        public Expression implement(
                ArrowRexToLixTranslator translator, RexCall call, RexImpTable.NullAs nullAs) {
            List<RexNode> operands = call.getOperands();
            assert operands.size() == 1;
            if (seek == null) {
                return translator.translate(operands.get(0),
                        negate ? RexImpTable.NullAs.FALSE: RexImpTable.NullAs.TRUE);
            }
            return maybeNegate(negate == seek,
                    translator.translate(operands.get(0),
                            seek ? RexImpTable.NullAs.FALSE: RexImpTable.NullAs.TRUE));
        }

        private Expression maybeNegate(boolean negate, Expression expression) {
            if (!negate) {
                return expression;
            }
            return Expressions.not(expression);
        }
    }
}
