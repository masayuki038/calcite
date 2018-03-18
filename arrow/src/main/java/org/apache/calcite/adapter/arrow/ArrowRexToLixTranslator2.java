package org.apache.calcite.adapter.arrow;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rex.*;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ControlFlowException;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;

/**
 * ArrowRexToLixTranslator2
 */
public class ArrowRexToLixTranslator2 {
//    final JavaTypeFactory typeFactory;
//    final RexBuilder builder;
//    private final RexProgram program;
//    private final Expression root;
//    private final RexToLixTranslator.InputGetter inputGetter;
//    private final BlockBuilder list;
//    private final Map<? extends RexNode, Boolean> exprNullableMap;
//    private final ArrowRexToLixTranslator2 parent;
//    private final Function1<String, RexToLixTranslator.InputGetter> correlates;
//
//    private ArrowRexToLixTranslator2(
//            RexProgram program,
//            JavaTypeFactory typeFactory,
//            Expression root,
//            RexToLixTranslator.InputGetter inputGetter,
//            BlockBuilder list) {
//        this(program, typeFactory, root, inputGetter, list,
//                Collections.<RexNode, Boolean>emptyMap(),
//                new RexBuilder(typeFactory));
//    }
//
//    private ArrowRexToLixTranslator2(
//            RexProgram program,
//            JavaTypeFactory typeFactory,
//            Expression root,
//            RexToLixTranslator.InputGetter inputGetter,
//            BlockBuilder list,
//            Map<RexNode, Boolean> exprNullableMap,
//            RexBuilder builder) {
//        this(program, typeFactory, root, inputGetter, list, exprNullableMap,
//                builder, null);
//    }
//
//    private ArrowRexToLixTranslator2(
//            RexProgram program,
//            JavaTypeFactory typeFactory,
//            Expression root,
//            RexToLixTranslator.InputGetter inputGetter,
//            BlockBuilder list,
//            Map<? extends RexNode, Boolean> exprNullableMap,
//            RexBuilder builder,
//            ArrowRexToLixTranslator2 parent) {
//        this(program, typeFactory, root, inputGetter, list, exprNullableMap,
//                builder, parent, null);
//    }
//
//    private ArrowRexToLixTranslator2(
//            RexProgram program,
//            JavaTypeFactory typeFactory,
//            Expression root,
//            RexToLixTranslator.InputGetter inputGetter,
//            BlockBuilder list,
//            Map<? extends RexNode, Boolean> exprNullableMap,
//            RexBuilder builder,
//            ArrowRexToLixTranslator2 parent,
//            Function1<String, RexToLixTranslator.InputGetter> correlates) {
//        this.program = program;
//        this.typeFactory = typeFactory;
//        this.root = root;
//        this.inputGetter = inputGetter;
//        this.list = list;
//        this.exprNullableMap = exprNullableMap;
//        this.builder = builder;
//        this.parent = parent;
//        this.correlates = correlates;
//    }
//
//    public static Expression translateCondition(
//            RexProgram program,
//            JavaTypeFactory typeFactory,
//            BlockBuilder list,
//            RexToLixTranslator.InputGetter inputGetter,
//            Function1<String, RexToLixTranslator.InputGetter> correlates) {
//        if (program.getCondition() == null) {
//            return RexImpTable.TRUE_EXPR;
//        }
//        final ParameterExpression root = DataContext.ROOT;
//        ArrowRexToLixTranslator2 translator =
//                new ArrowRexToLixTranslator2(program, typeFactory, root, inputGetter, list);
//        translator = translator.setCorrelates(correlates);
//        return translator.translate(
//                program.getCondition(),
//                RexImpTable.NullAs.FALSE);
//    }
//
//    public ArrowRexToLixTranslator2 setCorrelates(
//            Function1<String, RexToLixTranslator.InputGetter> correlates) {
//        if (this.correlates == correlates) {
//            return this;
//        }
//        return new ArrowRexToLixTranslator2(program, typeFactory, root, inputGetter, list,
//                Collections.<RexNode, Boolean>emptyMap(), builder, this, correlates);
//    }
//
//    Expression translate(RexNode expr, RexImpTable.NullAs nullAs) {
//        return translate(expr, nullAs, null);
//    }
//
//    Expression translate(RexNode expr, RexImpTable.NullAs nullAs,
//                         Type storageType) {
//        Expression expression = translate0(expr, nullAs, storageType);
//        expression = EnumUtils.enforce(storageType, expression);
//        assert expression != null;
//        return list.append("v", expression);
//    }
//
//    private Expression translate0(RexNode expr, RexImpTable.NullAs nullAs,
//                                  Type storageType) {
//        if (nullAs == RexImpTable.NullAs.NULL && !expr.getType().isNullable()) {
//            nullAs = RexImpTable.NullAs.NOT_POSSIBLE;
//        }
//        switch (expr.getKind()) {
//            case INPUT_REF:
//                final int index = ((RexInputRef) expr).getIndex();
//                Expression x = inputGetter.field(list, index, storageType);
//
//                Expression input = list.append("inp" + index + "_", x); // safe to share
//                if (nullAs == RexImpTable.NullAs.NOT_POSSIBLE
//                        && input.type.equals(storageType)) {
//                    // When we asked for not null input that would be stored as box, avoid
//                    // unboxing via nullAs.handle below.
//                    return input;
//                }
//                Expression nullHandled = nullAs.handle(input);
//
//                // If we get ConstantExpression, just return it (i.e. primitive false)
//                if (nullHandled instanceof ConstantExpression) {
//                    return nullHandled;
//                }
//
//                // if nullHandled expression is the same as "input",
//                // then we can just reuse it
//                if (nullHandled == input) {
//                    return input;
//                }
//
//                // If nullHandled is different, then it might be unsafe to compute
//                // early (i.e. unbox of null value should not happen _before_ ternary).
//                // Thus we wrap it into brand-new ParameterExpression,
//                // and we are guaranteed that ParameterExpression will not be shared
//                String unboxVarName = "v_unboxed";
//                if (input instanceof ParameterExpression) {
//                    unboxVarName = ((ParameterExpression) input).name + "_unboxed";
//                }
//                ParameterExpression unboxed = Expressions.parameter(nullHandled.getType(),
//                        list.newName(unboxVarName));
//                list.add(Expressions.declare(Modifier.FINAL, unboxed, nullHandled));
//
//                return unboxed;
//            case LOCAL_REF:
//                return translate(
//                        deref(expr),
//                        nullAs,
//                        storageType);
//            case LITERAL:
//                return translateLiteral(
//                        (RexLiteral) expr,
//                        nullifyType(
//                                expr.getType(),
//                                isNullable(expr)
//                                        && nullAs != RexImpTable.NullAs.NOT_POSSIBLE),
//                        typeFactory,
//                        nullAs);
//            case DYNAMIC_PARAM:
//                return translateParameter((RexDynamicParam) expr, nullAs, storageType);
//            case CORREL_VARIABLE:
//                throw new RuntimeException("Cannot translate " + expr + ". Correlated"
//                        + " variables should always be referenced by field access");
//            case FIELD_ACCESS:
//                RexFieldAccess fieldAccess = (RexFieldAccess) expr;
//                RexNode target = deref(fieldAccess.getReferenceExpr());
//                // only $cor.field access is supported
//                if (!(target instanceof RexCorrelVariable)) {
//                    throw new RuntimeException(
//                            "cannot translate expression " + expr);
//                }
//                if (correlates == null) {
//                    throw new RuntimeException("Cannot translate " + expr + " since "
//                            + "correlate variables resolver is not defined");
//                }
//                RexToLixTranslator.InputGetter getter =
//                        correlates.apply(((RexCorrelVariable) target).getName());
//                return getter.field(list, fieldAccess.getField().getIndex(), storageType);
//            default:
//                if (expr instanceof RexCall) {
//                    return translateCall((RexCall) expr, nullAs);
//                }
//                throw new RuntimeException(
//                        "cannot translate expression " + expr);
//        }
//    }
//
//    public boolean isNullable(RexNode e) {
//        if (!e.getType().isNullable()) {
//            return false;
//        }
//        final Boolean b = isKnownNullable(e);
//        return b == null || b;
//    }
//
//    protected Boolean isKnownNullable(RexNode node) {
//        if (!exprNullableMap.isEmpty()) {
//            Boolean nullable = exprNullableMap.get(node);
//            if (nullable != null) {
//                return nullable;
//            }
//        }
//        return parent == null ? null : parent.isKnownNullable(node);
//    }
//
//    public RexNode deref(RexNode expr) {
//        if (expr instanceof RexLocalRef) {
//            RexLocalRef ref = (RexLocalRef) expr;
//            final RexNode e2 = program.getExprList().get(ref.getIndex());
//            assert ref.getType().equals(e2.getType());
//            return e2;
//        } else {
//            return expr;
//        }
//    }
//
//    private Expression translateParameter(RexDynamicParam expr,
//                                          RexImpTable.NullAs nullAs, Type storageType) {
//        if (storageType == null) {
//            storageType = typeFactory.getJavaClass(expr.getType());
//        }
//        return nullAs.handle(
//                convert(
//                        Expressions.call(root, BuiltInMethod.DATA_CONTEXT_GET.method,
//                                Expressions.constant("?" + expr.getIndex())),
//                        storageType));
//    }
//
//    public static Expression convert(Expression operand, Type toType) {
//        final Type fromType = operand.getType();
//        return convert(operand, fromType, toType);
//    }
//
//    public static Expression convert(Expression operand, Type fromType,
//                                     Type toType) {
//        if (fromType.equals(toType)) {
//            return operand;
//        }
//        // E.g. from "Short" to "int".
//        // Generate "x.intValue()".
//        final Primitive toPrimitive = Primitive.of(toType);
//        final Primitive toBox = Primitive.ofBox(toType);
//        final Primitive fromBox = Primitive.ofBox(fromType);
//        final Primitive fromPrimitive = Primitive.of(fromType);
//        final boolean fromNumber = fromType instanceof Class
//                && Number.class.isAssignableFrom((Class) fromType);
//        if (fromType == String.class) {
//            if (toPrimitive != null) {
//                switch (toPrimitive) {
//                    case CHAR:
//                    case SHORT:
//                    case INT:
//                    case LONG:
//                    case FLOAT:
//                    case DOUBLE:
//                        // Generate "SqlFunctions.toShort(x)".
//                        return Expressions.call(
//                                SqlFunctions.class,
//                                "to" + SqlFunctions.initcap(toPrimitive.primitiveName),
//                                operand);
//                    default:
//                        // Generate "Short.parseShort(x)".
//                        return Expressions.call(
//                                toPrimitive.boxClass,
//                                "parse" + SqlFunctions.initcap(toPrimitive.primitiveName),
//                                operand);
//                }
//            }
//            if (toBox != null) {
//                switch (toBox) {
//                    case CHAR:
//                        // Generate "SqlFunctions.toCharBoxed(x)".
//                        return Expressions.call(
//                                SqlFunctions.class,
//                                "to" + SqlFunctions.initcap(toBox.primitiveName) + "Boxed",
//                                operand);
//                    default:
//                        // Generate "Short.valueOf(x)".
//                        return Expressions.call(
//                                toBox.boxClass,
//                                "valueOf",
//                                operand);
//                }
//            }
//        }
//        if (toPrimitive != null) {
//            if (fromPrimitive != null) {
//                // E.g. from "float" to "double"
//                return Expressions.convert_(
//                        operand, toPrimitive.primitiveClass);
//            }
//            if (fromNumber || fromBox == Primitive.CHAR) {
//                // Generate "x.shortValue()".
//                return Expressions.unbox(operand, toPrimitive);
//            } else {
//                // E.g. from "Object" to "short".
//                // Generate "SqlFunctions.toShort(x)"
//                return Expressions.call(
//                        SqlFunctions.class,
//                        "to" + SqlFunctions.initcap(toPrimitive.primitiveName),
//                        operand);
//            }
//        } else if (fromNumber && toBox != null) {
//            // E.g. from "Short" to "Integer"
//            // Generate "x == null ? null : Integer.valueOf(x.intValue())"
//            return Expressions.condition(
//                    Expressions.equal(operand, RexImpTable.NULL_EXPR),
//                    RexImpTable.NULL_EXPR,
//                    Expressions.box(
//                            Expressions.unbox(operand, toBox),
//                            toBox));
//        } else if (fromPrimitive != null && toBox != null) {
//            // E.g. from "int" to "Long".
//            // Generate Long.valueOf(x)
//            // Eliminate primitive casts like Long.valueOf((long) x)
//            if (operand instanceof UnaryExpression) {
//                UnaryExpression una = (UnaryExpression) operand;
//                if (una.nodeType == ExpressionType.Convert
//                        || Primitive.of(una.getType()) == toBox) {
//                    return Expressions.box(una.expression, toBox);
//                }
//            }
//            return Expressions.box(operand, toBox);
//        } else if (fromType == java.sql.Date.class) {
//            if (toBox == Primitive.INT) {
//                return Expressions.call(BuiltInMethod.DATE_TO_INT.method, operand);
//            } else {
//                return Expressions.convert_(operand, toType);
//            }
//        } else if (toType == java.sql.Date.class) {
//            // E.g. from "int" or "Integer" to "java.sql.Date",
//            // generate "SqlFunctions.internalToDate".
//            if (isA(fromType, Primitive.INT)) {
//                return Expressions.call(BuiltInMethod.INTERNAL_TO_DATE.method, operand);
//            } else {
//                return Expressions.convert_(operand, java.sql.Date.class);
//            }
//        } else if (toType == java.sql.Time.class) {
//            // E.g. from "int" or "Integer" to "java.sql.Time",
//            // generate "SqlFunctions.internalToTime".
//            if (isA(fromType, Primitive.INT)) {
//                return Expressions.call(BuiltInMethod.INTERNAL_TO_TIME.method, operand);
//            } else {
//                return Expressions.convert_(operand, java.sql.Time.class);
//            }
//        } else if (toType == java.sql.Timestamp.class) {
//            // E.g. from "long" or "Long" to "java.sql.Timestamp",
//            // generate "SqlFunctions.internalToTimestamp".
//            if (isA(fromType, Primitive.LONG)) {
//                return Expressions.call(BuiltInMethod.INTERNAL_TO_TIMESTAMP.method,
//                        operand);
//            } else {
//                return Expressions.convert_(operand, java.sql.Timestamp.class);
//            }
//        } else if (toType == BigDecimal.class) {
//            if (fromBox != null) {
//                // E.g. from "Integer" to "BigDecimal".
//                // Generate "x == null ? null : new BigDecimal(x.intValue())"
//                return Expressions.condition(
//                        Expressions.equal(operand, RexImpTable.NULL_EXPR),
//                        RexImpTable.NULL_EXPR,
//                        Expressions.new_(
//                                BigDecimal.class,
//                                Expressions.unbox(operand, fromBox)));
//            }
//            if (fromPrimitive != null) {
//                // E.g. from "int" to "BigDecimal".
//                // Generate "new BigDecimal(x)"
//                return Expressions.new_(
//                        BigDecimal.class, operand);
//            }
//            // E.g. from "Object" to "BigDecimal".
//            // Generate "x == null ? null : SqlFunctions.toBigDecimal(x)"
//            return Expressions.condition(
//                    Expressions.equal(operand, RexImpTable.NULL_EXPR),
//                    RexImpTable.NULL_EXPR,
//                    Expressions.call(
//                            SqlFunctions.class,
//                            "toBigDecimal",
//                            operand));
//        } else if (toType == String.class) {
//            if (fromPrimitive != null) {
//                switch (fromPrimitive) {
//                    case DOUBLE:
//                    case FLOAT:
//                        // E.g. from "double" to "String"
//                        // Generate "SqlFunctions.toString(x)"
//                        return Expressions.call(
//                                SqlFunctions.class,
//                                "toString",
//                                operand);
//                    default:
//                        // E.g. from "int" to "String"
//                        // Generate "Integer.toString(x)"
//                        return Expressions.call(
//                                fromPrimitive.boxClass,
//                                "toString",
//                                operand);
//                }
//            } else if (fromType == BigDecimal.class) {
//                // E.g. from "BigDecimal" to "String"
//                // Generate "x.toString()"
//                return Expressions.condition(
//                        Expressions.equal(operand, RexImpTable.NULL_EXPR),
//                        RexImpTable.NULL_EXPR,
//                        Expressions.call(
//                                SqlFunctions.class,
//                                "toString",
//                                operand));
//            } else {
//                // E.g. from "BigDecimal" to "String"
//                // Generate "x == null ? null : x.toString()"
//                return Expressions.condition(
//                        Expressions.equal(operand, RexImpTable.NULL_EXPR),
//                        RexImpTable.NULL_EXPR,
//                        Expressions.call(
//                                operand,
//                                "toString"));
//            }
//        }
//        return Expressions.convert_(operand, toType);
//    }
//
//    static boolean isA(Type fromType, Primitive primitive) {
//        return Primitive.of(fromType) == primitive
//                || Primitive.ofBox(fromType) == primitive;
//    }
//
//    private Expression translateCall(RexCall call, RexImpTable.NullAs nullAs) {
//        final SqlOperator operator = call.getOperator();
//        CallImplementor implementor =
//                RexImpTable.INSTANCE.get(operator);
//        if (implementor == null) {
//            throw new RuntimeException("cannot translate call " + call);
//        }
//        return implementor.implement(this, call, nullAs);
//    }
//
//    public static Expression translateLiteral(
//            RexLiteral literal,
//            RelDataType type,
//            JavaTypeFactory typeFactory,
//            RexImpTable.NullAs nullAs) {
//        if (literal.isNull()) {
//            switch (nullAs) {
//                case TRUE:
//                case IS_NULL:
//                    return RexImpTable.TRUE_EXPR;
//                case FALSE:
//                case IS_NOT_NULL:
//                    return RexImpTable.FALSE_EXPR;
//                case NOT_POSSIBLE:
//                    throw AlwaysNull.INSTANCE;
//                case NULL:
//                default:
//                    return RexImpTable.NULL_EXPR;
//            }
//        } else {
//            switch (nullAs) {
//                case IS_NOT_NULL:
//                    return RexImpTable.TRUE_EXPR;
//                case IS_NULL:
//                    return RexImpTable.FALSE_EXPR;
//            }
//        }
//        Type javaClass = typeFactory.getJavaClass(type);
//        final Object value2;
//        switch (literal.getType().getSqlTypeName()) {
//            case DECIMAL:
//                final BigDecimal bd = literal.getValueAs(BigDecimal.class);
//                if (javaClass == float.class) {
//                    return Expressions.constant(bd, javaClass);
//                }
//                assert javaClass == BigDecimal.class;
//                return Expressions.new_(BigDecimal.class,
//                        Expressions.constant(bd.toString()));
//            case DATE:
//            case TIME:
//            case TIME_WITH_LOCAL_TIME_ZONE:
//            case INTERVAL_YEAR:
//            case INTERVAL_YEAR_MONTH:
//            case INTERVAL_MONTH:
//                value2 = literal.getValueAs(Integer.class);
//                javaClass = int.class;
//                break;
//            case TIMESTAMP:
//            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
//            case INTERVAL_DAY:
//            case INTERVAL_DAY_HOUR:
//            case INTERVAL_DAY_MINUTE:
//            case INTERVAL_DAY_SECOND:
//            case INTERVAL_HOUR:
//            case INTERVAL_HOUR_MINUTE:
//            case INTERVAL_HOUR_SECOND:
//            case INTERVAL_MINUTE:
//            case INTERVAL_MINUTE_SECOND:
//            case INTERVAL_SECOND:
//                value2 = literal.getValueAs(Long.class);
//                javaClass = long.class;
//                break;
//            case CHAR:
//            case VARCHAR:
//                value2 = literal.getValueAs(String.class);
//                break;
//            case BINARY:
//            case VARBINARY:
//                return Expressions.new_(
//                        ByteString.class,
//                        Expressions.constant(
//                                literal.getValueAs(byte[].class),
//                                byte[].class));
//            case SYMBOL:
//                value2 = literal.getValueAs(Enum.class);
//                javaClass = value2.getClass();
//                break;
//            default:
//                final Primitive primitive = Primitive.ofBoxOr(javaClass);
//                final Comparable value = literal.getValueAs(Comparable.class);
//                if (primitive != null && value instanceof Number) {
//                    value2 = primitive.number((Number) value);
//                } else {
//                    value2 = value;
//                }
//        }
//        return Expressions.constant(value2, javaClass);
//    }
//
//    public RelDataType nullifyType(RelDataType type, boolean nullable) {
//        if (!nullable) {
//            final Primitive primitive = javaPrimitive(type);
//            if (primitive != null) {
//                return typeFactory.createJavaType(primitive.primitiveClass);
//            }
//        }
//        return typeFactory.createTypeWithNullability(type, nullable);
//    }
//
//    private Primitive javaPrimitive(RelDataType type) {
//        if (type instanceof RelDataTypeFactoryImpl.JavaType) {
//            return Primitive.ofBox(
//                    ((RelDataTypeFactoryImpl.JavaType) type).getJavaClass());
//        }
//        return null;
//    }
//
//    static class AlwaysNull extends ControlFlowException {
//        @SuppressWarnings("ThrowableInstanceNeverThrown")
//        public static final AlwaysNull INSTANCE = new AlwaysNull();
//
//        private AlwaysNull() {}
//    }
}
