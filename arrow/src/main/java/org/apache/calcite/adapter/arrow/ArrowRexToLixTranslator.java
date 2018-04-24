package org.apache.calcite.adapter.arrow;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.rex.*;
import static org.apache.calcite.adapter.enumerable.EnumUtils.NO_PARAMS;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.*;

/**
 * .ArrowRexToLixTranslator
 */
public class ArrowRexToLixTranslator extends RexToLixTranslator {

    private RexProgram program;
    private Expression root;
    private RexBuilder builder;

    private ArrowRexToLixTranslator(RexProgram program, JavaTypeFactory typeFactory, Expression root, InputGetter inputGetter, BlockBuilder list) {
        super(program, typeFactory, root, inputGetter, list);
        this.program = program;
        this.root = root;
        this.builder = new RexBuilder(typeFactory);
    }

    public ArrowRexToLixTranslator(RexProgram program, JavaTypeFactory typeFactory, Expression root, InputGetter inputGetter, BlockBuilder list, Map<RexNode, Boolean> rexNodeBooleanMap, RexBuilder builder, ArrowRexToLixTranslator arrowRexToLixTranslator, Function1<String, InputGetter> correlates) {
        //super(program, typeFactory, root, inputGetter, list, rexNodeBooleanMap, correlates);
        super(program, typeFactory, root, inputGetter, list);
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

    @Override
    public ArrowRexToLixTranslator setCorrelates(
            Function1<String, InputGetter> correlates) {
        if (this.correlates == correlates) {
            return this;
        }
        return new ArrowRexToLixTranslator(program, typeFactory, root, inputGetter, list,
                Collections.<RexNode, Boolean>emptyMap(), builder, this, correlates);
    }

    @Override
    public Expression translate(RexNode expr, RexImpTable.NullAs nullAs) {
        return translate(expr, nullAs, null);
    }

    @Override
    protected Expression translate(RexNode expr, RexImpTable.NullAs nullAs,
                                   Type storageType) {
        Expression expression = translate0(expr, nullAs, storageType);
        expression = EnumUtils.enforce(storageType, expression);
        assert expression != null;
        return list.append("v", expression);
    }

    protected Expression translate0(RexNode expr, RexImpTable.NullAs nullAs,
                                  Type storageType) {
        if (nullAs == RexImpTable.NullAs.NULL && !expr.getType().isNullable()) {
            nullAs = RexImpTable.NullAs.NOT_POSSIBLE;
        }
        switch (expr.getKind()) {
            case INPUT_REF:
                final int index = ((RexInputRef) expr).getIndex();
                Expression x = inputGetter.field(list, index, storageType);

                Expression input = list.append("inp" + index + "_", x); // safe to share
                if (nullAs == RexImpTable.NullAs.NOT_POSSIBLE
                        && input.type.equals(storageType)) {
                    // When we asked for not null input that would be stored as box, avoid
                    // unboxing via nullAs.handle below.
                    return input;
                }
                Expression nullHandled = nullAs.handle(input);

                // If we get ConstantExpression, just return it (i.e. primitive false)
                if (nullHandled instanceof ConstantExpression) {
                    return nullHandled;
                }

                // if nullHandled expression is the same as "input",
                // then we can just reuse it
                if (nullHandled == input) {
                    return input;
                }

                // If nullHandled is different, then it might be unsafe to compute
                // early (i.e. unbox of null value should not happen _before_ ternary).
                // Thus we wrap it into brand-new ParameterExpression,
                // and we are guaranteed that ParameterExpression will not be shared
                String unboxVarName = "v_unboxed";
                if (input instanceof ParameterExpression) {
                    unboxVarName = ((ParameterExpression) input).name + "_unboxed";
                }
                ParameterExpression unboxed = Expressions.parameter(nullHandled.getType(),
                        list.newName(unboxVarName));
                list.add(Expressions.declare(Modifier.FINAL, unboxed, nullHandled));

                return unboxed;
            case LOCAL_REF:
                return translate(
                        deref(expr),
                        nullAs,
                        storageType);
            case LITERAL:
                return translateLiteral(
                        (RexLiteral) expr,
                        nullifyType(
                                expr.getType(),
                                isNullable(expr)
                                        && nullAs != RexImpTable.NullAs.NOT_POSSIBLE),
                        typeFactory,
                        nullAs);
            case DYNAMIC_PARAM:
                return translateParameter((RexDynamicParam) expr, nullAs, storageType);
            case CORREL_VARIABLE:
                throw new RuntimeException("Cannot translate " + expr + ". Correlated"
                        + " variables should always be referenced by field access");
            case FIELD_ACCESS:
                RexFieldAccess fieldAccess = (RexFieldAccess) expr;
                RexNode target = deref(fieldAccess.getReferenceExpr());
                // only $cor.field access is supported
                if (!(target instanceof RexCorrelVariable)) {
                    throw new RuntimeException(
                            "cannot translate expression " + expr);
                }
                if (correlates == null) {
                    throw new RuntimeException("Cannot translate " + expr + " since "
                            + "correlate variables resolver is not defined");
                }
                InputGetter getter =
                        correlates.apply(((RexCorrelVariable) target).getName());
                return getter.field(list, fieldAccess.getField().getIndex(), storageType);
            default:
                if (expr instanceof RexCall) {
                    return translateCall((RexCall) expr, nullAs);
                }
                throw new RuntimeException(
                        "cannot translate expression " + expr);
        }
    }

    public static class InputGetterImpl implements InputGetter {
        private PhysType physType;

        public InputGetterImpl(PhysType physType) {
            this.physType = physType;
        }

        public Expression field(BlockBuilder list, int index, Type storageType) {
            List<Expression> paramList = new ArrayList<>();
            paramList.add(Expressions.parameter(int.class, "i"));
            paramList.add(Expressions.constant(index));

            Expression container =  Expressions.parameter(VectorSchemaRootContainer.class, "container");
            Expression call1 = Expressions.call(container, "getFieldVector", paramList);
            final Expression fieldVector = list.append("fieldVector", call1);

            Expression call2 = Expressions.call(fieldVector, "getAccessor", NO_PARAMS);
            final Expression accessor = list.append("accessor", call2);

            Expression call3 = Expressions.call(accessor, "getObject", Arrays.asList(Expressions.parameter(int.class, "j")));
            final Expression value = list.append("value", call3);
            Type fieldType = physType.fieldClass(index);
            return RexToLixTranslator.convert(value, value.getType(), fieldType);
        }
    }
}
