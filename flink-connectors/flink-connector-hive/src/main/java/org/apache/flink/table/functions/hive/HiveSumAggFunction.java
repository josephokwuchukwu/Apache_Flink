/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.functions.hive;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.UNIX_TIMESTAMP;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.call;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.hiveAggDecimalPlus;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.isNull;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.nullOf;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.plus;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.tryCast;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.typeLiteral;
import static org.apache.flink.table.types.logical.DecimalType.MAX_PRECISION;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DECIMAL;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getScale;

/** built-in hive sum aggregate function. */
public class HiveSumAggFunction extends HiveDeclarativeAggregateFunction {

    private final UnresolvedReferenceExpression sum = unresolvedRef("sum");
    private DataType argsType;
    private DataType resultType;

    @Override
    public int operandCount() {
        return 1;
    }

    @Override
    public UnresolvedReferenceExpression[] aggBufferAttributes() {
        return new UnresolvedReferenceExpression[] {sum};
    }

    @Override
    public DataType[] getAggBufferTypes() {
        return new DataType[] {getResultType()};
    }

    @Override
    public DataType getResultType() {
        return resultType;
    }

    @Override
    public Expression[] initialValuesExpressions() {
        return new Expression[] {/* sum = */ nullOf(getResultType())};
    }

    @Override
    public Expression[] accumulateExpressions() {
        Expression operand;
        // TimestampToNumericCastRule can't cast timestamp to numeric directly, so here use
        // UNIX_TIMESTAMP(CAST(timestamp_col AS STRING)) instead
        if (argsType.getLogicalType().is(TIMESTAMP_WITHOUT_TIME_ZONE)) {
            operand = castTimestampToLong(operand(0));
        } else {
            operand = operand(0);
        }
        Expression tryCastOperand = tryCast(operand, typeLiteral(getResultType()));
        return new Expression[] {
            /* sum = */ ifThenElse(
                    isNull(operand(0)),
                    sum,
                    ifThenElse(
                            isNull(tryCastOperand),
                            sum,
                            ifThenElse(
                                    isNull(sum),
                                    tryCastOperand,
                                    adjustedPlus(sum, tryCastOperand))))
        };
    }

    @Override
    public Expression[] retractExpressions() {
        throw new TableException("Sum aggregate function does not support retraction.");
    }

    @Override
    public Expression[] mergeExpressions() {
        return new Expression[] {
            /* sum = */ ifThenElse(
                    isNull(mergeOperand(sum)),
                    sum,
                    ifThenElse(
                            isNull(sum), mergeOperand(sum), adjustedPlus(sum, mergeOperand(sum))))
        };
    }

    @Override
    public Expression getValueExpression() {
        return sum;
    }

    @Override
    public void setArguments(CallContext callContext) {
        if (resultType == null) {
            argsType = callContext.getArgumentDataTypes().get(0);
            resultType = initResultType(argsType);
        }
    }

    private DataType initResultType(DataType argsType) {
        switch (argsType.getLogicalType().getTypeRoot()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return DataTypes.BIGINT();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
                return DataTypes.DOUBLE();
            case DECIMAL:
                int precision =
                        Math.min(MAX_PRECISION, getPrecision(argsType.getLogicalType()) + 10);
                return DataTypes.DECIMAL(precision, getScale(argsType.getLogicalType()));
            default:
                throw new TableException(
                        String.format(
                                "Sum aggregate function does not support type: '%s'. Please re-check the data type.",
                                argsType.getLogicalType().getTypeRoot()));
        }
    }

    private UnresolvedCallExpression adjustedPlus(Expression arg1, Expression arg2) {
        if (getResultType().getLogicalType().is(DECIMAL)) {
            return hiveAggDecimalPlus(arg1, arg2);
        } else {
            return plus(arg1, arg2);
        }
    }

    private UnresolvedCallExpression castTimestampToLong(Expression child) {
        return call(UNIX_TIMESTAMP, tryCast(child, typeLiteral(DataTypes.STRING())));
    }
}
