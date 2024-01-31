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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.table.planner.functions.sql.SqlDefaultOperator;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Binding supports to rewrite the DEFAULT operator. */
public class FlinkSqlCallBinding extends SqlCallBinding {

    private final List<RelDataType> argumentTypes;

    public FlinkSqlCallBinding(
            SqlValidator validator, @Nullable SqlValidatorScope scope, SqlCall call) {
        super(validator, scope, call);
        this.argumentTypes = getArgumentTypes();
    }

    @Override
    public int getOperandCount() {
        return operands().size();
    }

    @Override
    public List<SqlNode> operands() {
        List<SqlNode> operands = super.operands();
        List<SqlNode> rewrittenOperands = new ArrayList<>();
        if (isNamedArgument()) {
            for (int i = 0; i < operands.size(); i++) {
                SqlNode operand = operands.get(i);
                if (operand instanceof SqlCall
                        && ((SqlCall) operand).getOperator() == SqlStdOperatorTable.DEFAULT) {
                    rewrittenOperands.add(
                            new SqlDefaultOperator(argumentTypes.get(i))
                                    .createCall(SqlParserPos.ZERO));
                } else {
                    rewrittenOperands.add(operands.get(i));
                }
            }
            return rewrittenOperands;
        } else {
            return operands;
        }
    }

    @Override
    public RelDataType getOperandType(int ordinal) {
        return isNamedArgument()
                ? ((SqlOperandMetadata) getCall().getOperator().getOperandTypeChecker())
                        .paramTypes(typeFactory)
                        .get(ordinal)
                : super.getOperandType(ordinal);
    }

    public boolean isNamedArgument() {
        return !argumentTypes.isEmpty();
    }

    private List<RelDataType> getArgumentTypes() {
        SqlOperandTypeChecker sqlOperandTypeChecker = getOperator().getOperandTypeChecker();
        if (sqlOperandTypeChecker != null
                && sqlOperandTypeChecker.isFixedParameters()
                && sqlOperandTypeChecker instanceof SqlOperandMetadata) {
            SqlOperandMetadata sqlOperandMetadata =
                    ((SqlOperandMetadata) getOperator().getOperandTypeChecker());
            return sqlOperandMetadata.paramTypes(getTypeFactory());
        } else {
            return Collections.emptyList();
        }
    }
}
