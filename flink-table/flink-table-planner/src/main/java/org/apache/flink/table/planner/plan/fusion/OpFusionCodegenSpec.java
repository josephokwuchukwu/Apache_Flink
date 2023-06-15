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

package org.apache.flink.table.planner.plan.fusion;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ExprCodeGenerator;
import org.apache.flink.table.planner.codegen.GeneratedExpression;

import java.util.List;
import java.util.Set;

/** An interface for those physical operators that support operator fusion codegen. */
@Internal
public interface OpFusionCodegenSpec {

    /**
     * Initializes the operator spec. Sets access to the context. This method must be called before
     * doProduce and doConsume related methods.
     */
    void setup(OpFusionContext opFusionContext);

    /** Prefix used in the current operator's variable names. */
    String variablePrefix();

    /**
     * The subset of column index those should be evaluated before this operator.
     *
     * <p>We will use this to insert some code to access those columns that are actually used by
     * current operator before calling doProcessConsume().
     */
    Set<Integer> usedInputColumns(int inputId);

    /**
     * Specific inputId of current operator needed {@link RowData} type, this is used to notify the
     * upstream operator wrap the proper {@link RowData} we needed before call doProcessConsume
     * method. For example, HashJoin build side need {@link BinaryRowData}.
     */
    Class<? extends RowData> getInputRowDataClass(int inputId);

    /**
     * Every operator need one {@link CodeGeneratorContext} to store the context needed during
     * operator fusion codegen.
     */
    CodeGeneratorContext getCodeGeneratorContext();

    /** Get the {@link ExprCodeGenerator} used by this operator during operator fusion codegen, . */
    ExprCodeGenerator getExprCodeGenerator();

    /**
     * Generate the Java source code to process rows, only the leaf operator in operator DAG need to
     * generate the code which produce the row, other middle operators just call its input {@link
     * OpFusionCodegenSpecGenerator#processProduce(CodeGeneratorContext)} normally, otherwise, the
     * operator has some specific logic. The leaf operator produce row first, and then call {@link
     * OpFusionContext#processConsume(List)} method to consume row.
     *
     * <p>The code generated by leaf operator will be saved in fusionCtx, so this method doesn't has
     * return type.
     */
    void doProcessProduce(CodeGeneratorContext codegenCtx);

    /**
     * The process method is responsible for the operator data processing logic, so each operator
     * needs to implement this method to generate the code to process the row. This should only be
     * called from {@link OpFusionCodegenSpecGenerator#processConsume(List, String)}.
     *
     * <p>Note: A operator can either consume the rows as RowData (row), or a list of variables
     * (inputVars).
     *
     * @param inputId This is numbered starting from 1, and `1` indicates the first input.
     * @param inputVars field variables of current input.
     * @param row row variable of current input.
     */
    String doProcessConsume(
            int inputId, List<GeneratedExpression> inputVars, GeneratedExpression row);

    /**
     * Generate the Java source code to do operator clean work, only the leaf operator in operator
     * DAG need to generate the code, other middle operators just call its input `endInputProduce`
     * normally, otherwise, the operator has some specific logic.
     *
     * <p>The code generated by leaf operator will be saved in fusionCtx, so this method doesn't has
     * return type.
     */
    void doEndInputProduce(CodeGeneratorContext codegenCtx);

    /**
     * The endInput method is used to do clean work for operator corresponding input, such as the
     * HashAgg operator needs to flush data, and the HashJoin build side need to build hash table,
     * so each operator needs to implement the corresponding clean logic in this method.
     *
     * <p>For blocking operators such as HashAgg, the {@link OpFusionContext#processConsume(List,
     * String)} method needs to be called first to consume the data, followed by the
     * `endInputConsume` method to do the cleanup work of the downstream operators. For pipeline
     * operators such as Project, you only need to call the `endInputConsume` method.
     *
     * @param inputId This is numbered starting from 1, and `1` indicates the first input.
     */
    String doEndInputConsume(int inputId);
}
