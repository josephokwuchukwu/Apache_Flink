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
package org.apache.flink.table.planner.codegen.calls

import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, CodeGenUtils, GeneratedExpression}
import org.apache.flink.table.planner.codegen.GenerateUtils.generateCallIfArgsNotNull
import org.apache.flink.table.types.logical.LogicalType

import org.apache.calcite.rex.RexCall

/** [[CallGenerator]] for `HASHCODE`. */
class HashCodeNewCallGen(call: RexCall) extends CallGenerator {
  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    val inputTerm = operands.head.resultTerm
    val inputType = operands.head.resultType
    val code = CodeGenUtils.hashCodeForType(ctx, inputType, inputTerm)
    generateCallIfArgsNotNull(ctx, returnType, operands)(_ => code)
  }
}
