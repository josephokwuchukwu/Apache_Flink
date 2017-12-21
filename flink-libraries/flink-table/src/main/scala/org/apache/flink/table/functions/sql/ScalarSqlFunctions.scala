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
package org.apache.flink.table.functions.sql

import org.apache.calcite.sql.{SqlFunction, SqlFunctionCategory, SqlKind}
import org.apache.calcite.sql.`type`.{OperandTypes, ReturnTypes, SqlTypeFamily}

/**
  * All built-in scalar SQL functions.
  */
object ScalarSqlFunctions {

  val E = new SqlFunction(
    "E",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.DOUBLE,
    null,
    OperandTypes.NILADIC,
    SqlFunctionCategory.NUMERIC)

  val CONCAT = new SqlFunction(
    "CONCAT",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.VARCHAR_2000,
    null,
    OperandTypes.ONE_OR_MORE,
    SqlFunctionCategory.STRING)

  val CONCAT_WS = new SqlFunction(
    "CONCAT_WS",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.VARCHAR_2000,
    null,
    OperandTypes.ONE_OR_MORE,
    SqlFunctionCategory.STRING)

  val LOG = new SqlFunction(
    "LOG",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.DOUBLE_NULLABLE,
    null,
    OperandTypes.or(OperandTypes.NUMERIC,
      OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)),
    SqlFunctionCategory.NUMERIC)

  val SHIFT_LEFT = new SqlFunction(
    "SHIFT_LEFT",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.ARG0,
    null,
    OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
    SqlFunctionCategory.NUMERIC
  )

  val SHIFT_RIGHT = new SqlFunction(
    "SHIFT_RIGHT",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.ARG0,
    null,
    OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
    SqlFunctionCategory.NUMERIC
  )

}
