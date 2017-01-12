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
package org.apache.flink.table.api.scala.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

class TableSqlTest extends TableTestBase {

  @Test
  def testSql(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    val sqlTable = table.sql("SELECT a, b, c FROM _ WHERE b > 12")

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(0),
      term("select", "a, b, c"),
      term("where", ">(b, 12)"))

    util.verifyTable(sqlTable, expected)

    val table2 = util.addTable[(String, Boolean, Int)]('d, 'e, 'f)

    val sqlTable2 = table2.sql("SELECT d, e, f FROM _ WHERE e IS TRUE")

    val expected2 = unaryNode(
      "DataStreamCalc",
      streamTableNode(1),
      term("select", "d, e, f"),
      term("where", "IS TRUE(e)"))

    util.verifyTable(sqlTable2, expected2)
  }

}
