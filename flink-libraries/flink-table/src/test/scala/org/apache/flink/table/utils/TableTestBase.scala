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

package org.apache.flink.table.utils

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelNode
import org.apache.flink.api.java.LocalEnvironment
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.{DataSet => JDataSet}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment, TableSchema}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction}
import org.junit.Assert.assertEquals
import org.junit.{ComparisonFailure, Rule}
import org.junit.rules.ExpectedException
import org.mockito.Mockito.{mock, when}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import _root_.java.lang.{Boolean => JBool}

import util.control.Breaks._

/**
  * Test base for testing Table API / SQL plans.
  */
class TableTestBase {

  // used for accurate exception information checking.
  val expectedException = ExpectedException.none()

  @Rule
  def thrown = expectedException

  def batchTestUtil(): BatchTableTestUtil = {
    BatchTableTestUtil()
  }

  def streamTestUtil(): StreamTableTestUtil = {
    StreamTableTestUtil()
  }

  def verifyTableEquals(expected: Table, actual: Table): Unit = {
    assertEquals(
      "Logical plans do not match",
      LogicalPlanFormatUtils.formatTempTableId(RelOptUtil.toString(expected.getRelNode)),
      LogicalPlanFormatUtils.formatTempTableId(RelOptUtil.toString(actual.getRelNode)))
  }
}

abstract class TableTestUtil {

  protected var counter = 0

  def addTable[T: TypeInformation](fields: Expression*): Table = {
    counter += 1
    addTable[T](s"Table$counter", fields: _*)
  }

  def addTable[T: TypeInformation](name: String, fields: Expression*): Table

  def addFunction[T: TypeInformation](name: String, function: TableFunction[T]): TableFunction[T]

  def addFunction(name: String, function: ScalarFunction): Unit

  def verifySql(query: String, expected: String): Unit

  def verifyTable(resultTable: Table, expected: String): Unit

  def verifySchema(resultTable: Table, fields: Seq[(String, TypeInformation[_])]): Unit = {
    val actual = resultTable.getSchema
    val expected = new TableSchema(fields.map(_._1).toArray, fields.map(_._2).toArray)
    assertEquals(expected, actual)
  }

  // the print methods are for debugging purposes only
  def printTable(resultTable: Table): Unit

  def printSql(query: String): Unit

  protected def verifyString(expected: String, optimized: RelNode) {
    val actual = RelOptUtil.toString(optimized)
    // we remove the charset for testing because it
    // depends on the native machine (Little/Big Endian)
    val actualNoCharset = actual.replace("_UTF-16LE'", "'").replace("_UTF-16BE'", "'")

    val expectedLines = expected.split("\n").map(_.trim)
    val actualLines = actualNoCharset.split("\n").map(_.trim)

    val expectedMessage = expectedLines.mkString("\n")
    val actualMessage = actualLines.mkString("\n")

    breakable {
      for ((expectedLine, actualLine) <- expectedLines.zip(actualLines)) {
        if (expectedLine == TableTestUtil.ANY_NODE) {
        }
        else if (expectedLine == TableTestUtil.ANY_SUBTREE) {
          break
        }
        else if (expectedLine != actualLine) {
          throw new ComparisonFailure(null, expectedMessage, actualMessage)
        }
      }
    }
  }

  def explain(resultTable: Table): String
}

object TableTestUtil {
  val ANY_NODE = "%ANY_NODE%"

  val ANY_SUBTREE = "%ANY_SUBTREE%"

  // this methods are currently just for simplifying string construction,
  // we could replace it with logic later

  def unaryAnyNode(input: String): String = {
    s"""$ANY_NODE
       |$input
       |""".stripMargin.stripLineEnd
  }

  def anySubtree(): String = {
    ANY_SUBTREE
  }

  def unaryNode(node: String, input: String, term: String*): String = {
    s"""$node(${term.mkString(", ")})
       |$input
       |""".stripMargin.stripLineEnd
  }

  def binaryNode(node: String, left: String, right: String, term: String*): String = {
    s"""$node(${term.mkString(", ")})
       |$left
       |$right
       |""".stripMargin.stripLineEnd
  }

  def naryNode(node: String, inputs: List[AnyRef], term: String*): String = {
    val strInputs = inputs.mkString("\n")
    s"""$node(${term.mkString(", ")})
       |$strInputs
       |""".stripMargin.stripLineEnd
  }

  def values(node: String, term: String*): String = {
    s"$node(${term.mkString(", ")})"
  }

  def term(term: AnyRef, value: AnyRef*): String = {
    s"$term=[${value.mkString(", ")}]"
  }

  def tuples(value: List[AnyRef]*): String = {
    val listValues = value.map(listValue => s"{ ${listValue.mkString(", ")} }")
    term("tuples", "[" + listValues.mkString(", ") + "]")
  }

  def batchTableNode(idx: Int): String = {
    s"DataSetScan(table=[[_DataSetTable_$idx]])"
  }

  def AppendTableNode(idx: Int): String = {
    s"AppendStreamScan(table=[[_DataStreamTable_$idx]])"
  }

  def UpsertTableNode(idx: Int): String = {
    s"UpsertStreamScan(table=[[_DataStreamTable_$idx]])"
  }
}

case class BatchTableTestUtil() extends TableTestUtil {
  val javaEnv = new LocalEnvironment()
  val javaTableEnv = TableEnvironment.getTableEnvironment(javaEnv)
  val env = new ExecutionEnvironment(javaEnv)
  val tableEnv = TableEnvironment.getTableEnvironment(env)

  def addTable[T: TypeInformation](
      name: String,
      fields: Expression*)
    : Table = {
    val ds = mock(classOf[DataSet[T]])
    val jDs = mock(classOf[JDataSet[T]])
    when(ds.javaSet).thenReturn(jDs)
    val typeInfo: TypeInformation[T] = implicitly[TypeInformation[T]]
    when(jDs.getType).thenReturn(typeInfo)

    val t = ds.toTable(tableEnv, fields: _*)
    tableEnv.registerTable(name, t)
    t
  }

  def addJavaTable[T](typeInfo: TypeInformation[T], name: String, fields: String): Table = {

    val jDs = mock(classOf[JDataSet[T]])
    when(jDs.getType).thenReturn(typeInfo)

    val t = javaTableEnv.fromDataSet(jDs, fields)
    javaTableEnv.registerTable(name, t)
    t
  }

  def addFunction[T: TypeInformation](
      name: String,
      function: TableFunction[T])
    : TableFunction[T] = {
    tableEnv.registerFunction(name, function)
    function
  }

  def addFunction(name: String, function: ScalarFunction): Unit = {
    tableEnv.registerFunction(name, function)
  }

  def addFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      function: AggregateFunction[T, ACC]): Unit = {
    tableEnv.registerFunction(name, function)
  }

  def verifySql(query: String, expected: String): Unit = {
    verifyTable(tableEnv.sqlQuery(query), expected)
  }

  def verifyTable(resultTable: Table, expected: String): Unit = {
    val relNode = resultTable.getRelNode
    val optimized = tableEnv.optimize(relNode)
    verifyString(expected, optimized)
  }

  def verifyJavaSql(query: String, expected: String): Unit = {
    verifyJavaTable(javaTableEnv.sqlQuery(query), expected)
  }

  def verifyJavaTable(resultTable: Table, expected: String): Unit = {
    val relNode = resultTable.getRelNode
    val optimized = javaTableEnv.optimize(relNode)
    verifyString(expected, optimized)
  }

  def printTable(resultTable: Table): Unit = {
    val relNode = resultTable.getRelNode
    val optimized = tableEnv.optimize(relNode)
    println(RelOptUtil.toString(optimized))
  }

  def printSql(query: String): Unit = {
    printTable(tableEnv.sqlQuery(query))
  }

  def explain(resultTable: Table): String = {
    tableEnv.explain(resultTable)
  }
}

case class StreamTableTestUtil() extends TableTestUtil {
  val javaEnv = new LocalStreamEnvironment()
  javaEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val javaTableEnv = TableEnvironment.getTableEnvironment(javaEnv)
  val env = new StreamExecutionEnvironment(javaEnv)
  val tableEnv = TableEnvironment.getTableEnvironment(env)

  def addTable[T: TypeInformation](
      name: String,
      fields: Expression*)
    : Table = {

    val table = env.fromElements().toTableFromAppendStream(tableEnv, fields: _*)
    tableEnv.registerTable(name, table)
    table
  }

  def addJavaTable[T](typeInfo: TypeInformation[T], name: String, fields: String): Table = {
    val stream = javaEnv.addSource(new EmptySource[T], typeInfo)
    val table = javaTableEnv.fromAppendStream(stream, fields)
    javaTableEnv.registerTable(name, table)
    table
  }

  def addTableFromUpsert[T: TypeInformation](fields: Expression*): Table = {
    counter += 1
    addTableFromUpsert[T](s"Table$counter", fields: _*)
  }

  def addTableFromUpsert[T: TypeInformation](name: String, fields: Expression*): Table = {
    val table = env.fromElements().toTableFromUpsertStream(tableEnv, fields: _*)
    tableEnv.registerTable(name, table)
    table
  }

  def addJavaTableFromUpsert[T](
      typeInfo: TypeInformation[JTuple2[JBool, T]],
      name: String,
      fields: String): Table = {

    val stream = javaEnv.addSource(new EmptySource[JTuple2[JBool, T]], typeInfo)
    val table = javaTableEnv.fromUpsertStream(stream, fields)
    javaTableEnv.registerTable(name, table)
    table
  }

  def addFunction[T: TypeInformation](
      name: String,
      function: TableFunction[T])
    : TableFunction[T] = {
    tableEnv.registerFunction(name, function)
    function
  }

  def addFunction(name: String, function: ScalarFunction): Unit = {
    tableEnv.registerFunction(name, function)
  }

  def addFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      function: AggregateFunction[T, ACC]): Unit = {
    tableEnv.registerFunction(name, function)
  }

  def verifySql(query: String, expected: String): Unit = {
    verifyTable(tableEnv.sqlQuery(query), expected)
  }

  def verifySql(query: String, expected: String, updatesAsRetraction: Boolean): Unit = {
    verifyTable(tableEnv.sqlQuery(query), expected, updatesAsRetraction)
  }

  def verifySqlPlansIdentical(query1: String, queries: String*): Unit = {
    val resultTable1 = tableEnv.sqlQuery(query1)
    queries.foreach(s => verify2Tables(resultTable1, tableEnv.sqlQuery(s)))
  }

  def verifyTable(resultTable: Table, expected: String): Unit = {
    verifyTable(resultTable, expected, updatesAsRetraction = false)
  }

  def verifyTable(resultTable: Table, expected: String, updatesAsRetraction: Boolean): Unit = {
    val relNode = resultTable.getRelNode
    val optimized = tableEnv.optimize(relNode, updatesAsRetraction)
    verifyString(expected, optimized)
  }

  def verify2Tables(resultTable1: Table, resultTable2: Table): Unit = {
    val relNode1 = resultTable1.getRelNode
    val optimized1 = tableEnv.optimize(relNode1, updatesAsRetraction = false)
    val relNode2 = resultTable2.getRelNode
    val optimized2 = tableEnv.optimize(relNode2, updatesAsRetraction = false)
    assertEquals(RelOptUtil.toString(optimized1), RelOptUtil.toString(optimized2))
  }

  def verifyJavaSql(query: String, expected: String): Unit = {
    verifyJavaTable(javaTableEnv.sqlQuery(query), expected)
  }

  def verifyJavaSql(query: String, expected: String, updatesAsRetraction: Boolean): Unit = {
    verifyJavaTable(javaTableEnv.sqlQuery(query), expected, updatesAsRetraction)
  }

  def verifyJavaTable(resultTable: Table, expected: String): Unit = {
    verifyJavaTable(resultTable, expected, updatesAsRetraction = false)
  }

  def verifyJavaTable(resultTable: Table, expected: String, updatesAsRetraction: Boolean): Unit = {
    val relNode = resultTable.getRelNode
    val optimized = javaTableEnv.optimize(relNode, updatesAsRetraction)
    verifyString(expected, optimized)
  }

  // the print methods are for debugging purposes only
  def printTable(resultTable: Table): Unit = {
    val relNode = resultTable.getRelNode
    val optimized = tableEnv.optimize(relNode, updatesAsRetraction = false)
    println(RelOptUtil.toString(optimized))
  }

  def printSql(query: String): Unit = {
    printTable(tableEnv.sqlQuery(query))
  }

  def explain(resultTable: Table): String = {
    tableEnv.explain(resultTable)
  }
}

class EmptySource[T]() extends SourceFunction[T] {
  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
  }

  override def cancel(): Unit = {
  }
}
