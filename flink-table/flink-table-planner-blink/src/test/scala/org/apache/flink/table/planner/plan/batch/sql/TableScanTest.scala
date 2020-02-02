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

package org.apache.flink.table.planner.plan.batch.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}
import org.apache.flink.table.planner.expressions.utils.Func0
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

class TableScanTest extends TableTestBase {

  private val util = batchTestUtil()

  @Test
  def testTableSourceScan(): Unit = {
    util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.verifyPlan("SELECT * FROM MyTable")
  }

  @Test
  def testDDLTableScan(): Unit = {
    util.addTable(
      """
        |CREATE TABLE src (
        |  ts TIMESTAMP(3),
        |  a INT,
        |  b DOUBLE,
        |  WATERMARK FOR ts AS ts - INTERVAL '0.001' SECOND
        |) WITH (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'true'
        |)
      """.stripMargin)
    util.verifyPlan("SELECT * FROM src WHERE a > 1")
  }

  private def prepareComputedColumnDDL(): Unit = {
    // Create table with field as atom expression.
    util.tableEnv.registerFunction("my_udf", Func0)
    util.addTable(
      s"""
         |create table t1(
         |  a int,
         |  b varchar,
         |  c as a + 1,
         |  d as to_timestamp(b),
         |  e as my_udf(a)
         |) with (
         |  'connector' = 'COLLECTION',
         |  'is-bounded' = 'true'
         |)
       """.stripMargin)
  }

  @Test
  def testDDLWithComputedColumn(): Unit = {
    prepareComputedColumnDDL()
    util.verifyPlan("SELECT * FROM t1")
  }

  private def prepareWatermarkDDL(): Unit = {
    // Create table with field as atom expression.
    util.tableEnv.registerFunction("my_udf", Func0)
    util.addTable(
      s"""
         |create table t1(
         |  a int,
         |  b varchar,
         |  c as a + 1,
         |  d as to_timestamp(b),
         |  e as my_udf(a),
         |  WATERMARK FOR d AS d - INTERVAL '0.001' SECOND
         |) with (
         |  'connector' = 'COLLECTION',
         |  'is-bounded' = 'true'
         |)
       """.stripMargin)
  }

  @Test
  def testDDLWithWatermarkComputedColumn(): Unit = {
    prepareWatermarkDDL()
    util.verifyPlan("SELECT * FROM t1")
  }

  @Test
  def testTableApiScanWithComputedColumn(): Unit = {
    prepareComputedColumnDDL()
    util.verifyPlan(util.tableEnv.from("t1"))
  }

  @Test
  def testTableApiScanWithWatermark(): Unit = {
    prepareWatermarkDDL()
    util.verifyPlan(util.tableEnv.from("t1"))
  }

  @Test
  def testTableApiScanWithDDL(): Unit = {
    util.addTable(
      s"""
         |create table t1(
         |  a int,
         |  b varchar
         |) with (
         |  'connector' = 'COLLECTION',
         |  'is-bounded' = 'true'
         |)
       """.stripMargin)
    util.verifyPlan(util.tableEnv.from("t1"))
  }

  @Test
  def testTableApiScanWithTemporaryTable(): Unit = {
    util.tableEnv.connect(new FileSystem().path(tempFolder.newFile.getPath))
        .withFormat(new OldCsv())
        .withSchema(new Schema().field("word", DataTypes.STRING()))
        .createTemporaryTable("t1")
    util.verifyPlan(util.tableEnv.from("t1"))
  }
}
