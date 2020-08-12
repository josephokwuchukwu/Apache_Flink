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

package org.apache.flink.table.planner.runtime.stream.sql

import java.util

import org.apache.flink.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.catalog.{CatalogPartitionImpl, CatalogPartitionSpec, ObjectPath}
import org.apache.flink.table.planner.factories.{TestValuesCatalog, TestValuesTableFactory}
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestingAppendSink}
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConversions._

@RunWith(classOf[Parameterized])
class PartitionableSourceITCase(
  val sourceFetchPartitions: Boolean,
  val useCatalogFilter: Boolean) extends StreamingTestBase {

  @Before
  override def before() : Unit = {
    super.before()
    env.setParallelism(1) // set sink parallelism to 1
    val data = Seq(
      row(1, "ZhangSan", "A", 1),
      row(2, "LiSi", "A", 1),
      row(3, "Jack", "A", 2),
      row(4, "Tom", "B", 3),
      row(5, "Vivi", "C", 1)
    )
    val myTableDataId = TestValuesTableFactory.registerData(data)

    val ddlTemp =
      s"""
         |CREATE TABLE MyTable (
         |  id int,
         |  name string,
         |  part1 string,
         |  part2 int,
         |  virtualField as part2 + 1)
         |  partitioned by (part1, part2)
         |  with (
         |    'connector' = 'values',
         |    'data-id' = '$myTableDataId',
         |    'bounded' = 'true',
         |    'partition-list' = '%s'
         |)
         |""".stripMargin

    if (sourceFetchPartitions) {
      val partitions = "part1:A,part2:1;part1:A,part2:2;part1:B,part2:3;part1:C,part2:1"
      tEnv.executeSql(String.format(ddlTemp, partitions))
    } else {
      tEnv.executeSql("drop catalog default_catalog")
      val catalog =
        new TestValuesCatalog("default_catalog", "default_database", useCatalogFilter);
      tEnv.registerCatalog("default_catalog", catalog)
      tEnv.useCatalog("default_catalog")
      // register table without partitions
      tEnv.executeSql(String.format(ddlTemp, ""))
      val mytablePath = ObjectPath.fromString("default_database.MyTable")
      // partition map
      val partitions = Seq(
        Map("part1"->"A", "part2"->"1"),
        Map("part1"->"A", "part2"->"2"),
        Map("part1"->"B", "part2"->"3"),
        Map("part1"->"C", "part2"->"1"));
      partitions.foreach(partition => {
        val catalogPartitionSpec = new CatalogPartitionSpec(partition)
        val catalogPartition = new CatalogPartitionImpl(
          new java.util.HashMap[String, String](), "");
        catalog.createPartition(mytablePath, catalogPartitionSpec, catalogPartition, true)
      })
    }
  }

  @Test
  def testSimplePartitionFieldPredicate1(): Unit = {
    val query = "SELECT * FROM MyTable WHERE part1 = 'A'"
    val result = tEnv.sqlQuery(query).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq(
      "1,ZhangSan,A,1,2",
      "2,LiSi,A,1,2",
      "3,Jack,A,2,3"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testPartialPartitionFieldPredicatePushDown(): Unit = {
    val query = "SELECT * FROM MyTable WHERE (id > 2 OR part1 = 'A') AND part2 > 1"
    val result = tEnv.sqlQuery(query).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq(
      "3,Jack,A,2,3",
      "4,Tom,B,3,4"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testUnconvertedExpression(): Unit = {
    val query = "select * from MyTable where trim(part1) = 'A' and part2 > 1"
    val result = tEnv.sqlQuery(query).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq(
      "3,Jack,A,2,3"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }
}

object PartitionableSourceITCase {
  @Parameterized.Parameters(name = "sourceFetchPartitions={0}, useCatalogFilter={1}")
  def parameters(): util.Collection[Array[Any]] = {
    Seq[Array[Any]](
      Array(true, false),
      Array(false, false),
      Array(false, true)
    )
  }
}
