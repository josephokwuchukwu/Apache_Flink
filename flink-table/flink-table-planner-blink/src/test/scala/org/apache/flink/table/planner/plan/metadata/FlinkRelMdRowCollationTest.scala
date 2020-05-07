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

package org.apache.flink.table.planner.plan.metadata

import com.google.common.collect.ImmutableList
import org.apache.calcite.rel.logical.{LogicalFilter, LogicalProject, LogicalValues}
import org.apache.calcite.rel.{RelCollation, RelCollations, RelFieldCollation}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.fun.SqlStdOperatorTable.{LESS_THAN, PLUS}
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.JavaConversions._

class FlinkRelMdRowCollationTest extends FlinkRelMdHandlerTestBase {

  protected lazy val collationValues: LogicalValues = {
    val valuesType = relBuilder.getTypeFactory
      .builder()
      .add("a", SqlTypeName.BIGINT)
      .add("b", SqlTypeName.DOUBLE)
      .add("c", SqlTypeName.BOOLEAN)
      .add("d", SqlTypeName.INTEGER)
      .build()
    val tupleList = List(
      List("1", "9.0", "true", "2"),
      List("2", "6.0", "false", "3"),
      List("3", "3.0", "true", "4")
    ).map(createLiteralList(valuesType, _))
    relBuilder.clear()
    relBuilder.values(tupleList, valuesType)
    relBuilder.build().asInstanceOf[LogicalValues]
  }

  @Test
  def testCollationsOnTableScan(): Unit = {
    Array(studentLogicalScan, studentBatchScan, studentStreamScan).foreach { scan =>
      assertEquals(ImmutableList.of(), mq.collations(scan))
    }
  }

  @Test
  def testCollationsOnValues(): Unit = {
    assertEquals(ImmutableList.of(RelCollations.of(6)), mq.collations(logicalValues))
    assertEquals(
      ImmutableList.of(
        convertToRelCollation(List.range(0, 8)),
        convertToRelCollation(List.range(1, 8)),
        convertToRelCollation(List.range(2, 8)),
        convertToRelCollation(List.range(3, 8)),
        convertToRelCollation(List.range(4, 8)),
        convertToRelCollation(List.range(5, 8)),
        convertToRelCollation(List.range(6, 8)),
        convertToRelCollation(List.range(7, 8))
      ),
      mq.collations(emptyValues))
    assertEquals(
      ImmutableList.of(convertToRelCollation(List.range(0, 4)), RelCollations.of(3)),
      mq.collations(collationValues))
  }

  @Test
  def testCollationsOnProject(): Unit = {
    assertEquals(ImmutableList.of(), mq.collations(logicalProject))

    val project: LogicalProject = {
      relBuilder.push(collationValues)
      val projects = List(
        // a + b
        relBuilder.call(PLUS, relBuilder.field(0), relBuilder.literal(1)),
        // c
        relBuilder.field(2),
        // d
        relBuilder.field(3),
        // 2
        rexBuilder.makeLiteral(2L, longType, true)
      )
      relBuilder.project(projects).build().asInstanceOf[LogicalProject]
    }
    assertEquals(ImmutableList.of(RelCollations.of(2)), mq.collations(project))
  }

  @Test
  def testCollationsOnFilter(): Unit = {
    assertEquals(ImmutableList.of(), mq.collations(logicalFilter))

    relBuilder.push(studentLogicalScan)
    val filter: LogicalFilter = {
      relBuilder.push(collationValues)
      // a < 10
      val expr = relBuilder.call(LESS_THAN, relBuilder.field(0), relBuilder.literal(10))
      relBuilder.filter(expr).build.asInstanceOf[LogicalFilter]
    }
    assertEquals(
      ImmutableList.of(convertToRelCollation(List.range(0, 4)), RelCollations.of(3)),
      mq.collations(filter))
  }

  @Test
  def testCollationsOnExpand(): Unit = {
    Array(logicalExpand, flinkLogicalExpand, batchExpand, streamExpand).foreach {
      expand => assertEquals(ImmutableList.of(), mq.collations(expand))
    }
  }

  @Test
  def testCollationsOnExchange(): Unit = {
    Array(batchExchange, streamExchange).foreach {
      exchange => assertEquals(ImmutableList.of(), mq.collations(exchange))
    }
  }

  @Test
  def testCollationsOnRank(): Unit = {
    Array(logicalRank, flinkLogicalRank, batchLocalRank, streamRank).foreach {
      rank => assertEquals(ImmutableList.of(), mq.collations(rank))
    }
  }

  @Test
  def testCollationsOnSort(): Unit = {
    Array(logicalSort, flinkLogicalSort, batchSort, streamSort,
      logicalSortLimit, flinkLogicalSortLimit, batchSortLimit, streamSortLimit).foreach { sort =>
      assertEquals(
        ImmutableList.of(RelCollations.of(
          new RelFieldCollation(6),
          new RelFieldCollation(2, RelFieldCollation.Direction.DESCENDING))),
        mq.collations(sort))
    }

    Array(logicalLimit, logicalLimit, batchLimit, streamLimit).foreach { limit =>
      assertEquals(ImmutableList.of(RelCollations.of()), mq.collations(limit))
    }
  }

  @Test
  def testCollationsOnWindow(): Unit = {
    assertEquals(ImmutableList.of(), mq.collations(flinkLogicalOverAgg))
  }

  @Test
  def testCollationsOnAggregate(): Unit = {
    Array(logicalAgg, flinkLogicalAgg, batchGlobalAggWithLocal, batchGlobalAggWithoutLocal,
      batchLocalAgg).foreach {
      agg => assertEquals(ImmutableList.of(), mq.collations(agg))
    }
  }

  @Test
  def testCollationsOnJoin(): Unit = {
    Array(logicalInnerJoinOnUniqueKeys, logicalLeftJoinNotOnUniqueKeys,
      logicalRightJoinOnRHSUniqueKeys, logicalFullJoinWithoutEquiCond,
      logicalSemiJoinOnLHSUniqueKeys, logicalAntiJoinOnRHSUniqueKeys).foreach {
      join => assertEquals(ImmutableList.of(), mq.collations(join))
    }
  }

  @Test
  def testCollationsOnUnion(): Unit = {
    Array(logicalUnion, logicalUnionAll).foreach {
      union => assertEquals(ImmutableList.of(), mq.collations(union))
    }
  }

  @Test
  def testCollationsOnIntersect(): Unit = {
    Array(logicalIntersect, logicalIntersectAll).foreach {
      intersect => assertEquals(ImmutableList.of(), mq.collations(intersect))
    }
  }

  @Test
  def testCollationsOnMinus(): Unit = {
    Array(logicalMinus, logicalMinusAll).foreach {
      minus => assertEquals(ImmutableList.of(), mq.collations(minus))
    }
  }

  @Test
  def testCollationsOnDefault(): Unit = {
    assertEquals(ImmutableList.of(), mq.collations(testRel))
  }

  private def convertToRelCollation(relFieldCollations: List[Int]): RelCollation = {
    RelCollations.of(relFieldCollations.map(i => new RelFieldCollation(i)): _*)
  }
}
