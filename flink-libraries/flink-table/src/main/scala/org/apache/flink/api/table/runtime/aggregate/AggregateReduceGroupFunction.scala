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
package org.apache.flink.api.table.runtime.aggregate

import java.lang.Iterable

import com.google.common.base.Preconditions
import org.apache.flink.api.common.functions.{CombineFunction, RichGroupReduceFunction, RichMapPartitionFunction}
import org.apache.flink.api.table.Row
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._

/**
 * It wraps the aggregate logic inside of 
 * [[org.apache.flink.api.java.operators.GroupReduceOperator]].
 *
 * @param aggregates   The aggregate functions.
 * @param groupKeysMapping The index mapping of group keys between intermediate aggregate Row 
 *                         and output Row.
 * @param aggregateMapping The index mapping between aggregate function list and aggregated value
 *                         index in output Row.
 */
class AggregateReduceGroupFunction(
    private val aggregates: Array[Aggregate[_ <: Any]],
    private val groupKeysMapping: Array[(Int, Int)],
    private val aggregateMapping: Array[(Int, Int)])
    extends RichGroupReduceFunction[Row, Row] {

  private final val finalRowLength: Int = groupKeysMapping.length + aggregateMapping.length

  override def open(config: Configuration) {
    Preconditions.checkNotNull(aggregates)
    Preconditions.checkNotNull(groupKeysMapping)
  }

  /**
   * For grouped intermediate aggregate Rows, merge all of them into aggregate buffer,
   * calculate aggregated values output by aggregate buffer, and set them into output 
   * Row based on the mapping relation between intermediate aggregate data and output data.
   *
   * @param records  Grouped intermediate aggregate Rows iterator.
   * @param out The collector to hand results to.
   *
   */
  override def reduce(records: Iterable[Row], out: Collector[Row]): Unit = {
    
    val inputItr = records.iterator
    val buffer = inputItr.next
    while (inputItr.hasNext) {
      val next = inputItr.next
      aggregates.foreach(_.merge(next, buffer))
    }

    val output: Row = new Row(finalRowLength)

    groupKeysMapping.map {
      case (after, previous) =>
        output.setField(after, buffer.productElement(previous))
    }

    aggregateMapping.map {
      case (after, previous) =>
        output.setField(after, aggregates(previous).evaluate(buffer))
    }

    out.collect(output)
  }
}

/**
 * It wraps the aggregate logic inside of 
 * [[org.apache.flink.api.java.operators.GroupReduceOperator]] and 
 * [[org.apache.flink.api.java.operators.GroupCombineOperator]]
 *
 * @param aggregates   The aggregate functions.
 * @param groupKeysMapping The index mapping of group keys between intermediate aggregate Row 
 *                         and output Row.
 * @param aggregateMapping The index mapping between aggregate function list and aggregated value
 *                         index in output Row.
 */
class AggregateReduceCombineFunction(
    private val aggregates: Array[Aggregate[_ <: Any]],
    private val groupKeysMapping: Array[(Int, Int)],
    private val aggregateMapping: Array[(Int, Int)])
    extends RichGroupReduceFunction[Row, Row] with CombineFunction[Row, Row] {

  private val finalRowLength: Int = groupKeysMapping.length + aggregateMapping.length

  override def open(config: Configuration) {
    Preconditions.checkNotNull(aggregates)
    Preconditions.checkNotNull(groupKeysMapping)
  }

  /**
   * For grouped intermediate aggregate Rows, merge all of them into aggregate buffer,
   * calculate aggregated values output by aggregate buffer, and set them into output 
   * Row based on the mapping relation between intermediate aggregate Row and output Row.
   *
   * @param records  Grouped intermediate aggregate Rows iterator.
   * @param out The collector to hand results to.
   *
   */
  override def reduce(records: Iterable[Row], out: Collector[Row]): Unit = {
    val inputItr = records.iterator
    val buffer = inputItr.next
    while (inputItr.hasNext) {
      val next = inputItr.next
      aggregates.foreach(_.merge(next, buffer))
    }

    val output: Row = new Row(finalRowLength)

    groupKeysMapping.map {
      case (after, previous) =>
        output.setField(after, buffer.productElement(previous))
    }

    aggregateMapping.map {
      case (after, previous) =>
        output.setField(after, aggregates(previous).evaluate(buffer))
    }

    out.collect(output)
  }

  /**
   * For sub-grouped intermediate aggregate Rows, merge all of them into aggregate buffer,
   *
   * @param records  Sub-grouped intermediate aggregate Rows iterator.
   * @return Combined intermediate aggregate Row.
   *
   */
  override def combine(records: Iterable[Row]): Row = {

    val inputItr = records.iterator
    val buffer = inputItr.next
    while (inputItr.hasNext) {
      val next = inputItr.next
      aggregates.foreach(_.merge(next, buffer))
    }
    buffer
  }
}
