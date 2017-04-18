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
package org.apache.flink.table.runtime.aggregate

import java.lang.Iterable

import org.apache.flink.api.common.functions._
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.codegen.{Compiler, GeneratedAggregationsFunction}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
  * [[GroupCombineFunction]] and [[MapPartitionFunction]] to compute pre-aggregates for batch
  * (DataSet) queries.
  *
  * @param genAggregations Code-generated [[GeneratedAggregations]]
  */
class DataSetPreAggFunction(genAggregations: GeneratedAggregationsFunction)
  extends AbstractRichFunction
  with GroupCombineFunction[Row, Row]
  with MapPartitionFunction[Row, Row]
  with Compiler[GeneratedAggregations] {

  private var output: Row = _
  private var accumulators: Row = _

  val LOG = LoggerFactory.getLogger(this.getClass)
  private var function: GeneratedAggregations = _

  override def open(config: Configuration) {
    LOG.debug(s"Compiling AggregateHelper: $genAggregations.name \n\n " +
                s"Code:\n$genAggregations.code")
    val clazz = compile(
      getClass.getClassLoader,
      genAggregations.name,
      genAggregations.code)
    LOG.debug("Instantiating AggregateHelper.")
    function = clazz.newInstance()

    output = function.createOutputRow()
  }

  override def combine(values: Iterable[Row], out: Collector[Row]): Unit = {
    preaggregate(values, out)
  }

  override def mapPartition(values: Iterable[Row], out: Collector[Row]): Unit = {
    preaggregate(values, out)
  }

  def preaggregate(records: Iterable[Row], out: Collector[Row]): Unit = {

    // create accumulators
    accumulators = function.createAccumulators()

    val iterator = records.iterator()

    while (iterator.hasNext) {
      val record = iterator.next()

      // accumulate
      function.accumulate(accumulators, record)

      // check if this record is the last record
      if (!iterator.hasNext) {
        // set group keys value to output
        function.setKeyToOutput(record, output)

        function.copyAccumulatorsToBuffer(accumulators, output)

        out.collect(output)
      }
    }
  }

}
