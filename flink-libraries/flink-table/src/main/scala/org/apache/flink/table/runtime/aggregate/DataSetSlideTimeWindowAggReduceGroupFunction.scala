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

import org.apache.flink.api.common.functions.{CombineFunction, RichGroupReduceFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.codegen.{Compiler, GeneratedAggregationsFunction}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
  * It is used for sliding windows on batch for time-windows. It takes a prepared input row (with
  * aligned rowtime for pre-tumbling), pre-aggregates (pre-tumbles) rows, aligns the window start,
  * and replicates or omits records for different panes of a sliding window.
  *
  * This function is similar to [[DataSetTumbleCountWindowAggReduceGroupFunction]], however,
  * it does no final aggregate evaluation. It also includes the logic of
  * [[DataSetSlideTimeWindowAggFlatMapFunction]].
  *
  * @param genAggregations Code-generated [[GeneratedAggregations]]
  * @param keysAndAggregatesArity The total arity of keys and aggregates
  * @param windowSize window size of the sliding window
  * @param windowSlide window slide of the sliding window
  * @param returnType return type of this function
  */
class DataSetSlideTimeWindowAggReduceGroupFunction(
    private val genAggregations: GeneratedAggregationsFunction,
    private val keysAndAggregatesArity: Int,
    private val windowSize: Long,
    private val windowSlide: Long,
    @transient private val returnType: TypeInformation[Row])
  extends RichGroupReduceFunction[Row, Row]
  with CombineFunction[Row, Row]
  with ResultTypeQueryable[Row]
  with Compiler[GeneratedAggregations] {

  private val timeFieldPos = returnType.getArity - 1
  private val intermediateWindowStartPos = keysAndAggregatesArity

  protected var intermediateRow: Row = _
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

    accumulators = function.createAccumulators()
    intermediateRow = function.createOutputRow()
  }

  override def reduce(records: Iterable[Row], out: Collector[Row]): Unit = {

    // reset first accumulator
    function.resetAccumulator(accumulators)

    val iterator = records.iterator()

    while (iterator.hasNext) {
      val record = iterator.next()

      // accumulate
      function.mergeAccumulatorsPairWithKeyOffset(accumulators, record)

      // trigger tumbling evaluation
      if (!iterator.hasNext) {
        val windowStart = record.getField(timeFieldPos).asInstanceOf[Long]

        // adopted from SlidingEventTimeWindows.assignWindows
        var start: Long = TimeWindow.getWindowStartWithOffset(windowStart, 0, windowSlide)

        // skip preparing output if it is not necessary
        if (start > windowStart - windowSize) {

          // set group keys
          function.setKeyToOutput(record, intermediateRow)

          // set accumulators
          function.copyAccumulatorsToBuffer(accumulators, intermediateRow)

          // adopted from SlidingEventTimeWindows.assignWindows
          while (start > windowStart - windowSize) {
            intermediateRow.setField(intermediateWindowStartPos, start)
            out.collect(intermediateRow)
            start -= windowSlide
          }
        }
      }
    }
  }

  override def combine(records: Iterable[Row]): Row = {

    // reset first accumulator
    function.resetAccumulator(accumulators)

    val iterator = records.iterator()

    while (iterator.hasNext) {
      val record = iterator.next()

      function.mergeAccumulatorsPairWithKeyOffset(accumulators, record)

      // check if this record is the last record
      if (!iterator.hasNext) {

        // set group keys
        function.setKeyToOutput(record, intermediateRow)

        // set accumulators
        function.copyAccumulatorsToBuffer(accumulators, intermediateRow)

        intermediateRow.setField(timeFieldPos, record.getField(timeFieldPos))

        return intermediateRow
      }
    }

    // this code path should never be reached as we return before the loop finishes
    // we need this to prevent a compiler error
    throw new IllegalArgumentException("Group is empty. This should never happen.")
  }

  override def getProducedType: TypeInformation[Row] = {
    returnType
  }
}
