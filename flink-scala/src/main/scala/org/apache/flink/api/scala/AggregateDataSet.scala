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
package org.apache.flink.api.scala

import org.apache.flink.annotation.Public
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.operators.ScalaAggregateOperator

import scala.reflect.ClassTag

/**
 * The result of [[DataSet.aggregate]]. This can be used to chain more aggregations to the one
 * aggregate operator.
 *
 * @tparam T
 *   The type of the DataSet, i.e., the type of the elements of the DataSet.
 * @deprecated
 *   All Flink Scala APIs are deprecated and will be removed in a future Flink major version. You
 *   can still build your application in Scala, but you should move to the Java version of either
 *   the DataStream and/or Table API.
 * @see
 *   <a href="https://s.apache.org/flip-265">FLIP-265 Deprecate and remove Scala API support</a>
 */
@deprecated(org.apache.flink.api.scala.FLIP_265_WARNING, since = "1.18.0")
@Public
class AggregateDataSet[T: ClassTag](set: ScalaAggregateOperator[T]) extends DataSet[T](set) {

  /**
   * Adds the given aggregation on the given field to the previous aggregation operation.
   *
   * This only works on Tuple DataSets.
   */
  def and(agg: Aggregations, field: Int): AggregateDataSet[T] = {
    set.and(agg, field)
    this
  }

  /**
   * Adds the given aggregation on the given field to the previous aggregation operation.
   *
   * This only works on CaseClass DataSets.
   */
  def and(agg: Aggregations, field: String): AggregateDataSet[T] = {
    val fieldIndex = fieldNames2Indices(set.getType, Array(field))(0)
    set.and(agg, fieldIndex)
    this
  }

  /** Syntactic sugar for [[and]] with `SUM` */
  def andSum(field: Int) = {
    and(Aggregations.SUM, field)
  }

  /** Syntactic sugar for [[and]] with `MAX` */
  def andMax(field: Int) = {
    and(Aggregations.MAX, field)
  }

  /** Syntactic sugar for [[and]] with `MIN` */
  def andMin(field: Int) = {
    and(Aggregations.MIN, field)
  }

  /** Syntactic sugar for [[and]] with `SUM` */
  def andSum(field: String) = {
    and(Aggregations.SUM, field)
  }

  /** Syntactic sugar for [[and]] with `MAX` */
  def andMax(field: String) = {
    and(Aggregations.MAX, field)
  }

  /** Syntactic sugar for [[and]] with `MIN` */
  def andMin(field: String) = {
    and(Aggregations.MIN, field)
  }
}
