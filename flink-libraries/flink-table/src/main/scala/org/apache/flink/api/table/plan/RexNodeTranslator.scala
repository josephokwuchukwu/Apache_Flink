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

package org.apache.flink.api.table.plan

import org.apache.calcite.tools.RelBuilder.AggCall

import org.apache.flink.api.table.TableEnvironment
import org.apache.flink.api.table.expressions._

object RexNodeTranslator {

  /**
    * Extracts all aggregation expressions (zero, one, or more) from an expression, translates
    * these aggregation expressions into Calcite AggCalls, and replaces the original aggregation
    * expressions by field accesses expressions.
    */
  def extractAggCalls(
    exp: Expression,
    tableEnv: TableEnvironment): Pair[Expression, List[AggCall]] = {

    val relBuilder = tableEnv.getRelBuilder

    exp match {
      case agg: Aggregation =>
        val name = tableEnv.createUniqueAttributeName()
        val aggCall = agg.toAggCall(name)(relBuilder)
        val fieldExp = new UnresolvedFieldReference(name)
        (fieldExp, List(aggCall))
      case n @ Alias(agg: Aggregation, name) =>
        val aggCall = agg.toAggCall(name)(relBuilder)
        val fieldExp = new UnresolvedFieldReference(name)
        (fieldExp, List(aggCall))
      case l: LeafExpression =>
        (l, Nil)
      case u: UnaryExpression =>
        val c = extractAggCalls(u.child, tableEnv)
        (u.makeCopy(Array(c._1)), c._2)
      case b: BinaryExpression =>
        val l = extractAggCalls(b.left, tableEnv)
        val r = extractAggCalls(b.right, tableEnv)
        (b.makeCopy(Array(l._1, r._1)), l._2 ::: r._2)
      case e: Eval =>
        val c = extractAggCalls(e.condition, tableEnv)
        val t = extractAggCalls(e.ifTrue, tableEnv)
        val f = extractAggCalls(e.ifFalse, tableEnv)
        (e.makeCopy(Array(c._1, t._1, f._1)), c._2 ::: t._2 ::: f._2)

      // Scalar functions
      case c @ Call(name, args) =>
        val newArgs = args.map(extractAggCalls(_, tableEnv))
        (c.makeCopy((name +: args).toArray), newArgs.flatMap(_._2).toList)

      case e: Expression =>
        val newArgs = e.productIterator.map {
          case arg: Expression =>
            extractAggCalls(arg, tableEnv)
        }
        (e.makeCopy(newArgs.map(_._1).toArray), newArgs.flatMap(_._2).toList)
    }
  }

  def extractAggregations(
    exp: Expression,
    tableEnv: TableEnvironment): Pair[Expression, List[NamedExpression]] = {

    exp match {
      case agg: Aggregation =>
        val name = tableEnv.createUniqueAttributeName()
        val aggCall = Alias(agg, name)
        val fieldExp = new UnresolvedFieldReference(name)
        (fieldExp, List(aggCall))
      case n @ Alias(agg: Aggregation, name) =>
        val fieldExp = new UnresolvedFieldReference(name)
        (fieldExp, List(n))
      case l: LeafExpression =>
        (l, Nil)
      case u: UnaryExpression =>
        val c = extractAggregations(u.child, tableEnv)
        (u.makeCopy(Array(c._1)), c._2)
      case b: BinaryExpression =>
        val l = extractAggregations(b.left, tableEnv)
        val r = extractAggregations(b.right, tableEnv)
        (b.makeCopy(Array(l._1, r._1)), l._2 ::: r._2)

      // Scalar functions
      case c @ Call(name, args) =>
        val newArgs = args.map(extractAggregations(_, tableEnv))
        (c.makeCopy((name +: args).toArray), newArgs.flatMap(_._2).toList)

      case e: Expression =>
        val newArgs = e.productIterator.map {
          case arg: Expression =>
            extractAggregations(arg, tableEnv)
        }
        (e.makeCopy(newArgs.map(_._1).toArray), newArgs.flatMap(_._2).toList)
    }
  }
}
