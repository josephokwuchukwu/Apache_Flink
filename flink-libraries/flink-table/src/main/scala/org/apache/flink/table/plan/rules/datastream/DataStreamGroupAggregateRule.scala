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

package org.apache.flink.table.plan.rules.datastream

import org.apache.calcite.plan.{Convention, RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.flink.table.api.TableException
import org.apache.flink.table.plan.nodes.datastream.{DataStreamGroupAggregate, DataStreamConvention}

import scala.collection.JavaConversions._

/**
  * Rule to convert a [[LogicalAggregate]] into a [[DataStreamGroupAggregate]].
  */
class DataStreamGroupAggregateRule
  extends ConverterRule(
    classOf[LogicalAggregate],
    Convention.NONE,
    DataStreamConvention.INSTANCE,
    "DataStreamGroupAggregateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg: LogicalAggregate = call.rel(0).asInstanceOf[LogicalAggregate]

    // check if we have distinct aggregates
    val distinctAggs = agg.getAggCallList.exists(_.isDistinct)
    if (distinctAggs) {
      throw TableException("DISTINCT aggregates are currently not supported.")
    }

    // check if we have grouping sets
    val groupSets = agg.getGroupSets.size() != 1 || agg.getGroupSets.get(0) != agg.getGroupSet
    if (groupSets || agg.indicator) {
      throw TableException("GROUPING SETS are currently not supported.")
    }

    !distinctAggs && !groupSets && !agg.indicator
  }

  override def convert(rel: RelNode): RelNode = {
    val agg: LogicalAggregate = rel.asInstanceOf[LogicalAggregate]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(DataStreamConvention.INSTANCE)
    val convInput: RelNode = RelOptRule.convert(agg.getInput, DataStreamConvention.INSTANCE)

    new DataStreamGroupAggregate(
      rel.getCluster,
      traitSet,
      convInput,
      agg.getNamedAggCalls,
      rel.getRowType,
      agg.getInput.getRowType,
      agg.getGroupSet.toArray)
  }
}

object DataStreamGroupAggregateRule {
  val INSTANCE: RelOptRule = new DataStreamGroupAggregateRule
}

