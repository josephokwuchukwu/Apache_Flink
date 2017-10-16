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

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.sql.SemiJoinType
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.datastream.DataStreamCalc
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalCorrelate}
import org.apache.flink.table.plan.schema.RowSchema

class DataStreamCalcRule
  extends ConverterRule(
    classOf[FlinkLogicalCalc],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASTREAM,
    "DataStreamCalcRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc = call.rels(0).asInstanceOf[FlinkLogicalCalc]
    val input = calc.getInput.asInstanceOf[RelSubset].getBest
    input match {
      // All predicates, except for the local ones of the left table, are forbidden for
      // TableFunction LEFT OUTER JOIN now. See CALCITE-2004 for more details.
      case node: FlinkLogicalCorrelate
        if node.getJoinType == SemiJoinType.LEFT && calc.getProgram.getCondition != null => false
      case _ => true
    }
  }

  def convert(rel: RelNode): RelNode = {
    val calc: FlinkLogicalCalc = rel.asInstanceOf[FlinkLogicalCalc]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASTREAM)
    val convInput: RelNode = RelOptRule.convert(calc.getInput, FlinkConventions.DATASTREAM)

    new DataStreamCalc(
      rel.getCluster,
      traitSet,
      convInput,
      new RowSchema(convInput.getRowType),
      new RowSchema(rel.getRowType),
      calc.getProgram,
      description)
  }
}

object DataStreamCalcRule {
  val INSTANCE: RelOptRule = new DataStreamCalcRule
}
