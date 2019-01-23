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

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.datastream.{AppendStreamScan, UpsertStreamScan}
import org.apache.flink.table.plan.schema.{AppendStreamTable, RowSchema, UpsertStreamTable}
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalNativeTableScan

class DataStreamScanRule
  extends ConverterRule(
    classOf[FlinkLogicalNativeTableScan],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASTREAM,
    "DataStreamScanRule")
{

  override def matches(call: RelOptRuleCall): Boolean = {
    val scan: FlinkLogicalNativeTableScan = call.rel(0).asInstanceOf[FlinkLogicalNativeTableScan]
    val appendTable = scan.getTable.unwrap(classOf[AppendStreamTable[Any]])
    val upsertTable = scan.getTable.unwrap(classOf[UpsertStreamTable[Any]])

    // appendTable != null || upsertTable != null
    // avoid direct null here
    (appendTable, upsertTable) match {
      case _: (AppendStreamTable[_], _) => true
      case _: (_, UpsertStreamTable[_]) => true
      case _ => false
    }
  }

  def convert(rel: RelNode): RelNode = {
    val scan: FlinkLogicalNativeTableScan = rel.asInstanceOf[FlinkLogicalNativeTableScan]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASTREAM)
    val appendTable = scan.getTable.unwrap(classOf[AppendStreamTable[Any]])

    if (appendTable != null) {
      new AppendStreamScan(
        rel.getCluster,
        traitSet,
        scan.getTable,
        new RowSchema(rel.getRowType)
      )
    } else {
      new UpsertStreamScan(
        rel.getCluster,
        traitSet,
        scan.getTable,
        new RowSchema(rel.getRowType)
      )
    }
  }
}

object DataStreamScanRule {
  val INSTANCE: RelOptRule = new DataStreamScanRule
}
