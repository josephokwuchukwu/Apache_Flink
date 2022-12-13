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
package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.`trait`.{InputRelDistributionTrait, InputRelDistributionTraitDef}
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.RelDistribution.Type
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rex.{RexCall, RexInputRef, RexProgram}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.util.mapping.{Mapping, Mappings, MappingType}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/** Stream physical RelNode for [[Calc]]. */
class StreamPhysicalCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    calcProgram: RexProgram,
    outputRowType: RelDataType)
  extends StreamPhysicalCalcBase(cluster, traitSet, inputRel, calcProgram, outputRowType) {

  override def copy(traitSet: RelTraitSet, child: RelNode, program: RexProgram): Calc = {
    new StreamPhysicalCalc(cluster, traitSet, child, program, outputRowType)
  }

  override def satisfyTraitsFromInputs(inputsTraitSet: RelTraitSet): Option[RelNode] = {
    val inputDistribution = inputsTraitSet.getTrait(InputRelDistributionTraitDef.INSTANCE)
    // currently support only hash distribution
    if (inputDistribution == null || inputDistribution.getType != Type.HASH_DISTRIBUTED) {
      return None
    }
    val projects = calcProgram.getProjectList.asScala.map(calcProgram.expandLocalRef)

    def getProjectMapping: Mapping = {
      val mapping =
        Mappings.create(MappingType.FUNCTION, getInput.getRowType.getFieldCount, projects.size)
      projects.zipWithIndex.foreach {
        case (project, index) =>
          project match {
            case inputRef: RexInputRef => mapping.set(inputRef.getIndex, index)
            case call: RexCall if call.getKind == SqlKind.AS =>
              call.getOperands.head match {
                case inputRef: RexInputRef => mapping.set(inputRef.getIndex, index)
                case _ => // ignore
              }
            case _ => // ignore
          }
      }
      mapping
    }

    val mapping = getProjectMapping
    val appliedDistribution = inputDistribution.apply(mapping)

    if (appliedDistribution eq InputRelDistributionTrait.ANY) {
      return None
    }

    val providedTraits = getTraitSet.plus(appliedDistribution)
    Some(copy(providedTraits, Seq(getInput).asJava))
  }

  override def translateToExecNode(): ExecNode[_] = {
    val projection = calcProgram.getProjectList.map(calcProgram.expandLocalRef)
    val condition = if (calcProgram.getCondition != null) {
      calcProgram.expandLocalRef(calcProgram.getCondition)
    } else {
      null
    }

    new StreamExecCalc(
      unwrapTableConfig(this),
      projection,
      condition,
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription)
  }
}
