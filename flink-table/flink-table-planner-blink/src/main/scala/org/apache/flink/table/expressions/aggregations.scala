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
package org.apache.flink.table.expressions

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.MultisetTypeInfo
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.sql.FlinkSqlOperatorTable
import org.apache.flink.table.functions.utils.AggSqlFunction
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.functions.{AggregateFunction, FunctionRequirement, TableAggregateFunction, UserDefinedAggregateFunction}
import org.apache.flink.table.types.TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType
import org.apache.flink.table.typeutils.TypeInfoCheckUtils
import org.apache.flink.table.validate.{ValidationFailure, ValidationResult, ValidationSuccess}

import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.sql.fun.SqlSumAggFunction
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilder.AggCall

abstract sealed class Aggregation extends PlannerExpression {

  override def toString = s"Aggregate"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode =
    throw new UnsupportedOperationException("Aggregate cannot be transformed to RexNode")

  /**
    * Convert Aggregate to its counterpart in Calcite, i.e. AggCall
    */
  private[flink] def toAggCall(
      name: String,
      isDistinct: Boolean = false
  )(implicit relBuilder: RelBuilder): AggCall

  /**
    * Returns the SqlAggFunction for this Aggregation.
    */
  private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder): SqlAggFunction

}

case class DistinctAgg(child: PlannerExpression) extends Aggregation {

  def distinct: PlannerExpression = DistinctAgg(child)

  override private[flink] def resultType: TypeInformation[_] = child.resultType

  override private[flink] def validateInput(): ValidationResult = {
    super.validateInput()
    child match {
      case agg: Aggregation =>
        child.validateInput()
      case _ =>
        ValidationFailure(s"Distinct modifier cannot be applied to $child! " +
          s"It can only be applied to an aggregation expression, for example, " +
          s"'a.count.distinct which is equivalent with COUNT(DISTINCT a).")
    }
  }

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = true)(implicit relBuilder: RelBuilder) = {
    child.asInstanceOf[Aggregation].toAggCall(name, isDistinct = true)
  }

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) = {
    child.asInstanceOf[Aggregation].getSqlAggFunction()
  }

  override private[flink] def children = Seq(child)
}

case class Sum(child: PlannerExpression) extends Aggregation {
  override private[flink] def children: Seq[PlannerExpression] = Seq(child)
  override def toString = s"sum($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(
      FlinkSqlOperatorTable.SUM,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeInfoCheckUtils.assertNumericExpr(child.resultType, "sum")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) = {
    val returnType = relBuilder
      .getTypeFactory.asInstanceOf[FlinkTypeFactory]
      .createFieldTypeFromLogicalType(fromTypeInfoToLogicalType(resultType))
    new SqlSumAggFunction(returnType)
  }
}

case class Sum0(child: PlannerExpression) extends Aggregation {
  override private[flink] def children: Seq[PlannerExpression] = Seq(child)
  override def toString = s"sum0($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(
      FlinkSqlOperatorTable.SUM0,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeInfoCheckUtils.assertNumericExpr(child.resultType, "sum0")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    FlinkSqlOperatorTable.SUM0
}

case class Min(child: PlannerExpression) extends Aggregation {
  override private[flink] def children: Seq[PlannerExpression] = Seq(child)
  override def toString = s"min($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(
      FlinkSqlOperatorTable.MIN,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeInfoCheckUtils.assertOrderableExpr(child.resultType, "min")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) = {
    FlinkSqlOperatorTable.MIN
  }
}

case class Max(child: PlannerExpression) extends Aggregation {
  override private[flink] def children: Seq[PlannerExpression] = Seq(child)
  override def toString = s"max($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(
      FlinkSqlOperatorTable.MAX,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeInfoCheckUtils.assertOrderableExpr(child.resultType, "max")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) = {
    FlinkSqlOperatorTable.MAX
  }
}

case class Count(child: PlannerExpression) extends Aggregation {
  override private[flink] def children: Seq[PlannerExpression] = Seq(child)
  override def toString = s"count($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(
      FlinkSqlOperatorTable.COUNT,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)
  }

  override private[flink] def resultType = BasicTypeInfo.LONG_TYPE_INFO

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) = {
    FlinkSqlOperatorTable.COUNT
  }
}

case class Avg(child: PlannerExpression) extends Aggregation {
  override private[flink] def children: Seq[PlannerExpression] = Seq(child)
  override def toString = s"avg($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(
      FlinkSqlOperatorTable.AVG,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeInfoCheckUtils.assertNumericExpr(child.resultType, "avg")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) = {
    FlinkSqlOperatorTable.AVG
  }
}

/**
  * Returns a multiset aggregates.
  */
case class Collect(child: PlannerExpression) extends Aggregation  {

  override private[flink] def children: Seq[PlannerExpression] = Seq(child)

  override private[flink] def resultType: TypeInformation[_] =
    MultisetTypeInfo.getInfoFor(child.resultType)

  override def toString: String = s"collect($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(
      FlinkSqlOperatorTable.COLLECT,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)
  }

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) = {
    FlinkSqlOperatorTable.COLLECT
  }
}

case class StddevPop(child: PlannerExpression) extends Aggregation {
  override private[flink] def children: Seq[PlannerExpression] = Seq(child)
  override def toString = s"stddev_pop($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(
      FlinkSqlOperatorTable.STDDEV_POP,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeInfoCheckUtils.assertNumericExpr(child.resultType, "stddev_pop")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    FlinkSqlOperatorTable.STDDEV_POP
}

case class StddevSamp(child: PlannerExpression) extends Aggregation {
  override private[flink] def children: Seq[PlannerExpression] = Seq(child)
  override def toString = s"stddev_samp($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(
      FlinkSqlOperatorTable.STDDEV_SAMP,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeInfoCheckUtils.assertNumericExpr(child.resultType, "stddev_samp")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    FlinkSqlOperatorTable.STDDEV_SAMP
}

case class VarPop(child: PlannerExpression) extends Aggregation {
  override private[flink] def children: Seq[PlannerExpression] = Seq(child)
  override def toString = s"var_pop($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(
      FlinkSqlOperatorTable.VAR_POP,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeInfoCheckUtils.assertNumericExpr(child.resultType, "var_pop")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    FlinkSqlOperatorTable.VAR_POP
}

case class VarSamp(child: PlannerExpression) extends Aggregation {
  override private[flink] def children: Seq[PlannerExpression] = Seq(child)
  override def toString = s"var_samp($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(
      FlinkSqlOperatorTable.VAR_SAMP,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeInfoCheckUtils.assertNumericExpr(child.resultType, "var_samp")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    FlinkSqlOperatorTable.VAR_SAMP
}

/**
  * Expression for calling a user-defined aggregate function.
  */
case class AggFunctionCall(
    val aggregateFunction: UserDefinedAggregateFunction[_, _],
    resultTypeInfo: TypeInformation[_],
    accTypeInfo: TypeInformation[_],
    args: Seq[PlannerExpression])
  extends Aggregation {

  if (aggregateFunction.isInstanceOf[TableAggregateFunction[_, _]]) {
    throw new UnsupportedOperationException("TableAggregateFunction is unsupported now.")
  }

  private val aggFunction = aggregateFunction.asInstanceOf[AggregateFunction[_, _]]

  override private[flink] def children: Seq[PlannerExpression] = args

  override def resultType: TypeInformation[_] = resultTypeInfo

  override def validateInput(): ValidationResult = {
    val signature = children.map(_.resultType)
    // look for a signature that matches the input types
    val foundSignature = getAccumulateMethodSignature(
      aggFunction,
      signature.map(fromTypeInfoToLogicalType))
    if (foundSignature.isEmpty) {
      ValidationFailure(s"Given parameters do not match any signature. \n" +
                          s"Actual: ${
                            signatureToString(signature.map(fromLegacyInfoToDataType))} \n" +
                          s"Expected: ${
                            getMethodSignatures(aggregateFunction, "accumulate")
                              .map(_.drop(1))
                              .map(signatureToString)
                              .sorted  // make sure order to verify error messages in tests
                              .mkString(", ")}")
    } else {
      ValidationSuccess
    }
  }

  override def toString: String = s"${aggregateFunction.getClass.getSimpleName}($args)"

  override def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(
      this.getSqlAggFunction(),
      isDistinct,
      false,
      null,
      name,
      args.map(_.toRexNode): _*)
  }

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) = {
    val typeFactory = relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]

    val requiresOver = aggregateFunction match {
      case a: AggregateFunction[_, _] =>
        a.getRequirements.contains(FunctionRequirement.OVER_WINDOW_ONLY)
      case _ => false
    }

    AggSqlFunction(
      aggregateFunction.functionIdentifier,
      aggregateFunction.toString,
      aggFunction,
      fromLegacyInfoToDataType(resultType),
      fromLegacyInfoToDataType(accTypeInfo),
      typeFactory,
      requiresOver)
  }

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(this.getSqlAggFunction(), args.map(_.toRexNode): _*)
  }
}
