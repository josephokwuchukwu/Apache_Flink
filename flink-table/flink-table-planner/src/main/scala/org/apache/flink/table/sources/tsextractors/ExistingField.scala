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

package org.apache.flink.table.sources.tsextractors

import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Types, ValidationException}
import org.apache.flink.table.expressions._
import org.apache.flink.table.expressions.FunctionDefinitions.CAST

/**
  * Converts an existing [[Long]], [[java.sql.Timestamp]], or
  * timestamp formatted [[java.lang.String]] field (e.g., "2018-05-28 12:34:56.000") into
  * a rowtime attribute.
  *
  * @param field The field to convert into a rowtime attribute.
  */
final class ExistingField(val field: String) extends TimestampExtractor {

  override def getArgumentFields: Array[String] = Array(field)

  @throws[ValidationException]
  override def validateArgumentFields(argumentFieldTypes: Array[TypeInformation[_]]): Unit = {
    val fieldType = argumentFieldTypes(0)

    fieldType match {
      case Types.LONG => // OK
      case Types.SQL_TIMESTAMP => // OK
      case Types.STRING => // OK
      case _: TypeInformation[_] =>
        throw new ValidationException(
          s"Field '$field' must be of type Long or Timestamp or String but is of type $fieldType.")
    }
  }

  /**
    * Returns an [[Expression]] that casts a [[Long]], [[java.sql.Timestamp]], or
    * timestamp formatted [[java.lang.String]] field (e.g., "2018-05-28 12:34:56.000")
    * into a rowtime attribute.
    */
  override def getExpression(
      fieldAccesses: Array[FieldReferenceExpression],
      fieldTypes: Array[TypeInformation[_]]): Expression = {
    val fieldAccess: FieldReferenceExpression = fieldAccesses(0)
    val fieldType = fieldTypes(0)

    val fieldAccessWithType = new FieldReferenceExpression(fieldAccess.getName, Types.LONG)
    fieldType match {
      case Types.LONG =>
        // access LONG field
        fieldAccessWithType
      case Types.SQL_TIMESTAMP =>
        // cast timestamp to long
        ExpressionUtils.call(CAST, Seq(fieldAccessWithType, Types.LONG))
      case Types.STRING =>
        ExpressionUtils.call(
          CAST,
          Seq(
            ExpressionUtils.call(CAST, Seq(fieldAccessWithType, SqlTimeTypeInfo.TIMESTAMP)),
            Types.LONG))
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: ExistingField => field == that.field
    case _ => false
  }

  override def hashCode(): Int = {
    field.hashCode
  }
}
