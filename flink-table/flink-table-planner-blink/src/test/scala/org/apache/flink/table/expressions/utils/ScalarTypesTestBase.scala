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

package org.apache.flink.table.expressions.utils

import org.apache.flink.api.common.typeinfo.{PrimitiveArrayTypeInfo, Types}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.dataformat.Decimal
import org.apache.flink.table.typeutils.{DecimalTypeInfo, TimeIntervalTypeInfo}
import org.apache.flink.table.util.DateTimeTestUtil._
import org.apache.flink.types.Row

abstract class ScalarTypesTestBase extends ExpressionTestBase {

  override def testData: Row = {
    val testData = new Row(55)
    testData.setField(0, "This is a test String.")
    testData.setField(1, true)
    testData.setField(2, 42.toByte)
    testData.setField(3, 43.toShort)
    testData.setField(4, 44.toLong)
    testData.setField(5, 4.5.toFloat)
    testData.setField(6, 4.6)
    testData.setField(7, 3)
    testData.setField(8, " This is a test String. ")
    testData.setField(9, -42.toByte)
    testData.setField(10, -43.toShort)
    testData.setField(11, -44.toLong)
    testData.setField(12, -4.5.toFloat)
    testData.setField(13, -4.6)
    testData.setField(14, -3)
    testData.setField(15, Decimal.castFrom("-1231.1231231321321321111", 38, 19))
    testData.setField(16, UTCDate("1996-11-10"))
    testData.setField(17, UTCTime("06:55:44"))
    testData.setField(18, UTCTimestamp("1996-11-10 06:55:44.333"))
    testData.setField(19, 1467012213000L) // +16979 07:23:33.000
    testData.setField(20, 25) // +2-01
    testData.setField(21, null)
    testData.setField(22, Decimal.castFrom("2", 38, 19))
    testData.setField(23, "%This is a test String.")
    testData.setField(24, "*_This is a test String.")
    testData.setField(25, 0.42.toByte)
    testData.setField(26, 0.toShort)
    testData.setField(27, 0.toLong)
    testData.setField(28, 0.45.toFloat)
    testData.setField(29, 0.46)
    testData.setField(30, 1)
    testData.setField(31, Decimal.castFrom("-0.1231231321321321111", 38, 19))
    testData.setField(32, -1)
    testData.setField(33, null)
    testData.setField(34, Decimal.castFrom("1514356320000", 38, 0))
    testData.setField(35, "a")
    testData.setField(36, "b")
    testData.setField(37, Array[Byte](1, 2, 3, 4))
    testData.setField(38, "AQIDBA==")
    testData.setField(39, "1世3")
    testData.setField(40, null)
    testData.setField(41, null)
    testData.setField(42, 256.toLong)
    testData.setField(43, -1.toLong)
    testData.setField(44, 256)
    testData.setField(45, UTCTimestamp("1996-11-10 06:55:44.333").toString)
    testData.setField(46, "test1=1,test2=2,test3=3")
    testData.setField(47, null)
    testData.setField(48, false)
    testData.setField(49, Decimal.castFrom("1345.1231231321321321111", 38, 19))
    testData.setField(50, UTCDate("1997-11-11"))
    testData.setField(51, UTCTime("09:44:55"))
    testData.setField(52, UTCTimestamp("1997-11-11 09:44:55.333"))
    testData.setField(53, "hello world".getBytes)
    testData.setField(54, "This is a testing string.".getBytes)
    testData
  }

  override def typeInfo: RowTypeInfo = {
    new RowTypeInfo(
      /* 0 */  Types.STRING,
      /* 1 */  Types.BOOLEAN,
      /* 2 */  Types.BYTE,
      /* 3 */  Types.SHORT,
      /* 4 */  Types.LONG,
      /* 5 */  Types.FLOAT,
      /* 6 */  Types.DOUBLE,
      /* 7 */  Types.INT,
      /* 8 */  Types.STRING,
      /* 9 */  Types.BYTE,
      /* 10 */ Types.SHORT,
      /* 11 */ Types.LONG,
      /* 12 */ Types.FLOAT,
      /* 13 */ Types.DOUBLE,
      /* 14 */ Types.INT,
      /* 15 */ DecimalTypeInfo.of(38, 19),
      /* 16 */ Types.SQL_DATE,
      /* 17 */ Types.SQL_TIME,
      /* 18 */ Types.SQL_TIMESTAMP,
      /* 19 */ TimeIntervalTypeInfo.INTERVAL_MILLIS,
      /* 20 */ TimeIntervalTypeInfo.INTERVAL_MONTHS,
      /* 21 */ Types.BOOLEAN,
      /* 22 */ DecimalTypeInfo.of(38, 19),
      /* 23 */ Types.STRING,
      /* 24 */ Types.STRING,
      /* 25 */ Types.BYTE,
      /* 26 */ Types.SHORT,
      /* 27 */ Types.LONG,
      /* 28 */ Types.FLOAT,
      /* 29 */ Types.DOUBLE,
      /* 30 */ Types.INT,
      /* 31 */ DecimalTypeInfo.of(38, 19),
      /* 32 */ Types.INT,
      /* 33 */ Types.STRING,
      /* 34 */ DecimalTypeInfo.of(19, 0),
      /* 35 */ Types.STRING,
      /* 36 */ Types.STRING,
      /* 37 */ PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
      /* 38 */ Types.STRING,
      /* 39 */ Types.STRING,
      /* 40 */ Types.STRING,
      /* 41 */ DecimalTypeInfo.of(38, 19),
      /* 42 */ Types.LONG,
      /* 43 */ Types.LONG,
      /* 44 */ Types.INT,
      /* 45 */ Types.STRING,
      /* 46 */ Types.STRING,
      /* 47 */ Types.STRING,
      /* 48 */ Types.BOOLEAN,
      /* 49 */ DecimalTypeInfo.of(38, 19),
      /* 50 */ Types.SQL_DATE,
      /* 51 */ Types.SQL_TIME,
      /* 52 */ Types.SQL_TIMESTAMP,
      /* 53 */ Types.PRIMITIVE_ARRAY(Types.BYTE),
      /* 54 */ Types.PRIMITIVE_ARRAY(Types.BYTE))
  }
}
