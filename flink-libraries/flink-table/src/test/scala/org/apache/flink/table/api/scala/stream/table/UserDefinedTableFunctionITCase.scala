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
package org.apache.flink.table.api.scala.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.batch.utils.UDFTestUtils
import org.apache.flink.table.api.scala.stream.utils.{StreamITCase, StreamTestData}
import org.apache.flink.table.expressions.utils.RichFunc2
import org.apache.flink.table.utils.{RichTableFunc0, RichTableFunc1}
import org.apache.flink.types.Row
import org.junit.{Before, Test}

import scala.collection.JavaConverters._
import scala.collection.mutable

class UserDefinedTableFunctionITCase extends StreamingMultipleProgramsTestBase {

  @Before
  def setup(): Unit = {
    StreamITCase.clear
  }

  @Test
  def testOpenClose(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val tableFunc0 = new RichTableFunc0
    tEnv.registerFunction("RichTableFunc0", tableFunc0)

    val result = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .join(tableFunc0('c) as 's)
      .select('a, 's)

    val results = result.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,Hi",
      "2,Hello",
      "3,Hello world",
      "4,Hello world, how are you?",
      "5,I am fine.",
      "6,Luke Skywalker")
    StreamITCase.compareWithList(expected.asJava)
  }

  @Test
  def testSingleUDTFWithParameter(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val tableFunc1 = new RichTableFunc1
    tEnv.registerFunction("RichTableFunc1", tableFunc1)
    UDFTestUtils.setJobParameters(env, Map("word_separator" -> " "))

    val result = StreamTestData.getSmall3TupleDataStream(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .join(tableFunc1('c) as 's)
      .select('a, 's)

    val results = result.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("3,Hello", "3,world")
    StreamITCase.compareWithList(expected.asJava)
  }

  @Test
  def testUDTFWithUDF(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val tableFunc1 = new RichTableFunc1
    tEnv.registerFunction("RichTableFunc1", tableFunc1)
    tEnv.registerFunction("RichFunc2", RichFunc2)
    UDFTestUtils.setJobParameters(env, Map("word_separator" -> "#", "string.value" -> "test"))

    val sqlQuery = "SELECT a, s FROM t1, LATERAL TABLE(RichTableFunc1(RichFunc2(c))) as T(s)"

    val result = StreamTestData.getSmall3TupleDataStream(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .join(tableFunc1(RichFunc2('c)) as 's)
      .select('a, 's)

    val results = result.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,Hi",
      "1,test",
      "2,Hello",
      "2,test",
      "3,Hello world",
      "3,test")
    StreamITCase.compareWithList(expected.asJava)
  }

  @Test
  def testMultiUDTFs(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val tableFunc0 = new RichTableFunc0
    val tableFunc1 = new RichTableFunc1
    tEnv.registerFunction("RichTableFunc0", new RichTableFunc0)
    tEnv.registerFunction("RichTableFunc1", new RichTableFunc1)
    UDFTestUtils.setJobParameters(env, Map("word_separator" -> " "))

    val result = StreamTestData.getSmall3TupleDataStream(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .join(tableFunc0('c) as 's)
      .join(tableFunc1('c) as 'x)
      .select('a, 's, 'x)

    val results = result.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("3,Hello world,Hello", "3,Hello world,world")
    StreamITCase.compareWithList(expected.asJava)
  }

}
