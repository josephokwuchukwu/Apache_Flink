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

package org.apache.flink.api.scala.batch.table

import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase
import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.api.table.{Row, TableEnvironment, ValidationException}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.junit._

class SortValidationTest(
  mode: TestExecutionMode,
  configMode: TableConfigMode)
  extends TableProgramsTestBase(mode, configMode) {

  def getExecutionEnvironment = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env
  }

  @Test(expected = classOf[ValidationException])
  def testFetchWithoutOrder(): Unit = {
    val env = getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val ds = CollectionDataSets.get3TupleDataSet(env)
    val t = ds.toTable(tEnv).limit(0, 5)

    t.toDataSet[Row].collect()
  }

}
