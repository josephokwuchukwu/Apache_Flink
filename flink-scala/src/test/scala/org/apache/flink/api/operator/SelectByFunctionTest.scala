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
package org.apache.flink.api.operator

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.scala.{SelectByMaxFunction, SelectByMinFunction}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.junit.{Assert, Test}

/**
  *
  */
class SelectByFunctionTest {

  val tupleTypeInfo = new CaseClassTypeInfo(
    classOf[(Int, Long, String, Long, Int)], Array(), Array(BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.LONG_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO),
    Array("_1", "_2","_3","_4","_5")) {

    override def createSerializer(config: ExecutionConfig):
      TypeSerializer[(Int, Long, String, Long, Int)] = ???
  }

  private val bigger  = (10, 100L, "HelloWorld", 200L, 20)
  private val smaller = (10, 100L, "HelloWorld", 200L, 20)

  //Special case where only the last value determines if bigger or smaller
  private val specialCaseBigger  = (10, 100L, "HelloWorld", 200L, 20)
  private val specialCaseSmaller = (10, 100L, "HelloWorld", 200L, 20)

  /**
    * This test validates whether the order of tuples has
    *
    * any impact on the outcome and if the bigger tuple is returned.
    */
  @Test
  def testMaxByComparison(): Unit = {
    val a1 = Array(0)
    val maxByTuple = new SelectByMaxFunction(tupleTypeInfo, a1)
      try {
        Assert.assertSame("SelectByMax must return bigger tuple",
          bigger, maxByTuple.reduce(smaller, bigger))
        Assert.assertSame("SelectByMax must return bigger tuple",
          bigger, maxByTuple.reduce(bigger, smaller))
      } catch {
        case e : Exception =>
          Assert.fail("No exception should be thrown while comapring both tuples")
      }
  }

  // ----------------------- MAXIMUM FUNCTION TEST BELOW --------------------------

  /**
    * This test cases checks when two tuples only differ in one value, but this value is not
    * in the fields list. In that case it should be seen as equal
    * and then the first given tuple (value1) should be returned by reduce().
    */
  @Test
  def testMaxByComparisonSpecialCase1() : Unit = {
    val a1 = Array(0, 3)
    val maxByTuple = new SelectByMaxFunction(tupleTypeInfo, a1)

    try {
      Assert.assertSame("SelectByMax must return the first given tuple",
        specialCaseBigger, maxByTuple.reduce(specialCaseBigger, bigger))
      Assert.assertSame("SelectByMax must return the first given tuple",
        bigger, maxByTuple.reduce(bigger, specialCaseBigger))
    } catch {
      case e : Exception => Assert.fail("No exception should be thrown " +
        "while comapring both tuples")
    }
  }

  /**
    * This test cases checks when two tuples only differ in one value.
    */
  @Test
  def testMaxByComparisonSpecialCase2() : Unit = {
    val a1 = Array(0, 2, 1, 4, 3)
    val maxByTuple = new SelectByMaxFunction(tupleTypeInfo, a1)
    try {
      Assert.assertSame("SelectByMax must return bigger tuple",
        bigger, maxByTuple.reduce(specialCaseBigger, bigger))
      Assert.assertSame("SelectByMax must return bigger tuple",
        bigger, maxByTuple.reduce(bigger, specialCaseBigger))
    } catch {
      case e : Exception => Assert.fail("No exception should be thrown" +
        " while comapring both tuples")
    }
  }

  /**
    * This test validates that equality is independent of the amount of used indices.
    */
  @Test
  def testMaxByComparisonMultiple(): Unit = {
    val a1 = Array(0, 1, 2, 3, 4)
    val maxByTuple = new SelectByMaxFunction(tupleTypeInfo, a1)
    try {
      Assert.assertSame("SelectByMax must return bigger tuple",
        bigger, maxByTuple.reduce(smaller, bigger))
      Assert.assertSame("SelectByMax must return bigger tuple",
        bigger, maxByTuple.reduce(bigger, smaller))
    } catch {
      case e : Exception => Assert.fail("No exception should be thrown " +
        "while comapring both tuples")
    }
  }

  /**
    * Checks whether reduce does behave as expected if both values are the same object.
    */
  @Test
  def testMaxByComparisonMustReturnATuple() : Unit = {
    val a1 = Array(0)
    val maxByTuple = new SelectByMaxFunction(tupleTypeInfo, a1)

    try {
      Assert.assertSame("SelectByMax must return bigger tuple",
        bigger, maxByTuple.reduce(bigger, bigger))
      Assert.assertSame("SelectByMax must return smaller tuple",
        smaller, maxByTuple.reduce(smaller, smaller))
    } catch {
      case e : Exception => Assert.fail("No exception should be thrown" +
        " while comapring both tuples")
    }
  }

  // ----------------------- MINIMUM FUNCTION TEST BELOW --------------------------

  /**
    * This test validates whether the order of tuples has any impact
    * on the outcome and if the smaller tuple is returned.
    */
  @Test
  def testMinByComparison() : Unit = {
    val a1 = Array(0)
    val minByTuple = new SelectByMinFunction(tupleTypeInfo, a1)
    try {
      Assert.assertSame("SelectByMin must return smaller tuple",
        smaller, minByTuple.reduce(smaller, bigger))
      Assert.assertSame("SelectByMin must return smaller tuple",
        smaller, minByTuple.reduce(bigger, smaller))
    } catch {
      case e : Exception => Assert.fail("No exception should be thrown " +
        "while comapring both tuples")
    }
  }

  /**
    * This test cases checks when two tuples only differ in one value, but this value is not
    * in the fields list. In that case it should be seen as equal and
    * then the first given tuple (value1) should be returned by reduce().
    */
  @Test
  def testMinByComparisonSpecialCase1() : Unit = {
    val a1 = Array(0, 3)
    val minByTuple = new SelectByMinFunction(tupleTypeInfo, a1)

    try {
      Assert.assertSame("SelectByMin must return the first given tuple",
        specialCaseBigger, minByTuple.reduce(specialCaseBigger, bigger))
      Assert.assertSame("SelectByMin must return the first given tuple",
        bigger, minByTuple.reduce(bigger, specialCaseBigger))
    } catch {
      case e : Exception => Assert.fail("No exception should be thrown " +
        "while comapring both tuples")
    }
  }

  /**
    * This test validates that when two tuples only differ in one value
    * and that value's index is given at construction time. The smaller tuple must be returned
    * then.
    */
  @Test
  def  testMinByComparisonSpecialCase2() : Unit = {
    val a1 = Array(0, 2, 1, 4, 3)
    val minByTuple = new SelectByMinFunction(tupleTypeInfo, a1)

    try {
      Assert.assertSame("SelectByMin must return smaller tuple",
        smaller, minByTuple.reduce(specialCaseSmaller, smaller))
      Assert.assertSame("SelectByMin must return smaller tuple",
        smaller, minByTuple.reduce(smaller, specialCaseSmaller))
    } catch {
      case e : Exception => Assert.fail("No exception should be thrown" +
        " while comapring both tuples")
    }
  }

  /**
    * Checks whether reduce does behave as expected if both values are the same object.
    */
  @Test
  def testMinByComparisonMultiple() : Unit =  {
    val a1 = Array(0, 1, 2, 3, 4)
    val minByTuple = new SelectByMinFunction(tupleTypeInfo, a1)

    try {
      Assert.assertSame("SelectByMin must return smaller tuple",
        smaller, minByTuple.reduce(smaller, bigger))
      Assert.assertSame("SelectByMin must return smaller tuple",
        smaller, minByTuple.reduce(bigger, smaller))
    } catch {
      case e : Exception => Assert.fail("No exception should be thrown" +
        " while comapring both tuples")
    }
  }
}
