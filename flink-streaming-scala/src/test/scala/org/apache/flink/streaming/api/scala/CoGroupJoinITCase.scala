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

package org.apache.flink.streaming.api.scala

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingTimeWindows, TumblingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.junit.Test
import org.junit.Assert._

import scala.collection.mutable

class CoGroupJoinITCase extends StreamingMultipleProgramsTestBase {

  @Test
  def testCoGroup(): Unit = {
    CoGroupJoinITCase.testResults = mutable.MutableList()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val source1 = env.addSource(new SourceFunction[(String, Int)]() {
      def run(ctx: SourceFunction.SourceContext[(String, Int)]) {
        ctx.collect(("a", 0))
        ctx.collect(("a", 1))
        ctx.collect(("a", 2))
        ctx.collect(("b", 3))
        ctx.collect(("b", 4))
        ctx.collect(("b", 5))
        ctx.collect(("a", 6))
        ctx.collect(("a", 7))
        ctx.collect(("a", 8))
      }

      def cancel() {}
      
    }).assignTimestampsAndWatermarks(new CoGroupJoinITCase.Tuple2TimestampExtractor)

    val source2 = env.addSource(new SourceFunction[(String, Int)]() {
      def run(ctx: SourceFunction.SourceContext[(String, Int)]) {
        ctx.collect(("a", 0))
        ctx.collect(("a", 1))
        ctx.collect(("b", 3))
        ctx.collect(("c", 6))
        ctx.collect(("c", 7))
        ctx.collect(("c", 8))
      }

      def cancel() {
      }
    }).assignTimestampsAndWatermarks(new CoGroupJoinITCase.Tuple2TimestampExtractor)

    source1.coGroup(source2)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingTimeWindows.of(Time.of(3, TimeUnit.MILLISECONDS)))
      .apply { (first: Iterator[(String, Int)], second: Iterator[(String, Int)]) =>
          "F:" + first.mkString("") + " S:" + second.mkString("")
      }
      .addSink(new SinkFunction[String]() {
        def invoke(value: String) {
          CoGroupJoinITCase.testResults += value
        }
      })

    env.execute("CoGroup Test")

    val expectedResult = mutable.MutableList(
      "F:(a,0)(a,1)(a,2) S:(a,0)(a,1)",
      "F:(b,3)(b,4)(b,5) S:(b,3)",
      "F:(a,6)(a,7)(a,8) S:",
      "F: S:(c,6)(c,7)(c,8)")

    assertEquals(expectedResult.sorted, CoGroupJoinITCase.testResults.sorted)
  }

  @Test
  def testJoin(): Unit = {
    CoGroupJoinITCase.testResults = mutable.MutableList()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val source1 = env.addSource(new SourceFunction[(String, String, Int)]() {
      def run(ctx: SourceFunction.SourceContext[(String, String, Int)]) {
        ctx.collect(("a", "x", 0))
        ctx.collect(("a", "y", 1))
        ctx.collect(("a", "z", 2))

        ctx.collect(("b", "u", 3))
        ctx.collect(("b", "w", 5))

        ctx.collect(("a", "i", 6))
        ctx.collect(("a", "j", 7))
        ctx.collect(("a", "k", 8))
      }

      def cancel() {}
      
    }).assignTimestampsAndWatermarks(new CoGroupJoinITCase.Tuple3TimestampExtractor)

    val source2 = env.addSource(new SourceFunction[(String, String, Int)]() {
      def run(ctx: SourceFunction.SourceContext[(String, String, Int)]) {
        ctx.collect(("a", "u", 0))
        ctx.collect(("a", "w", 1))

        ctx.collect(("b", "i", 3))
        ctx.collect(("b", "k", 5))

        ctx.collect(("a", "x", 6))
        ctx.collect(("a", "z", 8))
      }

      def cancel() {}
      
    }).assignTimestampsAndWatermarks(new CoGroupJoinITCase.Tuple3TimestampExtractor)

    source1.join(source2)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingTimeWindows.of(Time.of(3, TimeUnit.MILLISECONDS)))
      .apply( (l, r) => l.toString + ":" + r.toString)
      .addSink(new SinkFunction[String]() {
        def invoke(value: String) {
          CoGroupJoinITCase.testResults += value
        }
      })

    env.execute("Join Test")

    val expectedResult = mutable.MutableList(
      "(a,x,0):(a,u,0)",
      "(a,x,0):(a,w,1)",
      "(a,y,1):(a,u,0)",
      "(a,y,1):(a,w,1)",
      "(a,z,2):(a,u,0)",
      "(a,z,2):(a,w,1)",
      "(b,u,3):(b,i,3)",
      "(b,u,3):(b,k,5)",
      "(b,w,5):(b,i,3)",
      "(b,w,5):(b,k,5)",
      "(a,i,6):(a,x,6)",
      "(a,i,6):(a,z,8)",
      "(a,j,7):(a,x,6)",
      "(a,j,7):(a,z,8)",
      "(a,k,8):(a,x,6)",
      "(a,k,8):(a,z,8)")

    assertEquals(expectedResult.sorted, CoGroupJoinITCase.testResults.sorted)
  }

  @Test
  def test2WindowsJoin(): Unit = {
    CoGroupJoinITCase.testResults = mutable.MutableList()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val source1 = env.addSource(new SourceFunction[(String, String, Int)]() {
      def run(ctx: SourceFunction.SourceContext[(String, String, Int)]) {
        ctx.collect(("a", "x", 0))
        ctx.collect(("b", "y", 1))
        ctx.collect(("c", "z", 2))

        ctx.collect(("d", "u", 3))
        ctx.collect(("e", "u", 4))
        ctx.collect(("f", "w", 5))

        ctx.collect(("h", "j", 6))
        ctx.collect(("g", "i", 7))
        ctx.collect(("i", "k", 8))
        ctx.collect(("j", "k", 9))
        ctx.collect(("k", "k", 10))

      }

      def cancel() {}

    }).assignTimestampsAndWatermarks(new CoGroupJoinITCase.Tuple3TimestampExtractor)

    val source2 = env.addSource(new SourceFunction[(String, String, Int)]() {
      def run(ctx: SourceFunction.SourceContext[(String, String, Int)]) {
        ctx.collect(("a", "u", 0))
        ctx.collect(("e", "w", 1))

        ctx.collect(("g", "i", 3))
        ctx.collect(("a", "i", 3))
        ctx.collect(("d", "i", 4))
        ctx.collect(("b", "k", 5))

        ctx.collect(("c", "x", 6))
        ctx.collect(("f", "x", 6))
        ctx.collect(("h", "x", 6))
        ctx.collect(("k", "z", 8))
        ctx.collect(("j", "z", 9))
        ctx.collect(("i", "z", 10))

      }

      def cancel() {}

    }).assignTimestampsAndWatermarks(new CoGroupJoinITCase.Tuple3TimestampExtractor)

    source1.timeJoin(source2)
      .where(_._1)
      .window(SlidingTimeWindows.of(
        Time.of(6, TimeUnit.MILLISECONDS), Time.of(2, TimeUnit.MILLISECONDS)))
      .equalTo(_._1)
      .window(TumblingTimeWindows.of(Time.of(2, TimeUnit.MILLISECONDS)))
      .apply((l, r) => l.toString + ":" + r.toString)
      .addSink(new SinkFunction[String]() {
        def invoke(value: String) {
          CoGroupJoinITCase.testResults += value
        }
      })

    env.execute("Join Test")

    val expectedResult = mutable.MutableList(
      "(a,x,0):(a,i,3)",
      "(a,x,0):(a,u,0)",
      "(b,y,1):(b,k,5)",
      "(c,z,2):(c,x,6)",
      "(d,u,3):(d,i,4)",
      "(f,w,5):(f,x,6)",
      "(h,j,6):(h,x,6)",
      "(i,k,8):(i,z,10)",
      "(j,k,9):(j,z,9)")

    assertEquals(expectedResult.sorted, CoGroupJoinITCase.testResults.sorted)
  }

  @Test
  def testSelfJoin(): Unit = {
    CoGroupJoinITCase.testResults = mutable.MutableList()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val source1 = env.addSource(new SourceFunction[(String, String, Int)]() {
      def run(ctx: SourceFunction.SourceContext[(String, String, Int)]) {
        ctx.collect(("a", "x", 0))
        ctx.collect(("a", "y", 1))
        ctx.collect(("a", "z", 2))

        ctx.collect(("b", "u", 3))
        ctx.collect(("b", "w", 5))

        ctx.collect(("a", "i", 6))
        ctx.collect(("a", "j", 7))
        ctx.collect(("a", "k", 8))
      }

      def cancel() {}
      
    }).assignTimestampsAndWatermarks(new CoGroupJoinITCase.Tuple3TimestampExtractor)

    source1.join(source1)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingTimeWindows.of(Time.of(3, TimeUnit.MILLISECONDS)))
      .apply( (l, r) => l.toString + ":" + r.toString)
      .addSink(new SinkFunction[String]() {
      def invoke(value: String) {
        CoGroupJoinITCase.testResults += value
      }
    })

    env.execute("Self-Join Test")

    val expectedResult = mutable.MutableList(
      "(a,x,0):(a,x,0)",
      "(a,x,0):(a,y,1)",
      "(a,x,0):(a,z,2)",
      "(a,y,1):(a,x,0)",
      "(a,y,1):(a,y,1)",
      "(a,y,1):(a,z,2)",
      "(a,z,2):(a,x,0)",
      "(a,z,2):(a,y,1)",
      "(a,z,2):(a,z,2)",
      "(b,u,3):(b,u,3)",
      "(b,u,3):(b,w,5)",
      "(b,w,5):(b,u,3)",
      "(b,w,5):(b,w,5)",
      "(a,i,6):(a,i,6)",
      "(a,i,6):(a,j,7)",
      "(a,i,6):(a,k,8)",
      "(a,j,7):(a,i,6)",
      "(a,j,7):(a,j,7)",
      "(a,j,7):(a,k,8)",
      "(a,k,8):(a,i,6)",
      "(a,k,8):(a,j,7)",
      "(a,k,8):(a,k,8)")

    assertEquals(expectedResult.sorted, CoGroupJoinITCase.testResults.sorted)
  }

}


object CoGroupJoinITCase {
  private var testResults: mutable.MutableList[String] = null

  private class Tuple2TimestampExtractor extends AssignerWithPunctuatedWatermarks[(String, Int)] {
    
    override def extractTimestamp(element: (String, Int), previousTimestamp: Long): Long = {
      element._2
    }

    override def checkAndGetNextWatermark(
        lastElement: (String, Int),
        extractedTimestamp: Long): Long = extractedTimestamp - 1
  }

  private class Tuple3TimestampExtractor extends 
        AssignerWithPunctuatedWatermarks[(String, String, Int)] {
    
    override def extractTimestamp(element: (String, String, Int), previousTimestamp: Long): Long
         = element._3

    override def checkAndGetNextWatermark(
        lastElement: (String, String, Int),
        extractedTimestamp: Long): Long = extractedTimestamp - 1
  }
}
