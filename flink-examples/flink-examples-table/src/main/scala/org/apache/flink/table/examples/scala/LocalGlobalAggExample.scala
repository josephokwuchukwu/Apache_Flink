package org.apache.flink.table.examples.scala

import java.util.Date

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, _}
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, _}
import org.apache.flink.types.Row

/**
  * @author peibin.wpb
  * @date 2020-03-09 16:26
  */
object LocalGlobalAggExample {

  class SimpleSource[T](items: Array[T], delay: Int, interval: Int, name: String) extends RichSourceFunction[T] {
    var running = true

    override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
      Thread.sleep(delay * 1000)
      var index = 0
      while (running) {
        val item = items(index % items.length)
        ctx.collect(item)
        index = index + 1
        Thread.sleep(interval * 1000)
      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = { // use blink planner in streaming mode
      val settings = EnvironmentSettings.newInstance()
        .useBlinkPlanner()
        .inStreamingMode()
        .build()
      StreamTableEnvironment.create(env, settings)
    }

    val configuration = tEnv.getConfig().getConfiguration()
    // set low-level key-value options
    configuration.setString("table.exec.mini-batch.enabled", "true") // local-global aggregation depends on mini-batch is enabled
    configuration.setString("table.exec.mini-batch.allow-latency", "5 s")
    configuration.setString("table.exec.mini-batch.size", "5000")
    configuration.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE") // enable two-phase, i.e. local-global aggregation

    val a = env.addSource(new SimpleSource(
      Array(
        Event("b", 1, 1),
        Event("c", 2, 2),
        Event("b", 2, 2),
        Event("b", 3, 3),
        Event("d", 1, 3)
      ), 1, 2, "A"
    ))

    tEnv.createTemporaryView("a", a, 'k, 'v, 'serial_no)

    val createViewFirstAgg =
      s"""
         | select k, sum(v) as v, last_value(v) as last_v
         | from a
         | group by k
        """.stripMargin
    val viewFirstAgg = tEnv.sqlQuery(createViewFirstAgg)
    viewFirstAgg.toRetractStream[Row].addSink(x => println((new Date, "result", x)))
    println(env.getExecutionPlan)
    env.execute()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class Event(k: String, v: Int, serial_no: Int)

}
