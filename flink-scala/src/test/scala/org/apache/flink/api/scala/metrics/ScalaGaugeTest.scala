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

package org.apache.flink.api.scala.metrics

import org.apache.flink.metrics.Gauge
import org.apache.flink.runtime.metrics.{MetricRegistry, MetricRegistryConfiguration}
import org.apache.flink.runtime.metrics.groups.GenericMetricGroup
import org.apache.flink.util.TestLogger
import org.junit.Test
import org.scalatest.junit.JUnitSuiteLike

class ScalaGaugeTest extends TestLogger with JUnitSuiteLike {

  @Test
  def testGaugeCorrectValue(): Unit = {
    val myGauge = new ScalaGauge[Long](4)
    assert( myGauge.getValue == 4 )
  }

  @Test
  def testRegister(): Unit = {
    val conf = MetricRegistryConfiguration.defaultMetricRegistryConfiguration()
    val registry = new MetricRegistry(conf)
    val metricGroup = new GenericMetricGroup(registry, null, "world")

    val myGauge = new ScalaGauge[Long](4)
    val gauge = metricGroup.gauge[Long, Gauge[Long]]("max", myGauge);

    assert( gauge == myGauge )
  }

}
