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
package org.apache.flink.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.metrics.reporter.Listener;
import org.apache.flink.metrics.util.TestReporter;
import org.junit.Assert;
import org.junit.Test;

public class MetricGroupTest {
	/**
	 * Verifies that group methods instantiate the correct metric with the given name.
	 */
	@Test
	public void testMetricInstantiation() {
		Configuration config = new Configuration();

		config.setString(MetricRegistry.KEY_METRICS_REPORTER_CLASS, TestReporter1.class.getName());

		MetricGroup root = new TaskManagerMetricGroup(new MetricRegistry(config), "host", "id");

		root.counter("counter");
		Assert.assertTrue(TestReporter1.lastPassedMetric instanceof Counter);
		Assert.assertEquals("counter", TestReporter1.lastPassedName);

		root.meter("meter");
		Assert.assertTrue(TestReporter1.lastPassedMetric instanceof Meter);
		Assert.assertEquals("meter", TestReporter1.lastPassedName);

		root.timer("timer");
		Assert.assertTrue(TestReporter1.lastPassedMetric instanceof Timer);
		Assert.assertEquals("timer", TestReporter1.lastPassedName);

		root.gauge("gauge", new Gauge<Object>() {
			@Override
			public Object getValue() {
				return null;
			}
		});
		Assert.assertTrue(TestReporter1.lastPassedMetric instanceof Gauge);
		Assert.assertEquals("gauge", TestReporter1.lastPassedName);

		root.histogram("histogram");
		Assert.assertTrue(TestReporter1.lastPassedMetric instanceof Histogram);
		Assert.assertEquals("histogram", TestReporter1.lastPassedName);
	}

	protected static class TestReporter1 extends TestReporter implements Listener {
		public static Metric lastPassedMetric;
		public static String lastPassedName;

		@Override
		public void notifyOfAddedMetric(Metric metric, String name) {
			lastPassedMetric = metric;
			lastPassedName = name;
		}

		@Override
		public void notifyOfRemovedMetric(Metric metric, String name) {
		}
	}

	/**
	 * Verifies that metric names containing special characters are rejected.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testInvalidMetricName() {
		Configuration config = new Configuration();

		MetricGroup root = new TaskManagerMetricGroup(new MetricRegistry(config), "host", "id");
		root.counter("=)(/!");
	}

	/**
	 * Verifies that when attempting to create a group with the name of an existing one the existing one will be returned instead.
	 */
	@Test
	public void testDuplicateGroupName() {
		Configuration config = new Configuration();

		MetricGroup root = new TaskManagerMetricGroup(new MetricRegistry(config), "host", "id");

		MetricGroup group1 = root.addGroup("group");
		MetricGroup group2 = root.addGroup("group");
		MetricGroup group3 = root.addGroup("group");
		Assert.assertTrue(group1 == group2 && group2 == group3);

	}
}
