/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;

import org.junit.Assert;
import org.junit.Test;
import org.rocksdb.ColumnFamilyHandle;

import javax.annotation.Nullable;

import java.math.BigInteger;
import java.util.ArrayList;

/**
 * validate native metric monitor.
 */
public class RocksDBNativeMetricMonitorTest {

	private static final String OPERATOR_NAME = "dummy";

	private static final String COLUMN_FAMILY_NAME = "column-family";

	private static final BigInteger ZERO = new BigInteger(1, new byte[]{0, 0, 0, 0});

	@Test
	public void testMetricMonitorLifecycle() throws Throwable {
		//We use a local variable here to manually control the life-cycle.
		// This allows us to verify that metrics do not try to access
		// RocksDB after the monitor was closed.
		RocksDBResource rocksDBResource = new RocksDBResource();
		rocksDBResource.before();

		SimpleMetricRegistry registry = new SimpleMetricRegistry();
		OperatorMetricGroup group = new OperatorMetricGroup(
			registry,
			UnregisteredMetricGroups.createUnregisteredTaskMetricGroup(),
			new OperatorID(),
			OPERATOR_NAME
		);

		RocksDBNativeMetricOptions options = new RocksDBNativeMetricOptions();
		// always returns a non-zero
		// value since empty memtables
		// have overhead.
		options.enableSizeAllMemTables();

		RocksDBNativeMetricMonitor monitor = new RocksDBNativeMetricMonitor(
			rocksDBResource.getRocksDB(),
			options,
			group
		);

		ColumnFamilyHandle handle = rocksDBResource.createNewColumnFamily(COLUMN_FAMILY_NAME);
		monitor.registerColumnFamily(COLUMN_FAMILY_NAME, handle);

		Assert.assertEquals("Failed to register metrics for column family", 1, registry.metrics.size());

		RocksDBNativeMetricMonitor.RocksDBNativeMetricView view = registry.metrics.get(0);

		view.update();

		Assert.assertNotEquals("Failed to pull metric from RocksDB", ZERO, view.getValue());

		view.setValue(0L);

		//After the monitor is closed no metric should be accessing RocksDB anymore.
		//If they do, then this test will likely fail with a segmentation fault.
		monitor.close();

		rocksDBResource.after();

		view.update();

		Assert.assertEquals("Failed to release RocksDB reference", ZERO, view.getValue());
	}

	@Test
	public void testReturnsUnsigned() throws Throwable {
		SimpleMetricRegistry registry = new SimpleMetricRegistry();
		OperatorMetricGroup group = new OperatorMetricGroup(
			registry,
			UnregisteredMetricGroups.createUnregisteredTaskMetricGroup(),
			new OperatorID(),
			OPERATOR_NAME
		);

		RocksDBNativeMetricOptions options = new RocksDBNativeMetricOptions();
		options.enableSizeAllMemTables();

		RocksDBNativeMetricMonitor monitor = new RocksDBNativeMetricMonitor(
			null,
			options,
			group
		);

		monitor.registerColumnFamily(COLUMN_FAMILY_NAME, null);
		RocksDBNativeMetricMonitor.RocksDBNativeMetricView view = registry.metrics.get(0);

		view.setValue(-1);
		BigInteger result = view.getValue();

		Assert.assertEquals("Failed to interpret RocksDB result as an unsigned long", 1, result.signum());
	}

	static class SimpleMetricRegistry implements MetricRegistry {
		ArrayList<RocksDBNativeMetricMonitor.RocksDBNativeMetricView> metrics = new ArrayList<>();

		@Override
		public char getDelimiter() {
			return 0;
		}

		@Override
		public char getDelimiter(int index) {
			return 0;
		}

		@Override
		public int getNumberReporters() {
			return 0;
		}

		@Override
		public void register(Metric metric, String metricName, AbstractMetricGroup group) {
			if (metric instanceof RocksDBNativeMetricMonitor.RocksDBNativeMetricView) {
				metrics.add((RocksDBNativeMetricMonitor.RocksDBNativeMetricView) metric);
			}
		}

		@Override
		public void unregister(Metric metric, String metricName, AbstractMetricGroup group) {

		}

		@Override
		public ScopeFormats getScopeFormats() {
			Configuration config = new Configuration();

			config.setString(MetricOptions.SCOPE_NAMING_TM, "A");
			config.setString(MetricOptions.SCOPE_NAMING_TM_JOB, "B");
			config.setString(MetricOptions.SCOPE_NAMING_TASK, "C");
			config.setString(MetricOptions.SCOPE_NAMING_OPERATOR, "D");

			return ScopeFormats.fromConfig(config);
		}

		@Nullable
		@Override
		public String getMetricQueryServicePath() {
			return null;
		}
	}
}
