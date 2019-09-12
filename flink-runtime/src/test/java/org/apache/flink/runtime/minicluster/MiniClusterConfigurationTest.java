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

package org.apache.flink.runtime.minicluster;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link MiniClusterConfiguration}.
 */
public class MiniClusterConfigurationTest extends TestLogger {

	private static final String TEST_SCHEDULER_NAME = "test-scheduler";

	@Test
	public void testSchedulerType_setViaSystemProperty() {
		System.setProperty(MiniClusterConfiguration.SCHEDULER_TYPE_KEY, TEST_SCHEDULER_NAME);
		final MiniClusterConfiguration miniClusterConfiguration = new MiniClusterConfiguration.Builder().build();

		Assert.assertEquals(
			TEST_SCHEDULER_NAME,
			miniClusterConfiguration.getConfiguration().getString(JobManagerOptions.SCHEDULER));
	}

	@Test
	public void testSchedulerType_notOverriddenIfExistingInConfig() {
		final Configuration config = new Configuration();
		config.setString(JobManagerOptions.SCHEDULER, JobManagerOptions.SCHEDULER.defaultValue());

		System.setProperty(MiniClusterConfiguration.SCHEDULER_TYPE_KEY, TEST_SCHEDULER_NAME);
		final MiniClusterConfiguration miniClusterConfiguration = new MiniClusterConfiguration.Builder()
			.setConfiguration(config)
			.build();

		Assert.assertEquals(
			JobManagerOptions.SCHEDULER.defaultValue(),
			miniClusterConfiguration.getConfiguration().getString(JobManagerOptions.SCHEDULER));
	}
}
