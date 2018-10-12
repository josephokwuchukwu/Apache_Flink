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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.testtasks.BlockingNoOpInvokable;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Integration test cases for the {@link MiniCluster}.
 */
public class MiniClusterITCase extends TestLogger {

	@Test
	public void runJobWithSingleRpcService() throws Exception {
		final int parallelism = 123;

		final MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
			.setNumTaskManagers(1)
			.setNumSlotsPerTaskManager(parallelism)
			.setRpcServiceSharing(RpcServiceSharing.SHARED)
			.setConfiguration(getDefaultConfiguration())
			.build();

		try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
			miniCluster.start();

			miniCluster.executeJobBlocking(getSimpleJob(parallelism));
		}
	}

	@Test
	public void runJobWithMultipleRpcServices() throws Exception {
		final int parallelism = 123;

		final MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
			.setNumTaskManagers(1)
			.setNumSlotsPerTaskManager(parallelism)
			.setRpcServiceSharing(RpcServiceSharing.DEDICATED)
			.setConfiguration(getDefaultConfiguration())
			.build();

		try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
			miniCluster.start();

			miniCluster.executeJobBlocking(getSimpleJob(parallelism));
		}
	}

	@Test
	public void testHandleJobsWhenNotEnoughSlot() throws Exception {
		final Configuration configuration = getDefaultConfiguration();
		configuration.setLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT, 1000L);

		final MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
			.setNumTaskManagers(1)
			.setNumSlotsPerTaskManager(1)
			.setRpcServiceSharing(RpcServiceSharing.DEDICATED)
			.setConfiguration(configuration)
			.build();

		try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
			miniCluster.start();

			final JobVertex vertex = new JobVertex("Test Vertex");
			vertex.setParallelism(2);
			vertex.setMaxParallelism(2);
			vertex.setInvokableClass(BlockingNoOpInvokable.class);

			final JobGraph graph = new JobGraph("Test Job", vertex);
			graph.setAllowQueuedScheduling(true);
			graph.setScheduleMode(ScheduleMode.EAGER);

			final JobSubmissionResult submissionResult = miniCluster.submitJob(graph).get();
			Assert.assertEquals(graph.getJobID(), submissionResult.getJobID());

			final JobResult jobResult = miniCluster.requestJobResult(graph.getJobID()).get();

			Assert.assertEquals(graph.getJobID(), jobResult.getJobId());
			Assert.assertEquals(ApplicationStatus.FAILED, jobResult.getApplicationStatus());

			Assert.assertTrue(jobResult.getSerializedThrowable().isPresent());

			final Throwable cause = jobResult.getSerializedThrowable().get()
				.deserializeError(getClass().getClassLoader());

			Assert.assertTrue(ExceptionUtils.findThrowable(cause, NoResourceAvailableException.class).isPresent());
			Assert.assertTrue(ExceptionUtils
				.findThrowableWithMessage(cause, "Slots required: 2, slots allocated: 1").isPresent());
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private Configuration getDefaultConfiguration() {
		final Configuration configuration = new Configuration();
		configuration.setInteger(RestOptions.PORT, 0);

		return configuration;
	}

	private static JobGraph getSimpleJob(int parallelism) throws IOException {
		final JobVertex task = new JobVertex("Test task");
		task.setParallelism(parallelism);
		task.setMaxParallelism(parallelism);
		task.setInvokableClass(NoOpInvokable.class);

		final JobGraph jg = new JobGraph(new JobID(), "Test Job", task);
		jg.setAllowQueuedScheduling(true);
		jg.setScheduleMode(ScheduleMode.EAGER);

		final ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000));
		jg.setExecutionConfig(executionConfig);

		return jg;
	}
}
