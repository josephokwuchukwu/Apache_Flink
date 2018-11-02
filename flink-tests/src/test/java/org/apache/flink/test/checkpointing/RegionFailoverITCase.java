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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for region failover with multi regions.
 */
public class RegionFailoverITCase extends TestLogger {

	private static final int FAIL_BASE = 1000;
	private static final int NUM_OF_REGIONS = 3;
	private static final int NUM_OF_RESTARTS = 3;
	private static final int NUM_ELEMENTS = FAIL_BASE * 10;

	private static volatile long lastCompletedCheckpointId = 0;
	private static volatile int numCompletedCheckpoints = 0;
	private static volatile AtomicInteger jobFailedCnt = new AtomicInteger(0);

	private static Map<Long, Integer> snapshotIndicesOfSubTask0 = new HashMap<>();

	private static MiniClusterWithClientResource cluster;

	private static boolean restoredState = false;

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	@Before
	public void setup() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "region");

		cluster = new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder()
				.setConfiguration(configuration)
				.setNumberTaskManagers(2)
				.setNumberSlotsPerTaskManager(2).build());
		cluster.before();
	}

	@AfterClass
	public static void shutDownExistingCluster() {
		if (cluster != null) {
			cluster.after();
			cluster = null;
		}
	}

	/**
	 * Tests that a simple job (Source -> Map) with multi regions could restore with operator state.
	 *
	 * <p>The subtask-0 of Map function would fail {@code NUM_OF_RESTARTS} times, and it will verify whether the restored state is
	 * identical to last completed checkpoint's.
	 */
	@Test(timeout = 10000)
	public void testMultiRegionFailover() {
		try {
			JobGraph jobGraph = createJobGraph();
			ClusterClient<?> client = cluster.getClusterClient();
			client.submitJob(jobGraph, RegionFailoverITCase.class.getClassLoader());
			Assert.assertTrue("The test multi-region job has never ever restored state.", restoredState);
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	private JobGraph createJobGraph() {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(NUM_OF_REGIONS);
		env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		env.disableOperatorChaining();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(NUM_OF_RESTARTS, 0L));
		env.getConfig().disableSysoutLogging();

		// there exists num of 'NUM_OF_REGIONS' individual regions.
		env.addSource(new StringGeneratingSourceFunction(NUM_ELEMENTS, NUM_ELEMENTS / NUM_OF_RESTARTS))
			.setParallelism(NUM_OF_REGIONS)
			.map(new FailingMapperFunction(NUM_OF_RESTARTS))
			.setParallelism(NUM_OF_REGIONS);

		return env.getStreamGraph().getJobGraph();
	}

	private static class StringGeneratingSourceFunction extends RichParallelSourceFunction<Integer>
		implements ListCheckpointed<Integer>, CheckpointListener {
		private static final long serialVersionUID = 1L;

		private final long numElements;
		private final long checkpointLatestAt;

		private int index = -1;

		private volatile boolean isRunning = true;

		StringGeneratingSourceFunction(long numElements, long checkpointLatestAt) {
			this.numElements = numElements;
			this.checkpointLatestAt = checkpointLatestAt;
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			if (index < 0) {
				// not been restored, so initialize
				index = 0;
			}

			while (isRunning && index < numElements) {
				//noinspection SynchronizationOnLocalVariableOrMethodParameter
				synchronized (ctx.getCheckpointLock()) {
					index += 1;
					ctx.collect(index);
				}

				if (numCompletedCheckpoints < 3) {
					// not yet completed enough checkpoints, so slow down
					if (index < checkpointLatestAt) {
						// mild slow down
						Thread.sleep(1);
					} else {
						// wait until the checkpoints are completed
						while (isRunning && numCompletedCheckpoints < 3) {
							Thread.sleep(300);
						}
					}
				}
				if (jobFailedCnt.get() < NUM_OF_RESTARTS) {
					// slow down if job has not failed for 'NUM_OF_RESTARTS' times.
					Thread.sleep(1);
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

		@Override
		public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
			if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
				snapshotIndicesOfSubTask0.put(checkpointId, index);
			}
			return Collections.singletonList(this.index);
		}

		@Override
		public void restoreState(List<Integer> state) throws Exception {
			restoredState = true;
			if (state.isEmpty() || state.size() > 1) {
				throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
			}
			this.index = state.get(0);
			if (index != snapshotIndicesOfSubTask0.get(lastCompletedCheckpointId)) {
				throw new RuntimeException("Test failed due to unexpected recovered index: " + index +
					", while last completed checkpoint record index: " + snapshotIndicesOfSubTask0.get(lastCompletedCheckpointId));
			}
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
				lastCompletedCheckpointId = checkpointId;
				numCompletedCheckpoints++;
			}
		}
	}

	private static class FailingMapperFunction extends RichMapFunction<Integer, Integer> {
		private final int restartTimes;

		FailingMapperFunction(int restartTimes) {
			this.restartTimes = restartTimes;
		}

		@Override
		public Integer map(Integer input) throws Exception {
			if (input > FAIL_BASE * (jobFailedCnt.get() + 1)) {

				if (jobFailedCnt.get() < restartTimes && getRuntimeContext().getIndexOfThisSubtask() == 0) {
					jobFailedCnt.incrementAndGet();
					throw new TestException();
				}
			}
			return input;
		}
	}

	private static class TestException extends IOException{
		private static final long serialVersionUID = 1L;
	}
}
