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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobType;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.io.network.api.reader.RecordReader;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.messages.JobManagerMessages.CancellationSuccess;
import org.apache.flink.runtime.messages.JobManagerMessages.CurrentJobStatus;
import org.apache.flink.runtime.messages.JobManagerMessages.JobNotFound;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.JobManagerActorTestUtils;
import org.apache.flink.types.IntValue;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.flink.runtime.messages.JobManagerMessages.CancelJob;
import static org.apache.flink.runtime.messages.JobManagerMessages.CancellationFailure;
import static org.apache.flink.runtime.messages.JobManagerMessages.RequestJobStatus;

public class TaskCancelTest {

	@Test
	public void testCancelUnion() throws Exception {
		// Test config
		int numberOfSources = 8;
		int sourceParallelism = 4;

		TestingCluster flink = null;

		try {
			// Start a cluster for the given test config
			final Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 2);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, sourceParallelism);
			config.setString(ConfigConstants.AKKA_ASK_TIMEOUT, TestingUtils.DEFAULT_AKKA_ASK_TIMEOUT());
			config.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SEGMENT_SIZE_KEY, 4096);
			config.setInteger(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, 2048);

			flink = new TestingCluster(config, false);
			flink.start();

			// Setup
			final JobGraph jobGraph = new JobGraph("Cancel Big Union", JobType.BATCHING);

			JobVertex[] sources = new JobVertex[numberOfSources];
			SlotSharingGroup group = new SlotSharingGroup();

			// Create multiple sources
			for (int i = 0; i < sources.length; i++) {
				sources[i] = new JobVertex("Source " + i);
				sources[i].setInvokableClass(InfiniteSource.class);
				sources[i].setParallelism(sourceParallelism);
				sources[i].setSlotSharingGroup(group);

				jobGraph.addVertex(sources[i]);
				group.addVertexToGroup(sources[i].getID());
			}

			// Union all sources
			JobVertex union = new JobVertex("Union");
			union.setInvokableClass(AgnosticUnion.class);
			union.setParallelism(sourceParallelism);

			jobGraph.addVertex(union);

			// Each source creates a separate result
			for (JobVertex source : sources) {
				union.connectNewDataSetAsInput(
						source,
						DistributionPattern.POINTWISE,
						ResultPartitionType.PIPELINED);
			}

			// run the job
			flink.submitJobDetached(jobGraph);

			// Wait for the job to make some progress and then cancel
			awaitRunning(
					flink.getLeaderGateway(TestingUtils.TESTING_DURATION()),
					jobGraph.getJobID(),
					TestingUtils.TESTING_DURATION());

			Thread.sleep(5000);

			cancelJob(
					flink.getLeaderGateway(TestingUtils.TESTING_DURATION()),
					jobGraph.getJobID(),
					TestingUtils.TESTING_DURATION());

			// Wait for the job to be cancelled
			JobManagerActorTestUtils.waitForJobStatus(jobGraph.getJobID(), JobStatus.CANCELED,
					flink.getLeaderGateway(TestingUtils.TESTING_DURATION()),
					TestingUtils.TESTING_DURATION());
		}
		finally {
			if (flink != null) {
				flink.shutdown();
			}
		}
	}

	// ---------------------------------------------------------------------------------------------

	/**
	 * Requests the {@link JobManager} to cancel a running job.
	 *
	 * @param jobManager The JobManager actor.
	 * @param jobId The JobID of the job to cancel.
	 * @param timeout Duration in which the JobManager must have responded.
	 */
	public static void cancelJob(ActorGateway jobManager, JobID jobId, FiniteDuration timeout)
			throws Exception {

		checkNotNull(jobManager);
		checkNotNull(jobId);
		checkNotNull(timeout);

		Future<Object> ask = jobManager.ask(new CancelJob(jobId), timeout);

		Object result = Await.result(ask, timeout);

		if (result instanceof CancellationSuccess) {
			// Success
			CancellationSuccess success = (CancellationSuccess) result;

			if (!success.jobID().equals(jobId)) {
				throw new Exception("JobManager responded for wrong job ID. Request: "
						+ jobId + ", response: " + success.jobID() + ".");
			}
		}
		else if (result instanceof CancellationFailure) {
			// Failure
			CancellationFailure failure = (CancellationFailure) result;

			throw new Exception("Failed to cancel job with ID " + failure.jobID() + ".",
					failure.cause());
		}
		else {
			throw new Exception("Unexpected response to cancel request: " + result);
		}
	}

	public static void awaitRunning(ActorGateway jobManager, JobID jobId, FiniteDuration timeout)
			throws Exception {

		checkNotNull(jobManager);
		checkNotNull(jobId);
		checkNotNull(timeout);

		while (true) {
			Future<Object> ask = jobManager.ask(
					new RequestJobStatus(jobId),
					timeout);

			Object result = Await.result(ask, timeout);

			if (result instanceof CurrentJobStatus) {
				// Success
				CurrentJobStatus status = (CurrentJobStatus) result;

				if (!status.jobID().equals(jobId)) {
					throw new Exception("JobManager responded for wrong job ID. Request: "
							+ jobId + ", response: " + status.jobID() + ".");
				}

				if (status.status() == JobStatus.RUNNING) {
					return;
				}
				else if (status.status().isTerminalState()) {
					throw new Exception("JobStatus changed to " + status.status()
							+ " while waiting for job to start running.");
				}
			}
			else if (result instanceof JobNotFound) {
				// Not found
				throw new Exception("Cannot find job with ID " + jobId + ".");
			}
			else {
				throw new Exception("Unexpected response to cancel request: " + result);
			}
		}

	}

	// ---------------------------------------------------------------------------------------------

	public static class InfiniteSource extends AbstractInvokable {

		@Override
		public void invoke() throws Exception {
			RecordWriter<IntValue> writer = new RecordWriter<>(getEnvironment().getWriter(0));

			final IntValue val = new IntValue();

			try {
				for (int i = 0; true; i++) {
					if (Thread.interrupted()) {
						return;
					}

					val.setValue(i);
					writer.emit(val);
				}
			}
			finally {
				writer.clearBuffers();
			}
		}
	}

	public static class AgnosticUnion extends AbstractInvokable {

		@Override
		public void invoke() throws Exception {
			UnionInputGate union = new UnionInputGate(getEnvironment().getAllInputGates());
			RecordReader<IntValue> reader = new RecordReader<>(union, IntValue.class);

			while (reader.next() != null) {
			}
		}
	}
}
