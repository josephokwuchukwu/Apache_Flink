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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.FallbackLibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.netty.PartitionProducerStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.UnregisteredTaskMetricsGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TaskStateHandles;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.util.SerializedValue;
import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This test checks that task restores that get stuck in the presence of interrupts
 * are handled properly.
 *
 * In practice, reading from HDFS is interrupt sensitive: The HDFS code frequently deadlocks
 * or livelocks if it is interrupted.
 */
public class InterruptSensitiveRestoreTest {

	private static final OneShotLatch IN_RESTORE_LATCH = new OneShotLatch();

	private static final int OPERATOR_MANAGED = 0;
	private static final int OPERATOR_RAW = 1;
	private static final int KEYED_MANAGED = 2;
	private static final int KEYED_RAW = 3;
	private static final int LEGACY = 4;

	@Test
	public void testRestoreWithInterruptLegacy() throws Exception {
		testRestoreWithInterrupt(LEGACY);
	}

	@Test
	public void testRestoreWithInterruptOperatorManaged() throws Exception {
		testRestoreWithInterrupt(OPERATOR_MANAGED);
	}

	@Test
	public void testRestoreWithInterruptOperatorRaw() throws Exception {
		testRestoreWithInterrupt(OPERATOR_RAW);
	}

	@Test
	public void testRestoreWithInterruptKeyedManaged() throws Exception {
		testRestoreWithInterrupt(KEYED_MANAGED);
	}

	@Test
	public void testRestoreWithInterruptKeyedRaw() throws Exception {
		testRestoreWithInterrupt(KEYED_RAW);
	}

	private void testRestoreWithInterrupt(int mode) throws Exception {

		IN_RESTORE_LATCH.reset();
		Configuration taskConfig = new Configuration();
		StreamConfig cfg = new StreamConfig(taskConfig);
		cfg.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		switch (mode) {
			case OPERATOR_MANAGED:
			case OPERATOR_RAW:
				cfg.setStreamOperator(new StreamSource<>(new TestSourceOperator()));
				break;
			case KEYED_MANAGED:
			case KEYED_RAW:
				cfg.setStateKeySerializer(IntSerializer.INSTANCE);
				cfg.setStreamOperator(new StreamSource<>(new TestSourceKeyed()));
				break;
			case LEGACY:
				cfg.setStreamOperator(new StreamSource<>(new TestSourceLegacy()));
				break;
			default:
				throw new IllegalArgumentException();
		}

		StreamStateHandle lockingHandle = new InterruptLockingStateHandle();

		Task task = createTask(taskConfig, lockingHandle, mode);

		// start the task and wait until it is in "restore"
		task.startTaskThread();
		IN_RESTORE_LATCH.await();

		// trigger cancellation and signal to continue
		task.cancelExecution();

		task.getExecutingThread().join(30000);

		if (task.getExecutionState() == ExecutionState.CANCELING) {
			fail("Task is stuck and not canceling");
		}

		assertEquals(ExecutionState.CANCELED, task.getExecutionState());
		assertNull(task.getFailureCause());
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static Task createTask(
			Configuration taskConfig,
			StreamStateHandle state,
			int mode) throws IOException {

		NetworkEnvironment networkEnvironment = mock(NetworkEnvironment.class);
		when(networkEnvironment.createKvStateTaskRegistry(any(JobID.class), any(JobVertexID.class)))
				.thenReturn(mock(TaskKvStateRegistry.class));


		ChainedStateHandle<StreamStateHandle> operatorState = null;
		List<KeyGroupsStateHandle> keyGroupStateFromBackend = Collections.emptyList();
		List<KeyGroupsStateHandle> keyGroupStateFromStream = Collections.emptyList();
		List<Collection<OperatorStateHandle>> operatorStateBackend = Collections.emptyList();
		List<Collection<OperatorStateHandle>> operatorStateStream = Collections.emptyList();

		Map<String, long[]> operatorStateMetadata = new HashMap<>(1);
		operatorStateMetadata.put(DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME, new long[]{0});

		KeyGroupRangeOffsets keyGroupRangeOffsets = new KeyGroupRangeOffsets(new KeyGroupRange(0,0));

		Collection<OperatorStateHandle> operatorStateHandles =
				Collections.singletonList(new OperatorStateHandle(operatorStateMetadata, state));

		List<KeyGroupsStateHandle> keyGroupsStateHandles =
				Collections.singletonList(new KeyGroupsStateHandle(keyGroupRangeOffsets, state));

		switch (mode) {
			case OPERATOR_MANAGED:
				operatorStateBackend = Collections.singletonList(operatorStateHandles);
				break;
			case OPERATOR_RAW:
				operatorStateStream = Collections.singletonList(operatorStateHandles);
				break;
			case KEYED_MANAGED:
				keyGroupStateFromBackend = keyGroupsStateHandles;
				break;
			case KEYED_RAW:
				keyGroupStateFromStream = keyGroupsStateHandles;
				break;
			case LEGACY:
				operatorState = new ChainedStateHandle<>(Collections.singletonList(state));
				break;
			default:
				throw new IllegalArgumentException();
		}

		TaskStateHandles taskStateHandles = new TaskStateHandles(
			operatorState,
			operatorStateBackend,
			operatorStateStream,
			keyGroupStateFromBackend,
			keyGroupStateFromStream);

		JobInformation jobInformation = new JobInformation(
			new JobID(),
			"test job name",
			new SerializedValue<>(new ExecutionConfig()),
			new Configuration(),
			Collections.<BlobKey>emptyList(),
			Collections.<URL>emptyList());

		TaskInformation taskInformation = new TaskInformation(
			new JobVertexID(),
			"test task name",
			1,
			1,
			SourceStreamTask.class.getName(),
			taskConfig);

		return new Task(
			jobInformation,
			taskInformation,
			new ExecutionAttemptID(),
			new AllocationID(),
			0,
			0,
			Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
			Collections.<InputGateDeploymentDescriptor>emptyList(),
			0,
			taskStateHandles,
			mock(MemoryManager.class),
			mock(IOManager.class),
			networkEnvironment,
			mock(BroadcastVariableManager.class),
			mock(TaskManagerActions.class),
			mock(InputSplitProvider.class),
			mock(CheckpointResponder.class),
			new FallbackLibraryCacheManager(),
			new FileCache(new String[] { EnvironmentInformation.getTemporaryFileDirectory() }),
			new TestingTaskManagerRuntimeInfo(),
			new UnregisteredTaskMetricsGroup(),
			mock(ResultPartitionConsumableNotifier.class),
			mock(PartitionProducerStateChecker.class),
			mock(Executor.class));

	}

	// ------------------------------------------------------------------------

	@SuppressWarnings("serial")
	private static class InterruptLockingStateHandle implements StreamStateHandle {

		private volatile boolean closed;

		@Override
		public FSDataInputStream openInputStream() throws IOException {

			closed = false;

			FSDataInputStream is = new FSDataInputStream() {

				@Override
				public void seek(long desired) throws IOException {
				}

				@Override
				public long getPos() throws IOException {
					return 0;
				}

				@Override
				public int read() throws IOException {
					block();
					throw new EOFException();
				}

				@Override
				public void close() throws IOException {
					super.close();
					closed = true;
				}
			};

			return is;
		}

		private void block() {
			IN_RESTORE_LATCH.trigger();
			// this mimics what happens in the HDFS client code.
			// an interrupt on a waiting object leads to an infinite loop
			try {
				synchronized (this) {
					//noinspection WaitNotInLoop
					wait();
				}
			}
			catch (InterruptedException e) {
				while (!closed) {
					try {
						synchronized (this) {
							wait();
						}
					} catch (InterruptedException ignored) {}
				}
			}
		}

		@Override
		public void discardState() throws Exception {}

		@Override
		public long getStateSize() {
			return 0;
		}
	}

	// ------------------------------------------------------------------------

	private static class TestSourceLegacy implements SourceFunction<Object>, Checkpointed<Serializable> {
		private static final long serialVersionUID = 1L;

		@Override
		public void run(SourceContext<Object> ctx) throws Exception {
			fail("should never be called");
		}

		@Override
		public void cancel() {}

		@Override
		public Serializable snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			fail("should never be called");
			return null;
		}

		@Override
		public void restoreState(Serializable state) throws Exception {
			fail("should never be called");
		}
	}

	private static class TestSourceOperator implements SourceFunction<Object>, CheckpointedFunction {
		private static final long serialVersionUID = 1L;

		@Override
		public void run(SourceContext<Object> ctx) throws Exception {
			fail("should never be called");
		}

		@Override
		public void cancel() {}


		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			fail("should never be called");
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			((StateInitializationContext)context).getRawOperatorStateInputs().iterator().next().getStream().read();
		}
	}

	private static class TestSourceKeyed implements SourceFunction<Object>, CheckpointedFunction {
		private static final long serialVersionUID = 1L;

		@Override
		public void run(SourceContext<Object> ctx) throws Exception {
			fail("should never be called");
		}

		@Override
		public void cancel() {}


		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			fail("should never be called");
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			((StateInitializationContext)context).getRawKeyedStateInputs().iterator().next().getStream().read();
		}
	}
}
