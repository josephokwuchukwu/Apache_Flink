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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A test verifying the termination process
 * (synchronous checkpoint and task termination) at the {@link SourceStreamTask}.
 */
@RunWith(Parameterized.class)
public class SourceTaskTerminationTest {

	private static OneShotLatch ready;
	private static MultiShotLatch runLoopStart;
	private static MultiShotLatch runLoopEnd;

	private static AtomicReference<Throwable> error;

	@Parameterized.Parameters(name = "expectedMaxWatermark = {1}")
	public static Collection<Boolean> parameters () {
		return Arrays.asList(true, false);
	}

	@Parameterized.Parameter
	public Boolean expectedMaxWatermak;

	@Before
	public void initialize() {
		ready = new OneShotLatch();
		runLoopStart = new MultiShotLatch();
		runLoopEnd = new MultiShotLatch();
		error = new AtomicReference<>();

		error.set(null);
	}

	@After
	public void validate() {
		validateNoExceptionsWereThrown();
	}

	@Test
	public void terminateShouldBlockDuringCheckpointingAndEmitMaxWatermark() throws Exception {
		stopWithSavepointStreamTaskTestHelper(expectedMaxWatermak);
	}

	private void stopWithSavepointStreamTaskTestHelper(final boolean expectMaxWatermark) throws Exception {
		final long syncSavepointId = 34L;

		final StreamTaskTestHarness<Long> srcTaskTestHarness = getSourceStreamTaskTestHarness();
		final Thread executionThread = srcTaskTestHarness.invoke();
		final StreamTask<Long, ?> srcTask = srcTaskTestHarness.getTask();

		ready.await();

		// step by step let the source thread emit elements
		emitAndVerifyWatermarkAndElement(srcTaskTestHarness, 1L);
		emitAndVerifyWatermarkAndElement(srcTaskTestHarness, 2L);

		emitAndVerifyCheckpoint(srcTaskTestHarness, srcTask, 31L);

		emitAndVerifyWatermarkAndElement(srcTaskTestHarness, 3L);

		final Thread syncSavepointThread = triggerSynchronousSavepointFromDifferentThread(srcTask, expectMaxWatermark, syncSavepointId);

		final SynchronousCheckpointLatch syncSavepointFuture = waitForSyncCheckpointFutureToBeSet(srcTask);

		if (expectMaxWatermark) {
			// if we are in TERMINATE mode, we expect the source task
			// to emit MAX_WM before the SYNC_SAVEPOINT barrier.
			verifyWatermark(srcTaskTestHarness.getOutput(), Watermark.MAX_WATERMARK);
		}

		verifyCheckpointBarrier(srcTaskTestHarness.getOutput(), syncSavepointId);

		assertFalse(syncSavepointFuture.isCompleted());
		assertTrue(syncSavepointFuture.isWaiting());

		srcTask.notifyCheckpointComplete(syncSavepointId);
		assertTrue(syncSavepointFuture.isCompleted());

		syncSavepointThread.join();
		executionThread.join();
	}

	private void validateNoExceptionsWereThrown() {
		if (error.get() != null && !(error.get() instanceof CancelTaskException)) {
			fail(error.get().getMessage());
		}
	}

	private Thread triggerSynchronousSavepointFromDifferentThread(
			final StreamTask<Long, ?> task,
			final boolean advanceToEndOfEventTime,
			final long syncSavepointId) {
		final Thread checkpointingThread = new Thread(() -> {
			try {
				task.triggerCheckpoint(
						new CheckpointMetaData(syncSavepointId, 900),
						new CheckpointOptions(CheckpointType.SYNC_SAVEPOINT, CheckpointStorageLocationReference.getDefault()),
						advanceToEndOfEventTime);
			} catch (Exception e) {
				error.set(e);
			}
		});
		checkpointingThread.start();

		return checkpointingThread;

	}

	private void emitAndVerifyCheckpoint(
			final StreamTaskTestHarness<Long> srcTaskTestHarness,
			final StreamTask<Long, ?> srcTask,
			final long checkpointId) throws Exception {

		srcTask.triggerCheckpoint(
				new CheckpointMetaData(checkpointId, 900),
				CheckpointOptions.forCheckpointWithDefaultLocation(),
				false);
		verifyCheckpointBarrier(srcTaskTestHarness.getOutput(), checkpointId);
	}

	private StreamTaskTestHarness<Long> getSourceStreamTaskTestHarness() {
		final StreamTaskTestHarness<Long> testHarness = new StreamTaskTestHarness<>(
				SourceStreamTask::new,
				BasicTypeInfo.LONG_TYPE_INFO);

		final LockStepSourceWithOneWmPerElement source = new LockStepSourceWithOneWmPerElement();

		testHarness.setupOutputForSingletonOperatorChain();
		testHarness.getExecutionConfig().setLatencyTrackingInterval(-1);

		StreamConfig streamConfig = testHarness.getStreamConfig();
		StreamSource<Long, ?> sourceOperator = new StreamSource<>(source);
		streamConfig.setStreamOperator(sourceOperator);
		streamConfig.setOperatorID(new OperatorID());
		return testHarness;
	}

	private SynchronousCheckpointLatch waitForSyncCheckpointFutureToBeSet(final StreamTask streamTaskUnderTest) throws InterruptedException {
		final SynchronousCheckpointLatch syncSavepointFuture = streamTaskUnderTest.getSynchronousSavepointLatch();
		while (!syncSavepointFuture.isWaiting()) {
			Thread.sleep(10L);

			validateNoExceptionsWereThrown();
		}
		return syncSavepointFuture;
	}

	private void emitAndVerifyWatermarkAndElement(
			final StreamTaskTestHarness<Long> srcTaskTestHarness,
			final long expectedElement) throws InterruptedException {

		runLoopStart.trigger();
		verifyWatermark(srcTaskTestHarness.getOutput(), new Watermark(expectedElement));
		verifyNextElement(srcTaskTestHarness.getOutput(), expectedElement);
		runLoopEnd.await();
	}

	private void verifyNextElement(BlockingQueue<Object> output, long expectedElement) throws InterruptedException {
		Object next = output.take();
		assertTrue("next element is not an event", next instanceof StreamRecord);
		assertEquals("wrong event", expectedElement, ((StreamRecord<Long>) next).getValue().longValue());
	}

	private void verifyWatermark(BlockingQueue<Object> output, Watermark expectedWatermark) throws InterruptedException {
		Object next = output.take();
		assertTrue("next element is not a watermark", next instanceof Watermark);
		assertEquals("wrong watermark", expectedWatermark, next);
	}

	private void verifyCheckpointBarrier(BlockingQueue<Object> output, long checkpointId) throws InterruptedException {
		Object next = output.take();
		assertTrue("next element is not a checkpoint barrier", next instanceof CheckpointBarrier);
		assertEquals("wrong checkpoint id", checkpointId, ((CheckpointBarrier) next).getId());
	}

	private static class LockStepSourceWithOneWmPerElement implements SourceFunction<Long> {

		private volatile boolean isRunning;

		@Override
		public void run(SourceContext<Long> ctx) throws Exception {
			long element = 1L;
			isRunning = true;

			ready.trigger();

			while (isRunning) {
				runLoopStart.await();
				ctx.emitWatermark(new Watermark(element));
				ctx.collect(element++);
				runLoopEnd.trigger();
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
			runLoopStart.trigger();
		}
	}
}
