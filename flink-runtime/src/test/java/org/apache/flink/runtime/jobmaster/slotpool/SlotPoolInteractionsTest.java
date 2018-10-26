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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobmanager.scheduler.DummyScheduledUnit;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.clock.Clock;
import org.apache.flink.runtime.util.clock.SystemClock;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.flink.runtime.jobmaster.slotpool.AvailableSlotsTest.DEFAULT_TESTING_PROFILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the SlotPool interactions.
 */
public class SlotPoolInteractionsTest extends TestLogger {

	private static final Time fastTimeout = Time.milliseconds(1L);

	private final ScheduledExecutor testMainThreadExecutor = new ScheduledExecutorServiceAdapter(Executors.newSingleThreadScheduledExecutor());

	// ------------------------------------------------------------------------
	//  tests
	// ------------------------------------------------------------------------

	@Test
	public void testSlotAllocationNoResourceManager() throws Exception {
		final JobID jid = new JobID();

		try (SlotPool pool = new SlotPool(
			jid,
			SystemClock.getInstance(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime()
		)) {

			pool.start(JobMasterId.generate(), "foobar", testMainThreadExecutor);
			Scheduler scheduler = new Scheduler(new HashMap<>(), LocationPreferenceSlotSelection.INSTANCE, pool);
			scheduler.start(testMainThreadExecutor);

			CompletableFuture<LogicalSlot> future = scheduler.allocateSlot(
				new SlotRequestId(),
				new ScheduledUnit(SchedulerTestUtils.getDummyTask()),
				SlotProfile.noLocality(DEFAULT_TESTING_PROFILE),
				true,
				fastTimeout);

			try {
				future.get();
				fail("We expected an ExecutionException.");
			} catch (ExecutionException e) {
				assertTrue(ExceptionUtils.stripExecutionException(e) instanceof TimeoutException);
			}
		}
	}

	@Test
	public void testCancelSlotAllocationWithoutResourceManager() throws Exception {
		final JobID jid = new JobID();

		try (TestingSlotPool pool = new TestingSlotPool(
			jid,
			SystemClock.getInstance(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime())) {

			final CompletableFuture<SlotRequestId> timeoutFuture = new CompletableFuture<>();
			pool.setTimeoutPendingSlotRequestConsumer(timeoutFuture::complete);
			pool.start(JobMasterId.generate(), "foobar", testMainThreadExecutor);
			Scheduler scheduler = new Scheduler(new HashMap<>(), LocationPreferenceSlotSelection.INSTANCE, pool);
			scheduler.start(testMainThreadExecutor);

			SlotRequestId requestId = new SlotRequestId();
			CompletableFuture<LogicalSlot> future = scheduler.allocateSlot(
				requestId,
				new ScheduledUnit(SchedulerTestUtils.getDummyTask()),
				SlotProfile.noLocality(DEFAULT_TESTING_PROFILE),
				true,
				fastTimeout);

			try {
				future.get();
				fail("We expected a TimeoutException.");
			} catch (ExecutionException e) {
				assertTrue(ExceptionUtils.stripExecutionException(e) instanceof TimeoutException);
			}

			// wait for the timeout of the pending slot request
			timeoutFuture.get();

			assertEquals(0L, pool.getNumberOfWaitingForResourceRequests());
		}
	}

	/**
	 * Tests that a slot allocation times out wrt to the specified time out.
	 */
	@Test
	public void testSlotAllocationTimeout() throws Exception {
		final JobID jid = new JobID();

		try (TestingSlotPool pool = new TestingSlotPool(
			jid,
			SystemClock.getInstance(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime())) {

			pool.start(JobMasterId.generate(), "foobar", testMainThreadExecutor);

			final CompletableFuture<SlotRequestId> slotRequestTimeoutFuture = new CompletableFuture<>();
			pool.setTimeoutPendingSlotRequestConsumer(slotRequestTimeoutFuture::complete);

			ResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
			pool.connectToResourceManager(resourceManagerGateway);

			Scheduler scheduler = new Scheduler(new HashMap<>(), LocationPreferenceSlotSelection.INSTANCE, pool);
			scheduler.start(testMainThreadExecutor);

			SlotRequestId requestId = new SlotRequestId();
			CompletableFuture<LogicalSlot> future = scheduler.allocateSlot(
				requestId,
				new DummyScheduledUnit(),
				SlotProfile.noLocality(DEFAULT_TESTING_PROFILE),
				true,
				fastTimeout);

			try {
				future.get();
				fail("We expected a TimeoutException.");
			} catch (ExecutionException e) {
				assertTrue(ExceptionUtils.stripExecutionException(e) instanceof TimeoutException);
			}

			// wait until we have timed out the slot request
			slotRequestTimeoutFuture.get();

			assertEquals(0L, pool.getNumberOfPendingRequests());
		}
	}

	/**
	 * Tests that extra slots are kept by the {@link SlotPool}.
	 */
	@Test
	public void testExtraSlotsAreKept() throws Exception {
		final JobID jid = new JobID();

		try (TestingSlotPool pool = new TestingSlotPool(
			jid,
			SystemClock.getInstance(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime())) {

			pool.start(JobMasterId.generate(), "foobar", testMainThreadExecutor);

			Scheduler scheduler = new Scheduler(new HashMap<>(), LocationPreferenceSlotSelection.INSTANCE, pool);
			scheduler.start(testMainThreadExecutor);

			final CompletableFuture<AllocationID> allocationIdFuture = new CompletableFuture<>();

			TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
			resourceManagerGateway.setRequestSlotConsumer(
				(SlotRequest slotRequest) -> allocationIdFuture.complete(slotRequest.getAllocationId()));

			final CompletableFuture<SlotRequestId> slotRequestTimeoutFuture = new CompletableFuture<>();
			pool.setTimeoutPendingSlotRequestConsumer(slotRequestTimeoutFuture::complete);

			pool.connectToResourceManager(resourceManagerGateway);

			SlotRequestId requestId = new SlotRequestId();
			CompletableFuture<LogicalSlot> future = scheduler.allocateSlot(
				requestId,
				new ScheduledUnit(SchedulerTestUtils.getDummyTask()),
				SlotProfile.noLocality(DEFAULT_TESTING_PROFILE),
				true,
				fastTimeout);

			try {
				future.get();
				fail("We expected a TimeoutException.");
			} catch (ExecutionException e) {
				assertTrue(ExceptionUtils.stripExecutionException(e) instanceof TimeoutException);
			}

			// wait until we have timed out the slot request
			slotRequestTimeoutFuture.get();

			assertEquals(0L, pool.getNumberOfPendingRequests());

			AllocationID allocationId = allocationIdFuture.get();
			final SlotOffer slotOffer = new SlotOffer(
				allocationId,
				0,
				DEFAULT_TESTING_PROFILE);
			final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			final TaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();

			pool.registerTaskManager(taskManagerLocation.getResourceID()).get();

			assertTrue(pool.offerSlot(taskManagerLocation, taskManagerGateway, slotOffer).get());

			assertTrue(pool.containsAvailableSlot(allocationId));
		}
	}

	/**
	 * This case make sure when allocateSlot in ProviderAndOwner timeout,
	 * it will automatically call cancelSlotAllocation as will inject future.whenComplete in ProviderAndOwner.
	 */
	@Test
	public void testProviderAndOwnerSlotAllocationTimeout() throws Exception {
		final JobID jid = new JobID();

		try (TestingSlotPool pool = new TestingSlotPool(
			jid,
			SystemClock.getInstance(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime())) {

			final CompletableFuture<SlotRequestId> releaseSlotFuture = new CompletableFuture<>();

			pool.setReleaseSlotConsumer(releaseSlotFuture::complete);

			pool.start(JobMasterId.generate(), "foobar", testMainThreadExecutor);
			ResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
			pool.connectToResourceManager(resourceManagerGateway);

			Scheduler scheduler = new Scheduler(new HashMap<>(), LocationPreferenceSlotSelection.INSTANCE, pool);
			scheduler.start(testMainThreadExecutor);

			// test the pending request is clear when timed out
			CompletableFuture<LogicalSlot> future = scheduler.allocateSlot(
				new DummyScheduledUnit(),
				true,
				SlotProfile.noRequirements(),
				fastTimeout);
			try {
				future.get();
				fail("We expected a TimeoutException.");
			} catch (ExecutionException e) {
				assertTrue(ExceptionUtils.stripExecutionException(e) instanceof TimeoutException);
			}

			// wait for the cancel call on the SlotPool
			releaseSlotFuture.get();

			assertEquals(0L, pool.getNumberOfPendingRequests());
		}
	}

	/**
	 * Testing SlotPool which exposes internal state via some testing methods.
	 */
	private static final class TestingSlotPool extends SlotPool {

		private volatile Consumer<SlotRequestId> releaseSlotConsumer;

		private volatile Consumer<SlotRequestId> timeoutPendingSlotRequestConsumer;

		public TestingSlotPool(
				JobID jobId,
				Clock clock,
				Time rpcTimeout,
				Time idleSlotTimeout) {
			super(
				jobId,
				clock,
				rpcTimeout,
				idleSlotTimeout);

			releaseSlotConsumer = null;
			timeoutPendingSlotRequestConsumer = null;
		}

		public void setReleaseSlotConsumer(Consumer<SlotRequestId> releaseSlotConsumer) {
			this.releaseSlotConsumer = Preconditions.checkNotNull(releaseSlotConsumer);
		}

		public void setTimeoutPendingSlotRequestConsumer(Consumer<SlotRequestId> timeoutPendingSlotRequestConsumer) {
			this.timeoutPendingSlotRequestConsumer = Preconditions.checkNotNull(timeoutPendingSlotRequestConsumer);
		}

		@Override
		public void releaseSlot(
			@Nonnull SlotRequestId slotRequestId,
			@Nullable Throwable cause) {
			final Consumer<SlotRequestId> currentReleaseSlotConsumer = releaseSlotConsumer;

			super.releaseSlot(slotRequestId, cause);

			if (currentReleaseSlotConsumer != null) {
				currentReleaseSlotConsumer.accept(slotRequestId);
			}
		}

		@Override
		protected void timeoutPendingSlotRequest(SlotRequestId slotRequestId) {
			final Consumer<SlotRequestId> currentTimeoutPendingSlotRequestConsumer = timeoutPendingSlotRequestConsumer;

			if (currentTimeoutPendingSlotRequestConsumer != null) {
				currentTimeoutPendingSlotRequestConsumer.accept(slotRequestId);
			}

			super.timeoutPendingSlotRequest(slotRequestId);
		}

		boolean containsAllocatedSlot(AllocationID allocationId) {
			return getAllocatedSlots().contains(allocationId);
		}

		boolean containsAvailableSlot(AllocationID allocationId) {
			return getAvailableSlots().contains(allocationId);
		}

		int getNumberOfPendingRequests() {
			return getPendingRequests().size();
		}

		int getNumberOfWaitingForResourceRequests() {
			return getWaitingForResourceManager().size();
		}
	}

	private <V> V executeInMainThreadAndJoin(Supplier<V> supplier) throws Exception {
		return CompletableFuture.supplyAsync(supplier, testMainThreadExecutor).get();
	}
}
