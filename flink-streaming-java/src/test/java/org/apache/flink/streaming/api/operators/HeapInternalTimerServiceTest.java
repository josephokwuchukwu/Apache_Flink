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
package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupsList;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link HeapInternalTimerService}.
 */
public class HeapInternalTimerServiceTest {

	private static final int startKeyGroupIdx = 0;
	private static final int endKeyGroupIdx = 10;
	private static final KeyGroupsList testKeyGroupList =
		new KeyGroupRange(startKeyGroupIdx, endKeyGroupIdx);

	private static InternalTimer<Integer, String> anyInternalTimer() {
		return any();
	}

	@Test
	public void testKeyGroupStartIndexSetting() {

		int startKeyGroupIdx = 7;
		int endKeyGroupIdx = 21;
		KeyGroupsList testKeyGroupList = new KeyGroupRange(startKeyGroupIdx, endKeyGroupIdx);

		TestKeyContext keyContext = new TestKeyContext();

		TestProcessingTimeService processingTimeService = new TestProcessingTimeService();

		HeapInternalTimerService<Integer, String> service =
				new HeapInternalTimerService<>(
						testKeyGroupList.getNumberOfKeyGroups(),
						testKeyGroupList,
						keyContext,
						processingTimeService);

		Assert.assertEquals(startKeyGroupIdx, service.getLocalKeyGroupRangeStartIdx());
	}

	@Test
	public void testTimerAssignmentToKeyGroups() {
		int totalNoOfTimers = 100;

		int totalNoOfKeyGroups = 100;
		int startKeyGroupIdx = 0;
		int endKeyGroupIdx = totalNoOfKeyGroups - 1; // we have 0 to 99

		@SuppressWarnings("unchecked")
		Set<InternalTimer<Integer, String>>[] expectedNonEmptyTimerSets = new HashSet[totalNoOfKeyGroups];

		TestKeyContext keyContext = new TestKeyContext();
		HeapInternalTimerService<Integer, String> timerService =
				new HeapInternalTimerService<>(
						totalNoOfKeyGroups,
						new KeyGroupRange(startKeyGroupIdx, endKeyGroupIdx),
						keyContext,
						new TestProcessingTimeService());

		timerService.startTimerService(IntSerializer.INSTANCE, StringSerializer.INSTANCE, mock(Triggerable.class));

		for (int i = 0; i < totalNoOfTimers; i++) {

			// create the timer to be registered
			InternalTimer<Integer, String> timer = new InternalTimer<>(10 + i, i, "hello_world_"+ i);
			int keyGroupIdx =  KeyGroupRangeAssignment.assignToKeyGroup(timer.getKey(), totalNoOfKeyGroups);

			// add it in the adequate expected set of timers per keygroup
			Set<InternalTimer<Integer, String>> timerSet = expectedNonEmptyTimerSets[keyGroupIdx];
			if (timerSet == null) {
				timerSet = new HashSet<>();
				expectedNonEmptyTimerSets[keyGroupIdx] = timerSet;
			}
			timerSet.add(timer);

			// register the timer as both processing and event time one
			keyContext.setCurrentKey(timer.getKey());
			timerService.registerEventTimeTimer(timer.getNamespace(), timer.getTimestamp());
			timerService.registerProcessingTimeTimer(timer.getNamespace(), timer.getTimestamp());
		}

		Set<InternalTimer<Integer, String>>[] eventTimeTimers = timerService.getEventTimeTimersPerKeyGroup();
		Set<InternalTimer<Integer, String>>[] processingTimeTimers = timerService.getProcessingTimeTimersPerKeyGroup();

		// finally verify that the actual timers per key group sets are the expected ones.
		for (int i = 0; i < expectedNonEmptyTimerSets.length; i++) {
			Set<InternalTimer<Integer, String>> expected = expectedNonEmptyTimerSets[i];
			Set<InternalTimer<Integer, String>> actualEvent = eventTimeTimers[i];
			Set<InternalTimer<Integer, String>> actualProcessing = processingTimeTimers[i];

			if (expected == null) {
				Assert.assertNull(actualEvent);
				Assert.assertNull(actualProcessing);
			} else {
				Assert.assertArrayEquals(expected.toArray(), actualEvent.toArray());
				Assert.assertArrayEquals(expected.toArray(), actualProcessing.toArray());
			}
		}
	}

	/**
	 * Verify that we only ever have one processing-time task registered at the
	 * {@link ProcessingTimeService}.
	 */
	@Test
	public void testOnlySetsOnePhysicalProcessingTimeTimer() throws Exception {
		@SuppressWarnings("unchecked")
		Triggerable<Integer, String> mockTriggerable = mock(Triggerable.class);

		TestKeyContext keyContext = new TestKeyContext();

		TestProcessingTimeService processingTimeService = new TestProcessingTimeService();

		HeapInternalTimerService<Integer, String> timerService =
				createTimerService(mockTriggerable, keyContext, processingTimeService);

		keyContext.setCurrentKey(0);

		timerService.registerProcessingTimeTimer("ciao", 10);
		timerService.registerProcessingTimeTimer("ciao", 20);
		timerService.registerProcessingTimeTimer("ciao", 30);
		timerService.registerProcessingTimeTimer("hello", 10);
		timerService.registerProcessingTimeTimer("hello", 20);

		assertEquals(5, timerService.numProcessingTimeTimers());
		assertEquals(2, timerService.numProcessingTimeTimers("hello"));
		assertEquals(3, timerService.numProcessingTimeTimers("ciao"));

		assertEquals(1, processingTimeService.getNumRegisteredTimers());
		assertThat(processingTimeService.getRegisteredTimerTimestamps(), containsInAnyOrder(10L));

		processingTimeService.setCurrentTime(10);

		assertEquals(3, timerService.numProcessingTimeTimers());
		assertEquals(1, timerService.numProcessingTimeTimers("hello"));
		assertEquals(2, timerService.numProcessingTimeTimers("ciao"));

		assertEquals(1, processingTimeService.getNumRegisteredTimers());
		assertThat(processingTimeService.getRegisteredTimerTimestamps(), containsInAnyOrder(20L));

		processingTimeService.setCurrentTime(20);

		assertEquals(1, timerService.numProcessingTimeTimers());
		assertEquals(0, timerService.numProcessingTimeTimers("hello"));
		assertEquals(1, timerService.numProcessingTimeTimers("ciao"));

		assertEquals(1, processingTimeService.getNumRegisteredTimers());
		assertThat(processingTimeService.getRegisteredTimerTimestamps(), containsInAnyOrder(30L));

		processingTimeService.setCurrentTime(30);

		assertEquals(0, timerService.numProcessingTimeTimers());

		assertEquals(0, processingTimeService.getNumRegisteredTimers());

		timerService.registerProcessingTimeTimer("ciao", 40);

		assertEquals(1, processingTimeService.getNumRegisteredTimers());
	}

	/**
	 * Verify that registering a processing-time timer that is earlier than the existing timers
	 * removes the one physical timer and creates one for the earlier timestamp
	 * {@link ProcessingTimeService}.
	 */
	@Test
	public void testRegisterEarlierProcessingTimerMovesPhysicalProcessingTimer() throws Exception {
		@SuppressWarnings("unchecked")
		Triggerable<Integer, String> mockTriggerable = mock(Triggerable.class);

		TestKeyContext keyContext = new TestKeyContext();

		TestProcessingTimeService processingTimeService = new TestProcessingTimeService();

		HeapInternalTimerService<Integer, String> timerService =
				createTimerService(mockTriggerable, keyContext, processingTimeService);

		keyContext.setCurrentKey(0);

		timerService.registerProcessingTimeTimer("ciao", 20);

		assertEquals(1, timerService.numProcessingTimeTimers());

		assertEquals(1, processingTimeService.getNumRegisteredTimers());
		assertThat(processingTimeService.getRegisteredTimerTimestamps(), containsInAnyOrder(20L));

		timerService.registerProcessingTimeTimer("ciao", 10);

		assertEquals(2, timerService.numProcessingTimeTimers());

		assertEquals(1, processingTimeService.getNumRegisteredTimers());
		assertThat(processingTimeService.getRegisteredTimerTimestamps(), containsInAnyOrder(10L));
	}

	/**
	 */
	@Test
	public void testRegisteringProcessingTimeTimerInOnProcessingTimeDoesNotLeakPhysicalTimers() throws Exception {
		@SuppressWarnings("unchecked")
		Triggerable<Integer, String> mockTriggerable = mock(Triggerable.class);

		TestKeyContext keyContext = new TestKeyContext();

		TestProcessingTimeService processingTimeService = new TestProcessingTimeService();

		final HeapInternalTimerService<Integer, String> timerService =
				createTimerService(mockTriggerable, keyContext, processingTimeService);

		keyContext.setCurrentKey(0);

		timerService.registerProcessingTimeTimer("ciao", 10);

		assertEquals(1, timerService.numProcessingTimeTimers());

		assertEquals(1, processingTimeService.getNumRegisteredTimers());
		assertThat(processingTimeService.getRegisteredTimerTimestamps(), containsInAnyOrder(10L));

		doAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Exception {
				timerService.registerProcessingTimeTimer("ciao", 20);
				return null;
			}
		}).when(mockTriggerable).onProcessingTime(anyInternalTimer());

		processingTimeService.setCurrentTime(10);

		assertEquals(1, processingTimeService.getNumRegisteredTimers());
		assertThat(processingTimeService.getRegisteredTimerTimestamps(), containsInAnyOrder(20L));

		doAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Exception {
				timerService.registerProcessingTimeTimer("ciao", 30);
				return null;
			}
		}).when(mockTriggerable).onProcessingTime(anyInternalTimer());

		processingTimeService.setCurrentTime(20);

		assertEquals(1, timerService.numProcessingTimeTimers());

		assertEquals(1, processingTimeService.getNumRegisteredTimers());
		assertThat(processingTimeService.getRegisteredTimerTimestamps(), containsInAnyOrder(30L));
	}


	@Test
	public void testCurrentProcessingTime() throws Exception {

		@SuppressWarnings("unchecked")
		Triggerable<Integer, String> mockTriggerable = mock(Triggerable.class);

		TestKeyContext keyContext = new TestKeyContext();
		TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
		HeapInternalTimerService<Integer, String> timerService =
				createTimerService(mockTriggerable, keyContext, processingTimeService);

		processingTimeService.setCurrentTime(17L);
		assertEquals(17, timerService.currentProcessingTime());

		processingTimeService.setCurrentTime(42);
		assertEquals(42, timerService.currentProcessingTime());
	}

	@Test
	public void testCurrentEventTime() throws Exception {

		@SuppressWarnings("unchecked")
		Triggerable<Integer, String> mockTriggerable = mock(Triggerable.class);

		TestKeyContext keyContext = new TestKeyContext();
		TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
		HeapInternalTimerService<Integer, String> timerService =
				createTimerService(mockTriggerable, keyContext, processingTimeService);

		timerService.advanceWatermark(17);
		assertEquals(17, timerService.currentWatermark());

		timerService.advanceWatermark(42);
		assertEquals(42, timerService.currentWatermark());
	}

	/**
	 * This also verifies that we don't have leakage between keys/namespaces.
	 */
	@Test
	public void testSetAndFireEventTimeTimers() throws Exception {
		@SuppressWarnings("unchecked")
		Triggerable<Integer, String> mockTriggerable = mock(Triggerable.class);

		TestKeyContext keyContext = new TestKeyContext();
		TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
		HeapInternalTimerService<Integer, String> timerService =
				createTimerService(mockTriggerable, keyContext, processingTimeService);

		keyContext.setCurrentKey(0);

		timerService.registerEventTimeTimer("ciao", 10);
		timerService.registerEventTimeTimer("hello", 10);

		keyContext.setCurrentKey(1);

		timerService.registerEventTimeTimer("ciao", 10);
		timerService.registerEventTimeTimer("hello", 10);

		assertEquals(4, timerService.numEventTimeTimers());
		assertEquals(2, timerService.numEventTimeTimers("hello"));
		assertEquals(2, timerService.numEventTimeTimers("ciao"));

		timerService.advanceWatermark(10);

		verify(mockTriggerable, times(4)).onEventTime(anyInternalTimer());
		verify(mockTriggerable, times(1)).onEventTime(eq(new InternalTimer<>(10, 0, "ciao")));
		verify(mockTriggerable, times(1)).onEventTime(eq(new InternalTimer<>(10, 0, "hello")));
		verify(mockTriggerable, times(1)).onEventTime(eq(new InternalTimer<>(10, 1, "ciao")));
		verify(mockTriggerable, times(1)).onEventTime(eq(new InternalTimer<>(10, 1, "hello")));

		assertEquals(0, timerService.numEventTimeTimers());
	}

	/**
	 * This also verifies that we don't have leakage between keys/namespaces.
	 */
	@Test
	public void testSetAndFireProcessingTimeTimers() throws Exception {
		@SuppressWarnings("unchecked")
		Triggerable<Integer, String> mockTriggerable = mock(Triggerable.class);

		TestKeyContext keyContext = new TestKeyContext();
		TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
		HeapInternalTimerService<Integer, String> timerService =
				createTimerService(mockTriggerable, keyContext, processingTimeService);

		keyContext.setCurrentKey(0);

		timerService.registerProcessingTimeTimer("ciao", 10);
		timerService.registerProcessingTimeTimer("hello", 10);

		keyContext.setCurrentKey(1);

		timerService.registerProcessingTimeTimer("ciao", 10);
		timerService.registerProcessingTimeTimer("hello", 10);

		assertEquals(4, timerService.numProcessingTimeTimers());
		assertEquals(2, timerService.numProcessingTimeTimers("hello"));
		assertEquals(2, timerService.numProcessingTimeTimers("ciao"));

		processingTimeService.setCurrentTime(10);

		verify(mockTriggerable, times(4)).onProcessingTime(anyInternalTimer());
		verify(mockTriggerable, times(1)).onProcessingTime(eq(new InternalTimer<>(10, 0, "ciao")));
		verify(mockTriggerable, times(1)).onProcessingTime(eq(new InternalTimer<>(10, 0, "hello")));
		verify(mockTriggerable, times(1)).onProcessingTime(eq(new InternalTimer<>(10, 1, "ciao")));
		verify(mockTriggerable, times(1)).onProcessingTime(eq(new InternalTimer<>(10, 1, "hello")));

		assertEquals(0, timerService.numProcessingTimeTimers());
	}

	/**
	 * This also verifies that we don't have leakage between keys/namespaces.
	 *
	 * <p>This also verifies that deleted timers don't fire.
	 */
	@Test
	public void testDeleteEventTimeTimers() throws Exception {
		@SuppressWarnings("unchecked")
		Triggerable<Integer, String> mockTriggerable = mock(Triggerable.class);

		TestKeyContext keyContext = new TestKeyContext();
		TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
		HeapInternalTimerService<Integer, String> timerService =
				createTimerService(mockTriggerable, keyContext, processingTimeService);

		keyContext.setCurrentKey(0);

		timerService.registerEventTimeTimer("ciao", 10);
		timerService.registerEventTimeTimer("hello", 10);

		keyContext.setCurrentKey(1);

		timerService.registerEventTimeTimer("ciao", 10);
		timerService.registerEventTimeTimer("hello", 10);

		assertEquals(4, timerService.numEventTimeTimers());
		assertEquals(2, timerService.numEventTimeTimers("hello"));
		assertEquals(2, timerService.numEventTimeTimers("ciao"));

		keyContext.setCurrentKey(0);
		timerService.deleteEventTimeTimer("hello", 10);

		keyContext.setCurrentKey(1);
		timerService.deleteEventTimeTimer("ciao", 10);

		assertEquals(2, timerService.numEventTimeTimers());
		assertEquals(1, timerService.numEventTimeTimers("hello"));
		assertEquals(1, timerService.numEventTimeTimers("ciao"));

		timerService.advanceWatermark(10);

		verify(mockTriggerable, times(2)).onEventTime(anyInternalTimer());
		verify(mockTriggerable, times(1)).onEventTime(eq(new InternalTimer<>(10, 0, "ciao")));
		verify(mockTriggerable, times(0)).onEventTime(eq(new InternalTimer<>(10, 0, "hello")));
		verify(mockTriggerable, times(0)).onEventTime(eq(new InternalTimer<>(10, 1, "ciao")));
		verify(mockTriggerable, times(1)).onEventTime(eq(new InternalTimer<>(10, 1, "hello")));

		assertEquals(0, timerService.numEventTimeTimers());
	}

	/**
	 * This also verifies that we don't have leakage between keys/namespaces.
	 *
	 * <p>This also verifies that deleted timers don't fire.
	 */
	@Test
	public void testDeleteProcessingTimeTimers() throws Exception {
		@SuppressWarnings("unchecked")
		Triggerable<Integer, String> mockTriggerable = mock(Triggerable.class);

		TestKeyContext keyContext = new TestKeyContext();
		TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
		HeapInternalTimerService<Integer, String> timerService =
				createTimerService(mockTriggerable, keyContext, processingTimeService);

		keyContext.setCurrentKey(0);

		timerService.registerProcessingTimeTimer("ciao", 10);
		timerService.registerProcessingTimeTimer("hello", 10);

		keyContext.setCurrentKey(1);

		timerService.registerProcessingTimeTimer("ciao", 10);
		timerService.registerProcessingTimeTimer("hello", 10);

		assertEquals(4, timerService.numProcessingTimeTimers());
		assertEquals(2, timerService.numProcessingTimeTimers("hello"));
		assertEquals(2, timerService.numProcessingTimeTimers("ciao"));

		keyContext.setCurrentKey(0);
		timerService.deleteProcessingTimeTimer("hello", 10);

		keyContext.setCurrentKey(1);
		timerService.deleteProcessingTimeTimer("ciao", 10);

		assertEquals(2, timerService.numProcessingTimeTimers());
		assertEquals(1, timerService.numProcessingTimeTimers("hello"));
		assertEquals(1, timerService.numProcessingTimeTimers("ciao"));

		processingTimeService.setCurrentTime(10);

		verify(mockTriggerable, times(2)).onProcessingTime(anyInternalTimer());
		verify(mockTriggerable, times(1)).onProcessingTime(eq(new InternalTimer<>(10, 0, "ciao")));
		verify(mockTriggerable, times(0)).onProcessingTime(eq(new InternalTimer<>(10, 0, "hello")));
		verify(mockTriggerable, times(0)).onProcessingTime(eq(new InternalTimer<>(10, 1, "ciao")));
		verify(mockTriggerable, times(1)).onProcessingTime(eq(new InternalTimer<>(10, 1, "hello")));

		assertEquals(0, timerService.numEventTimeTimers());
	}

	@Test
	public void testSnapshotAndRestore() throws Exception {
		@SuppressWarnings("unchecked")
		Triggerable<Integer, String> mockTriggerable = mock(Triggerable.class);

		TestKeyContext keyContext = new TestKeyContext();
		TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
		HeapInternalTimerService<Integer, String> timerService =
				createTimerService(mockTriggerable, keyContext, processingTimeService);

		keyContext.setCurrentKey(0);

		timerService.registerProcessingTimeTimer("ciao", 10);
		timerService.registerEventTimeTimer("hello", 10);

		keyContext.setCurrentKey(1);

		timerService.registerEventTimeTimer("ciao", 10);
		timerService.registerProcessingTimeTimer("hello", 10);

		assertEquals(2, timerService.numProcessingTimeTimers());
		assertEquals(1, timerService.numProcessingTimeTimers("hello"));
		assertEquals(1, timerService.numProcessingTimeTimers("ciao"));
		assertEquals(2, timerService.numEventTimeTimers());
		assertEquals(1, timerService.numEventTimeTimers("hello"));
		assertEquals(1, timerService.numEventTimeTimers("ciao"));

		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		for (int keyGroupIdx = startKeyGroupIdx; keyGroupIdx < endKeyGroupIdx; keyGroupIdx++) {
			timerService.snapshotTimersForKeyGroup(new DataOutputViewStreamWrapper(outStream), keyGroupIdx);
		}
		outStream.close();

		@SuppressWarnings("unchecked")
		Triggerable<Integer, String> mockTriggerable2 = mock(Triggerable.class);

		keyContext = new TestKeyContext();
		processingTimeService = new TestProcessingTimeService();

		timerService = restoreTimerService(
				new ByteArrayInputStream(outStream.toByteArray()),
				mockTriggerable2,
				keyContext,
				processingTimeService);

		processingTimeService.setCurrentTime(10);
		timerService.advanceWatermark(10);

		verify(mockTriggerable2, times(2)).onProcessingTime(anyInternalTimer());
		verify(mockTriggerable2, times(1)).onProcessingTime(eq(new InternalTimer<>(10, 0, "ciao")));
		verify(mockTriggerable2, times(1)).onProcessingTime(eq(new InternalTimer<>(10, 1, "hello")));
		verify(mockTriggerable2, times(2)).onEventTime(anyInternalTimer());
		verify(mockTriggerable2, times(1)).onEventTime(eq(new InternalTimer<>(10, 0, "hello")));
		verify(mockTriggerable2, times(1)).onEventTime(eq(new InternalTimer<>(10, 1, "ciao")));

		assertEquals(0, timerService.numEventTimeTimers());
	}


	private static class TestKeyContext implements KeyContext {

		private Object key;

		@Override
		public void setCurrentKey(Object key) {
			this.key = key;
		}

		@Override
		public Object getCurrentKey() {
			return key;
		}
	}

	private static HeapInternalTimerService<Integer, String> createTimerService(
			Triggerable<Integer, String> triggerable,
			KeyContext keyContext,
			ProcessingTimeService processingTimeService) {
		HeapInternalTimerService<Integer, String> service =
			new HeapInternalTimerService<>(
				testKeyGroupList.getNumberOfKeyGroups(),
				testKeyGroupList,
				keyContext,
				processingTimeService);

		service.startTimerService(IntSerializer.INSTANCE, StringSerializer.INSTANCE, triggerable);
		return service;
	}

	private static HeapInternalTimerService<Integer, String> restoreTimerService(
			InputStream stateStream,
			Triggerable<Integer, String> triggerable,
			KeyContext keyContext,
			ProcessingTimeService processingTimeService) throws Exception {

		// create an empty service
		HeapInternalTimerService<Integer, String> service =
			new HeapInternalTimerService<>(
				testKeyGroupList.getNumberOfKeyGroups(),
				testKeyGroupList,
				keyContext,
				processingTimeService);

		// restore the timers
		for (int keyGroupIdx = startKeyGroupIdx; keyGroupIdx < endKeyGroupIdx; keyGroupIdx++) {
			service.restoreTimersForKeyGroup(
				new DataInputViewStreamWrapper(stateStream),
				keyGroupIdx,
				HeapInternalTimerServiceTest.class.getClassLoader());
		}

		// initialize the service
		service.startTimerService(IntSerializer.INSTANCE, StringSerializer.INSTANCE, triggerable);
		return service;
	}
}
