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

package org.apache.flink.runtime.rest.handler;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link InFlightRequestTracker}.
 */
public class InFlightRequestTrackerTest {

	private InFlightRequestTracker inFlightRequestTracker;

	@Before
	public void setUp() {
		inFlightRequestTracker = new InFlightRequestTracker();
	}

	@Test
	public void testShouldFinishAwaitAsyncImmediatelyIfNoRequests() {
		assertTrue(inFlightRequestTracker.awaitAsync().isDone());
	}

	@Test
	public void testShouldFinishAwaitAsyncIffAllRequestsDeregistered() {
		inFlightRequestTracker.registerRequest();

		final CompletableFuture<Void> closeFuture = inFlightRequestTracker.awaitAsync();
		assertFalse(closeFuture.isDone());

		inFlightRequestTracker.deregisterRequest();
		assertTrue(closeFuture.isDone());
	}

	@Test
	public void testAwaitAsyncIsIdempotent() {
		assertTrue(inFlightRequestTracker.awaitAsync().isDone());
		assertTrue(inFlightRequestTracker.awaitAsync().isDone());
	}

	@Test
	public void testShouldTolerateRegisterAfterAwaitAsync() {
		assertTrue(inFlightRequestTracker.awaitAsync().isDone());
		inFlightRequestTracker.registerRequest();
	}
}
