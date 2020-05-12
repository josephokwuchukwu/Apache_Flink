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

package org.apache.flink.runtime.memory;

import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link OpaqueMemoryResource}.
 */
public class OpaqueMemoryResourceTest {

	@Test
	public void testCloseIsIdempotent() throws Exception {
		final CountingCloseable disposer = new CountingCloseable();
		final OpaqueMemoryResource<Object> resource = new OpaqueMemoryResource<>(new Object(), 10, disposer);

		resource.close();
		resource.close();

		assertEquals(1, disposer.count);
	}

	private static final class CountingCloseable implements ThrowingRunnable<Exception> {

		int count = 0;

		@Override
		public void run() throws Exception {
			count++;
		}
	}
}
