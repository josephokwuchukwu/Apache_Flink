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

package org.apache.flink.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A simple implementation of {@link Counter} that is thread-safe.
 */
public class ConcurrentCounter implements Counter {

	/** the current count. */
	private AtomicLong count = new AtomicLong();

	/**
	 * Increment the current count by 1.
	 */
	@Override
	public void inc() {
		count.getAndIncrement();
	}

	/**
	 * Increment the current count by the given value.
	 *
	 * @param n value to increment the current count by
	 */
	@Override
	public void inc(long n) {
		count.getAndAdd(n);
	}

	/**
	 * Decrement the current count by 1.
	 */
	@Override
	public void dec() {
		count.getAndDecrement();
	}

	/**
	 * Decrement the current count by the given value.
	 *
	 * @param n value to decrement the current count by
	 */
	@Override
	public void dec(long n) {
		count.getAndAdd(-n);
	}

	/**
	 * Returns the current count.
	 *
	 * @return current count
	 */
	@Override
	public long getCount() {
		return count.get();
	}
}
