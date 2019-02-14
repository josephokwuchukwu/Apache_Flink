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

package org.apache.flink.streaming.connectors.kafka.config;

import org.apache.flink.api.common.functions.RuntimeContext;

import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.RateLimiter;

/**
 * A default KafkaRateLimiter that uses Guava's RateLimiter to throttle the bytes read from Kafka.
 */
public class DefaultKafkaRateLimiter implements FlinkConnectorRateLimiter {

	/** Rate in bytes per second for the consumer on a whole. */
	private long globalRateBytesPerSecond;

	/** Rate in bytes per second per subtask of the consumer. */
	private long localRateBytesPerSecond;

	/** Runtime context. **/
	private RuntimeContext runtimeContext;

	@Override
	public void open(RuntimeContext runtimeContext) {
		this.runtimeContext = runtimeContext;
	}

	/** Create and return a Guava RateLimiter based on a desired rate.
	 * @return
	 */
	@Override
	public RateLimiter create() {
		localRateBytesPerSecond = globalRateBytesPerSecond / runtimeContext.getNumberOfParallelSubtasks();
		return RateLimiter.create(localRateBytesPerSecond);
	}

	/**
	 * Set the global per consumer and per sub-task rates.
	 * @param globalRate Value of rate in bytes per second.
	 */
	@Override
	public void setRate(long globalRate) {
		this.globalRateBytesPerSecond = globalRate;
	}

	@Override
	public void close() {

	}
}
