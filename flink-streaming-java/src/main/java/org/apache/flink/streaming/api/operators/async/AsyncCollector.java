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

package org.apache.flink.streaming.api.operators.async;

import org.apache.flink.annotation.Internal;

import java.util.List;

/**
 * {@link AsyncCollector} collects data / error in user codes while processing async i/o.
 *
 * @param <IN> Input type
 * @param <OUT> Output type
 */
@Internal
public class AsyncCollector<IN, OUT> {
	private List<OUT> result;
	private Throwable error;

	private boolean isDone = false;

	private AsyncCollectorBuffer<IN, OUT> buffer;

	public AsyncCollector(AsyncCollectorBuffer<IN, OUT> buffer) {
		this.buffer = buffer;
	}

	public AsyncCollector(AsyncCollectorBuffer<IN, OUT> buffer, boolean isDone) {
		this(buffer);
		this.isDone = isDone;
	}

	/**
	 * Set result
	 * @param result A list of results.
	 */
	public void collect(List<OUT> result) {
		this.result = result;
		isDone = true;
		buffer.mark(this);
	}

	/**
	 * Set error
	 * @param error A Throwable object.
	 */
	public void collect(Throwable error) {
		this.error = error;
		isDone = true;
		buffer.mark(this);
	}

	/**
	 * Get result. Throw RuntimeException while encountering an error.
	 *
	 * @return A List of result.
	 * @throws RuntimeException RuntimeException wrapping errors from user codes.
	 */
	public List<OUT> getResult() throws RuntimeException {
		if (error != null)
			throw new RuntimeException(error);
		return result;
	}

	public boolean isDone() {
		return isDone;
	}
}
