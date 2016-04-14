/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.streaming.api.watermark.Watermark;

public abstract class MockSourceContext<T> implements SourceFunction.SourceContext<T> {
	protected Object lock;

	public MockSourceContext() {
		this(new Object());
	}
	
	public MockSourceContext(Object lock) {
		this.lock = lock;
	}

	@Override
	public void collectWithTimestamp(T element, long timestamp) {
		collect(element);
	}

	@Override
	public void emitWatermark(Watermark mark) {}

	@Override
	public Object getCheckpointLock() {
		return this.lock;
	}

	@Override
	public void close() {}
}
