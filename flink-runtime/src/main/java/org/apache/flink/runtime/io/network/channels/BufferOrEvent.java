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

package org.apache.flink.runtime.io.network.channels;

import org.apache.flink.runtime.event.task.AbstractEvent;
import org.apache.flink.runtime.io.network.Buffer;

/**
 * Either type for {@link org.apache.flink.runtime.io.network.Buffer} and {@link AbstractEvent}.
 */
public class BufferOrEvent {
	
	private final Buffer buffer;
	
	private final AbstractEvent event;
	
	public BufferOrEvent(Buffer buffer) {
		this.buffer = buffer;
		this.event = null;
	}
	
	public BufferOrEvent(AbstractEvent event) {
		this.buffer = null;
		this.event = event;
	}
	
	public boolean isBuffer() {
		return this.buffer != null;
	}
	
	public boolean isEvent() {
		return this.event != null;
	}
	
	public Buffer getBuffer() {
		return this.buffer;
	}
	
	public AbstractEvent getEvent() {
		return this.event;
	}
}
