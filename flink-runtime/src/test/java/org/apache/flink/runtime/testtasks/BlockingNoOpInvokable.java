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

package org.apache.flink.runtime.testtasks;

import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.state.TaskStateHandles;

/**
 * A task that does nothing but blocks indefinitely, until the executing thread is interrupted.
 */
public class BlockingNoOpInvokable extends AbstractInvokable {

	public BlockingNoOpInvokable(Environment environment, TaskStateHandles taskStateHandles) {
		super(environment, taskStateHandles);
	}

	@Override
	public void invoke() throws Exception {
		final Object o = new Object();
		//noinspection SynchronizationOnLocalVariableOrMethodParameter
		synchronized (o) {
			//noinspection InfiniteLoopStatement
			while (true) {
				o.wait();
			}
		}
	}

	@Override
	public boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) throws Exception {
		throw new UnsupportedOperationException(String.format("triggerCheckpoint not supported by %s", this.getClass().getName()));
	}

	@Override
	public void triggerCheckpointOnBarrier(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, CheckpointMetrics checkpointMetrics) throws Exception {
		throw new UnsupportedOperationException(String.format("triggerCheckpointOnBarrier not supported by %s", this.getClass().getName()));
	}

	@Override
	public void abortCheckpointOnBarrier(long checkpointId, Throwable cause) throws Exception {
		throw new UnsupportedOperationException(String.format("abortCheckpointOnBarrier not supported by %s", this.getClass().getName()));
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		throw new UnsupportedOperationException(String.format("notifyCheckpointComplete not supported by %s", this.getClass().getName()));
	}
}
