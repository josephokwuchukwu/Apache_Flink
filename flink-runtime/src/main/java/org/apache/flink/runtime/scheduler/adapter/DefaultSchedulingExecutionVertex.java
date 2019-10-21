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

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.api.common.InputDependencyConstraint;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverVertex;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link SchedulingExecutionVertex}.
 */
class DefaultSchedulingExecutionVertex
	implements SchedulingExecutionVertex<DefaultSchedulingExecutionVertex, DefaultSchedulingResultPartition>,
		FailoverVertex<DefaultSchedulingExecutionVertex, DefaultSchedulingResultPartition> {

	private final ExecutionVertexID executionVertexId;

	private final List<DefaultSchedulingResultPartition> consumedPartitions;

	private final List<DefaultSchedulingResultPartition> producedPartitions;

	private final Supplier<ExecutionState> stateSupplier;

	private final InputDependencyConstraint inputDependencyConstraint;

	DefaultSchedulingExecutionVertex(
			ExecutionVertexID executionVertexId,
			List<DefaultSchedulingResultPartition> producedPartitions,
			Supplier<ExecutionState> stateSupplier,
			InputDependencyConstraint constraint) {
		this.executionVertexId = checkNotNull(executionVertexId);
		this.consumedPartitions = new ArrayList<>();
		this.stateSupplier = checkNotNull(stateSupplier);
		this.producedPartitions = checkNotNull(producedPartitions);
		this.inputDependencyConstraint = checkNotNull(constraint);
	}

	@Override
	public ExecutionVertexID getId() {
		return executionVertexId;
	}

	@Override
	public ExecutionState getState() {
		return stateSupplier.get();
	}

	@Override
	public Iterable<DefaultSchedulingResultPartition> getConsumedResults() {
		return consumedPartitions;
	}

	@Override
	public Iterable<DefaultSchedulingResultPartition> getProducedResults() {
		return producedPartitions;
	}

	@Override
	public InputDependencyConstraint getInputDependencyConstraint() {
		return inputDependencyConstraint;
	}

	void addConsumedPartition(DefaultSchedulingResultPartition partition) {
		consumedPartitions.add(partition);
	}
}
