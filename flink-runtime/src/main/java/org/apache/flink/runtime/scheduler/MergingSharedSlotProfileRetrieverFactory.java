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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Factory for {@link MergingSharedSlotProfileRetriever}.
 */
class MergingSharedSlotProfileRetrieverFactory implements SharedSlotProfileRetriever.SharedSlotProfileRetrieverFactory {
	private final PreferredLocationsRetriever preferredLocationsRetriever;

	private final Function<ExecutionVertexID, AllocationID> priorAllocationIdRetriever;

	MergingSharedSlotProfileRetrieverFactory(
			PreferredLocationsRetriever preferredLocationsRetriever,
			Function<ExecutionVertexID, AllocationID> priorAllocationIdRetriever) {
		this.preferredLocationsRetriever = Preconditions.checkNotNull(preferredLocationsRetriever);
		this.priorAllocationIdRetriever = Preconditions.checkNotNull(priorAllocationIdRetriever);
	}

	@Override
	public SharedSlotProfileRetriever createFromBulk(Set<ExecutionVertexID> bulk) {
		Set<AllocationID> allPriorAllocationIds = bulk
			.stream()
			.map(priorAllocationIdRetriever)
			.filter(Objects::nonNull)
			.collect(Collectors.toSet());
		return new MergingSharedSlotProfileRetriever(allPriorAllocationIds, bulk);
	}

	/**
	 * Computes a merged {@link SlotProfile} of an execution slot sharing group within a bulk to schedule.
	 */
	private class MergingSharedSlotProfileRetriever implements SharedSlotProfileRetriever {
		/**
		 * All previous {@link AllocationID}s of the bulk to schedule.
		 */
		private final Set<AllocationID> allBulkPriorAllocationIds;

		/**
		 * All {@link ExecutionVertexID}s of the bulk.
		 */
		private final Set<ExecutionVertexID> bulkExecutions;

		private MergingSharedSlotProfileRetriever(
				Set<AllocationID> allBulkPriorAllocationIds,
				Set<ExecutionVertexID> bulkExecutions) {
			this.allBulkPriorAllocationIds = Preconditions.checkNotNull(allBulkPriorAllocationIds);
			this.bulkExecutions = Preconditions.checkNotNull(bulkExecutions);
		}

		/**
		 * Computes a {@link SlotProfile} of an execution slot sharing group.
		 *
		 * <p>The preferred locations of the {@link SlotProfile} is a union of the preferred locations
		 * of all executions sharing the slot. The input locations within the bulk are ignored to avoid cyclic dependencies
		 * within the region, e.g. in case of all-to-all pipelined connections, so that the allocations do not block each other.
		 *
		 * <p>The preferred {@link AllocationID}s of the {@link SlotProfile} are all previous {@link AllocationID}s
		 * of all executions sharing the slot.
		 *
		 * <p>The {@link SlotProfile} also refers to all previous {@link AllocationID}s
		 * of all executions within the bulk.
		 *
		 * @param executionSlotSharingGroup executions sharing the slot.
		 * @param physicalSlotResourceProfile {@link ResourceProfile} of the slot.
		 * @return a future of the {@link SlotProfile} to allocate for the {@code executionSlotSharingGroup}.
		 */
		@Override
		public CompletableFuture<SlotProfile> getSlotProfileFuture(
				ExecutionSlotSharingGroup executionSlotSharingGroup,
				ResourceProfile physicalSlotResourceProfile) {
			Collection<AllocationID> priorAllocations = new HashSet<>();
			Collection<CompletableFuture<Collection<TaskManagerLocation>>> preferredLocationsPerExecution = new ArrayList<>();

			Set<ExecutionVertexID> groupExecutionsInBulk = new HashSet<>(executionSlotSharingGroup.getExecutionVertexIds());
			groupExecutionsInBulk.retainAll(bulkExecutions);

			for (ExecutionVertexID execution : groupExecutionsInBulk) {
				priorAllocations.add(priorAllocationIdRetriever.apply(execution));
				preferredLocationsPerExecution.add(preferredLocationsRetriever
					.getPreferredLocations(execution, bulkExecutions));
			}

			CompletableFuture<Collection<TaskManagerLocation>> preferredLocationsFuture = FutureUtils
				.combineAll(preferredLocationsPerExecution)
				.thenApply(executionPreferredLocations ->
					executionPreferredLocations.stream().flatMap(Collection::stream).collect(Collectors.toList()));

			return preferredLocationsFuture.thenApply(
				preferredLocations ->
					SlotProfile.priorAllocation(
						physicalSlotResourceProfile,
						physicalSlotResourceProfile,
						preferredLocations,
						priorAllocations,
						allBulkPriorAllocationIds));
		}
	}
}
