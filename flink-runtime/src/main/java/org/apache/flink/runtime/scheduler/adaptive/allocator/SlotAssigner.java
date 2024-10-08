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

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import static org.apache.flink.util.Preconditions.checkState;

/** The Interface for assigning slots to slot sharing groups. */
@Internal
public interface SlotAssigner {

    Collection<SlotAssignment> assignSlots(
            JobInformation jobInformation,
            Collection<? extends SlotInfo> freeSlots,
            VertexParallelism vertexParallelism,
            JobAllocationsInformation previousAllocations);

    /**
     * Pick the target slots to assign with the requested groups.
     *
     * @param slotsByTaskExecutor slots per task executor.
     * @param requestedGroups the number of the request execution slot sharing groups.
     * @return the target slots that are distributed on the minimal task executors.
     */
    default Collection<? extends SlotInfo> pickSlotsInMinimalTaskExecutors(
            Map<TaskManagerLocation, ? extends Set<? extends SlotInfo>> slotsByTaskExecutor,
            int requestedGroups,
            Iterator<TaskManagerLocation> sortedTaskExecutors) {
        final List<SlotInfo> pickedSlots = new ArrayList<>();
        while (pickedSlots.size() < requestedGroups) {
            Set<? extends SlotInfo> slotInfos = slotsByTaskExecutor.get(sortedTaskExecutors.next());
            pickedSlots.addAll(slotInfos);
        }
        return pickedSlots;
    }

    /**
     * Sort the task executors with the order that aims to priority assigning requested groups on
     * it.
     *
     * @param taskManagerLocations task executors to sort.
     * @param taskExecutorComparator the comparator to compare the target task executors.
     * @return The sorted task executors list with the specified order by the comparator.
     */
    static Iterator<TaskManagerLocation> sortTaskExecutors(
            Collection<TaskManagerLocation> taskManagerLocations,
            Comparator<TaskManagerLocation> taskExecutorComparator) {
        return taskManagerLocations.stream().sorted(taskExecutorComparator).iterator();
    }

    static Map<TaskManagerLocation, ? extends Set<? extends SlotInfo>> getSlotsPerTaskExecutor(
            Collection<? extends SlotInfo> slots) {
        return slots.stream()
                .collect(
                        Collectors.groupingBy(
                                SlotInfo::getTaskManagerLocation,
                                Collectors.mapping(identity(), Collectors.toSet())));
    }

    static List<ExecutionSlotSharingGroup> createExecutionSlotSharingGroups(
            VertexParallelism vertexParallelism, SlotSharingGroup slotSharingGroup) {
        final Map<Integer, Set<ExecutionVertexID>> sharedSlotToVertexAssignment = new HashMap<>();
        slotSharingGroup
                .getJobVertexIds()
                .forEach(
                        jobVertexId -> {
                            int parallelism = vertexParallelism.getParallelism(jobVertexId);
                            for (int subtaskIdx = 0; subtaskIdx < parallelism; subtaskIdx++) {
                                sharedSlotToVertexAssignment
                                        .computeIfAbsent(subtaskIdx, ignored -> new HashSet<>())
                                        .add(new ExecutionVertexID(jobVertexId, subtaskIdx));
                            }
                        });
        return sharedSlotToVertexAssignment.values().stream()
                .map(SlotSharingSlotAllocator.ExecutionSlotSharingGroup::new)
                .collect(Collectors.toList());
    }

    static void checkSlotsSufficient(
            JobInformation jobInformation, Collection<? extends SlotInfo> freeSlots) {
        checkState(
                freeSlots.size() >= jobInformation.getSlotSharingGroups().size(),
                "Not enough slots to allocate all the slot sharing groups (have: %s, need: %s)",
                freeSlots.size(),
                jobInformation.getSlotSharingGroups().size());
    }
}
