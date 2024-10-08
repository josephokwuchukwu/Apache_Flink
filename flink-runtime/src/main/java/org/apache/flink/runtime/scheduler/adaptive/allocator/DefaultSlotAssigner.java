/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.runtime.scheduler.adaptive.allocator.SlotAssigner.checkSlotsSufficient;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.SlotAssigner.createExecutionSlotSharingGroups;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.SlotAssigner.getSlotsPerTaskExecutor;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.SlotAssigner.sortTaskExecutors;

/** Simple {@link SlotAssigner} that treats all slots and slot sharing groups equally. */
public class DefaultSlotAssigner implements SlotAssigner {

    @Override
    public Collection<SlotAssignment> assignSlots(
            JobInformation jobInformation,
            Collection<? extends SlotInfo> freeSlots,
            VertexParallelism vertexParallelism,
            JobAllocationsInformation previousAllocations) {
        checkSlotsSufficient(jobInformation, freeSlots);

        final List<ExecutionSlotSharingGroup> allGroups = new ArrayList<>();
        for (SlotSharingGroup slotSharingGroup : jobInformation.getSlotSharingGroups()) {
            allGroups.addAll(createExecutionSlotSharingGroups(vertexParallelism, slotSharingGroup));
        }

        Collection<? extends SlotInfo> pickedSlots = freeSlots;
        if (freeSlots.size() > allGroups.size()) {
            final Map<TaskManagerLocation, ? extends Set<? extends SlotInfo>> slotsPerTaskExecutor =
                    getSlotsPerTaskExecutor(freeSlots);
            pickedSlots =
                    pickSlotsInMinimalTaskExecutors(
                            slotsPerTaskExecutor,
                            allGroups.size(),
                            getSortedTaskExecutors(slotsPerTaskExecutor));
        }

        Iterator<? extends SlotInfo> iterator = pickedSlots.iterator();
        Collection<SlotAssignment> assignments = new ArrayList<>();
        for (ExecutionSlotSharingGroup group : allGroups) {
            assignments.add(new SlotAssignment(iterator.next(), group));
        }
        return assignments;
    }

    /**
     * In order to minimize the using of task executors at the resource manager side in the
     * session-mode and release more task executors in a timely manner, it is a good choice to
     * prioritize selecting slots on task executors with the least available slots. This strategy
     * also ensures that relatively fewer task executors can be used in application-mode.
     */
    @VisibleForTesting
    Iterator<TaskManagerLocation> getSortedTaskExecutors(
            Map<TaskManagerLocation, ? extends Set<? extends SlotInfo>> slotsPerTaskExecutor) {
        final Comparator<TaskManagerLocation> taskExecutorComparator =
                Comparator.comparingInt(tml -> slotsPerTaskExecutor.get(tml).size());
        return sortTaskExecutors(slotsPerTaskExecutor.keySet(), taskExecutorComparator);
    }
}
