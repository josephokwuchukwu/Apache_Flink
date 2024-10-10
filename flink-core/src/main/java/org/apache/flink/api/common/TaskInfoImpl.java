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

package org.apache.flink.api.common;

import org.apache.flink.annotation.Internal;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The default implementation of {@link TaskInfo}. */
@Internal
public class TaskInfoImpl implements TaskInfo {

    private final String taskName;
    private final String taskNameWithSubtasks;
    private final String allocationIDAsString;
    private final int maxNumberOfParallelSubtasks;
    private final int indexOfSubtask;
    private final int numberOfParallelSubtasks;
    private final int attemptNumber;

    public TaskInfoImpl(
            String taskName,
            int maxNumberOfParallelSubtasks,
            int indexOfSubtask,
            int numberOfParallelSubtasks,
            int attemptNumber) {
        this(
                taskName,
                maxNumberOfParallelSubtasks,
                indexOfSubtask,
                numberOfParallelSubtasks,
                attemptNumber,
                "UNKNOWN");
    }

    public TaskInfoImpl(
            String taskName,
            int maxNumberOfParallelSubtasks,
            int indexOfSubtask,
            int numberOfParallelSubtasks,
            int attemptNumber,
            String allocationIDAsString) {

        checkArgument(indexOfSubtask >= 0, "Task index must be a non-negative number.");
        checkArgument(
                maxNumberOfParallelSubtasks >= 1, "Max parallelism must be a positive number.");
        checkArgument(numberOfParallelSubtasks >= 1, "Parallelism must be a positive number.");
        checkArgument(
                indexOfSubtask < numberOfParallelSubtasks,
                "Task index must be less than parallelism.");
        checkArgument(attemptNumber >= 0, "Attempt number must be a non-negative number.");
        this.taskName = checkNotNull(taskName, "Task Name must not be null.");
        this.maxNumberOfParallelSubtasks = maxNumberOfParallelSubtasks;
        this.indexOfSubtask = indexOfSubtask;
        this.numberOfParallelSubtasks = numberOfParallelSubtasks;
        this.attemptNumber = attemptNumber;
        this.taskNameWithSubtasks =
                taskName
                        + " ("
                        + (indexOfSubtask + 1)
                        + '/'
                        + numberOfParallelSubtasks
                        + ')'
                        + "#"
                        + attemptNumber;
        this.allocationIDAsString = checkNotNull(allocationIDAsString);
    }

    @Override
    public String getTaskName() {
        return this.taskName;
    }

    @Override
    public int getMaxNumberOfParallelSubtasks() {
        return this.maxNumberOfParallelSubtasks;
    }

    @Override
    public int getIndexOfThisSubtask() {
        return this.indexOfSubtask;
    }

    @Override
    public int getNumberOfParallelSubtasks() {
        return this.numberOfParallelSubtasks;
    }

    @Override
    public int getAttemptNumber() {
        return this.attemptNumber;
    }

    @Override
    public String getTaskNameWithSubtasks() {
        return this.taskNameWithSubtasks;
    }

    @Override
    public String getAllocationIDAsString() {
        return this.allocationIDAsString;
    }
}
