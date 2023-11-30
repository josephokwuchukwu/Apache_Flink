/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.deployment.TaskDeployResult;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/** Executor to batch deploy {@link Execution}. */
public class BatchExecutionDeployExecutor implements ExecutionDeployExecutor {

    private Logger log;

    private ExecutionOperations executionOperations;

    private final ScheduledExecutor futureExecutor;

    private final Time rpcTimeout;

    private final Map<TaskManagerLocation, TaskBatchDeploymentGroup> taskManagerLocationMap;

    private final Map<ExecutionAttemptID, CompletableFuture<Acknowledge>> deployResultMap;

    public BatchExecutionDeployExecutor(
            Logger log,
            ExecutionOperations executionOperations,
            ScheduledExecutor futureExecutor,
            Time rpcTimeout) {
        this.log = log;
        this.executionOperations = executionOperations;
        this.futureExecutor = futureExecutor;
        this.rpcTimeout = rpcTimeout;
        this.taskManagerLocationMap = new ConcurrentHashMap<>();
        this.deployResultMap = new ConcurrentHashMap<>();
    }

    @Override
    public void executeDeploy(Execution execution) throws JobException {
        executionOperations.deploy(execution, deployTask());
    }

    @Override
    public void flushDeploy() {
        for (TaskBatchDeploymentGroup taskBatchDeploymentGroup : taskManagerLocationMap.values()) {
            Collection<TaskDeploymentDescriptor> taskDeploymentDescriptors =
                    taskBatchDeploymentGroup.getTaskDeploymentDescriptors();
            CompletableFuture.supplyAsync(
                            () ->
                                    taskBatchDeploymentGroup
                                            .getTaskManagerGateway()
                                            .submitTasks(taskDeploymentDescriptors, rpcTimeout),
                            futureExecutor)
                    .thenCompose(Function.identity())
                    .whenComplete(
                            (list, deployThrowable) -> {
                                if (deployThrowable != null) {
                                    for (CompletableFuture<Acknowledge> future :
                                            deployResultMap.values()) {
                                        future.completeExceptionally(deployThrowable);
                                    }
                                    return;
                                }
                                for (TaskDeployResult taskDeployResult : list) {
                                    final Throwable taskDeployResultThrowable =
                                            taskDeployResult.getThrowable();
                                    if (taskDeployResultThrowable != null) {
                                        deployResultMap
                                                .get(taskDeployResult.getExecutionAttemptID())
                                                .completeExceptionally(taskDeployResultThrowable);
                                    } else {
                                        deployResultMap
                                                .get(taskDeployResult.getExecutionAttemptID())
                                                .complete(Acknowledge.get());
                                    }
                                }
                            });
        }
    }

    private Function<Execution, CompletableFuture<Acknowledge>> deployTask() {
        return (execution) -> {
            try {
                final TaskDeploymentDescriptor deploymentDescriptor =
                        execution.getDeploymentDescriptor();
                final LogicalSlot logicalSlot = execution.getAssignedResource();
                final TaskManagerLocation taskManagerLocation =
                        logicalSlot.getTaskManagerLocation();
                taskManagerLocationMap
                        .computeIfAbsent(
                                taskManagerLocation,
                                t ->
                                        new TaskBatchDeploymentGroup(
                                                logicalSlot.getTaskManagerGateway()))
                        .addTaskDeploymentDescriptor(deploymentDescriptor);
                final CompletableFuture<Acknowledge> deployResult = new CompletableFuture<>();
                deployResultMap.put(deploymentDescriptor.getExecutionAttemptId(), deployResult);
                return deployResult;
            } catch (Exception e) {
                return FutureUtils.completedExceptionally(e);
            }
        };
    }

    /** Factory to instantiate the {@link BatchExecutionDeployExecutor}. */
    public static class Factory implements ExecutionDeployExecutor.Factory {
        @Override
        public ExecutionDeployExecutor createInstance(
                Logger log,
                ExecutionOperations executionOperations,
                ScheduledExecutor scheduledExecutor,
                Time rpcTimeout) {
            return new BatchExecutionDeployExecutor(
                    log, executionOperations, scheduledExecutor, rpcTimeout);
        }
    }
}
