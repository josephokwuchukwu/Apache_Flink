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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.checkpoint.CheckpointPlanCalculatorContext;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link CheckpointPlanCalculatorContext} implementation based on the information from an {@link
 * ExecutionGraph}.
 */
public class ExecutionGraphCheckpointPlanCalculatorContext
        implements CheckpointPlanCalculatorContext {

    private final ExecutionGraph executionGraph;

    public ExecutionGraphCheckpointPlanCalculatorContext(ExecutionGraph executionGraph) {
        this.executionGraph = checkNotNull(executionGraph);
    }

    @Override
    public ScheduledExecutor getMainExecutor() {
        return executionGraph.getJobMasterMainThreadExecutor();
    }

    @Override
    public boolean hasFinishedTasks() {
        return executionGraph.getNumFinishedVertices() > 0;
    }
}
