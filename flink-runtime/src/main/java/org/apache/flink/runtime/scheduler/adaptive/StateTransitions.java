/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.runtime.checkpoint.CheckpointScheduling;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * An interface covering all possible {@link State} transitions. The main purpose is to align the
 * transition methods between different contexts.
 */
public interface StateTransitions {

    interface ToCancelling extends StateTransitions {

        /**
         * Transitions into the {@link Canceling} state.
         *
         * @param executionGraph executionGraph to pass to the {@link Canceling} state
         * @param executionGraphHandler executionGraphHandler to pass to the {@link Canceling} state
         * @param operatorCoordinatorHandler operatorCoordinatorHandler to pass to the {@link
         *     Canceling} state
         */
        void goToCanceling(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler);
    }

    interface ToCreatingExecutionGraph extends StateTransitions {

        /** Transitions into the {@link CreatingExecutionGraph} state. */
        void goToCreatingExecutionGraph();
    }

    interface ToExecuting extends StateTransitions {

        /**
         * Transitions into the {@link Executing} state.
         *
         * @param executionGraph executionGraph to pass to the {@link Executing} state
         * @param executionGraphHandler executionGraphHandler to pass to the {@link Executing} state
         * @param operatorCoordinatorHandler operatorCoordinatorHandler to pass to the {@link
         *     Executing} state
         */
        void goToExecuting(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler);
    }

    interface ToFinished extends StateTransitions {

        /**
         * Transitions into the {@link Finished} state.
         *
         * @param archivedExecutionGraph archivedExecutionGraph which is passed to the {@link
         *     Finished} state
         */
        void goToFinished(ArchivedExecutionGraph archivedExecutionGraph);
    }

    interface ToFailing extends StateTransitions {

        /**
         * Transitions into the {@link Failing} state.
         *
         * @param executionGraph executionGraph to pass to the {@link Failing} state
         * @param executionGraphHandler executionGraphHandler to pass to the {@link Failing} state
         * @param operatorCoordinatorHandler operatorCoordinatorHandler to pass to the {@link
         *     Failing} state
         * @param failureCause failureCause describing why the job execution failed
         */
        void goToFailing(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Throwable failureCause);
    }

    interface ToRestarting extends StateTransitions {

        /**
         * Transitions into the {@link Restarting} state.
         *
         * @param executionGraph executionGraph to pass to the {@link Restarting} state
         * @param executionGraphHandler executionGraphHandler to pass to the {@link Restarting}
         *     state
         * @param operatorCoordinatorHandler operatorCoordinatorHandler to pas to the {@link
         *     Restarting} state
         * @param backoffTime backoffTime to wait before transitioning to the {@link Restarting}
         *     state
         */
        void goToRestarting(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Duration backoffTime);
    }

    interface ToStopWithSavepoint extends StateTransitions {

        /**
         * Transitions into the {@link StopWithSavepoint} state.
         *
         * @param executionGraph executionGraph to pass to the {@link StopWithSavepoint} state
         * @param executionGraphHandler executionGraphHandler to pass to the {@link
         *     StopWithSavepoint} state
         * @param operatorCoordinatorHandler operatorCoordinatorHandler to pass to the {@link
         *     StopWithSavepoint} state
         * @param savepointFuture Future for the savepoint to complete.
         * @return Location of the savepoint.
         */
        CompletableFuture<String> goToStopWithSavepoint(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                CheckpointScheduling checkpointScheduling,
                CompletableFuture<String> savepointFuture);
    }

    interface ToWaitingForResources extends StateTransitions {

        /** Transitions into the {@link WaitingForResources} state. */
        void goToWaitingForResources();
    }
}
