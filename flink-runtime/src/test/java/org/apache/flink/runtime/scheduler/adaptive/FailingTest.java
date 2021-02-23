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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.function.Consumer;

import static org.apache.flink.runtime.scheduler.adaptive.WaitingForResourcesTest.assertNonNull;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link Failing} state of the {@link AdaptiveScheduler}. */
public class FailingTest extends TestLogger {

    private final Throwable testFailureCause = new RuntimeException();

    @Test
    public void testFailingStateOnEnter() throws Exception {
        try (MockFailingContext ctx = new MockFailingContext()) {
            MockExecutionGraph meg = new MockExecutionGraph();
            Failing failing = createFailingState(ctx, meg);
            failing.onEnter();
            assertThat(meg.isFailing(), is(true));
            ctx.assertNoStateTransition();
        }
    }

    @Test
    public void testTransitionToFailedWhenFailingCompletes() throws Exception {
        try (MockFailingContext ctx = new MockFailingContext()) {
            MockExecutionGraph meg = new MockExecutionGraph();
            Failing failing = createFailingState(ctx, meg);
            failing.onEnter(); // transition from RUNNING to FAILING
            ctx.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState(), is(JobStatus.FAILED)));
            meg.completeCancellation(); // transition to FAILED
        }
    }

    @Test
    public void testTransitionToCancelingOnCancel() throws Exception {
        try (MockFailingContext ctx = new MockFailingContext()) {
            MockExecutionGraph meg = new MockExecutionGraph();
            Failing failing = createFailingState(ctx, meg);
            ctx.setExpectCanceling(assertNonNull());
            failing.onEnter();
            failing.cancel();
        }
    }

    @Test
    public void testTransitionToFinishedOnSuspend() throws Exception {
        try (MockFailingContext ctx = new MockFailingContext()) {
            MockExecutionGraph meg = new MockExecutionGraph();
            Failing failing = createFailingState(ctx, meg);
            ctx.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState(), is(JobStatus.SUSPENDED)));

            failing.onEnter();
            failing.suspend(new RuntimeException("suspend"));
        }
    }

    @Test
    public void testIgnoreGlobalFailure() throws Exception {
        try (MockFailingContext ctx = new MockFailingContext()) {
            MockExecutionGraph meg = new MockExecutionGraph();
            Failing failing = createFailingState(ctx, meg);
            failing.onEnter();
            failing.handleGlobalFailure(new RuntimeException());
            ctx.assertNoStateTransition();
        }
    }

    @Test
    public void testTaskFailuresAreIgnored() throws Exception {
        try (MockFailingContext ctx = new MockFailingContext()) {
            MockExecutionGraph meg = new MockExecutionGraph();
            Failing failing = createFailingState(ctx, meg);
            failing.onEnter();
            // register execution at EG
            ExecutingTest.MockExecutionJobVertex ejv =
                    new ExecutingTest.MockExecutionJobVertex(failing.getExecutionGraph());
            TaskExecutionStateTransition update =
                    new TaskExecutionStateTransition(
                            new TaskExecutionState(
                                    ejv.getMockExecutionVertex()
                                            .getCurrentExecutionAttempt()
                                            .getAttemptId(),
                                    ExecutionState.FAILED,
                                    new RuntimeException()));
            failing.updateTaskExecutionState(update);
            ctx.assertNoStateTransition();
        }
    }

    @Test
    public void testStateDoesNotExposeGloballyTerminalExecutionGraph() throws Exception {
        try (MockFailingContext ctx = new MockFailingContext()) {
            MockExecutionGraph meg = new MockExecutionGraph();
            Failing failing = createFailingState(ctx, meg);

            // ideally we'd delay the async call to #onGloballyTerminalState instead, but the
            // context does not support that
            ctx.setExpectFinished(eg -> {});
            failing.onEnter();

            meg.completeCancellation();

            // this is just a sanity check for the test
            assertThat(meg.getState(), is(JobStatus.FAILED));

            assertThat(failing.getJobStatus(), is(JobStatus.FAILING));
            assertThat(failing.getJob().getState(), is(JobStatus.FAILING));
            assertThat(failing.getJob().getStatusTimestamp(JobStatus.FAILED), is(0L));
        }
    }

    private Failing createFailingState(MockFailingContext ctx, ExecutionGraph executionGraph) {
        final ExecutionGraphHandler executionGraphHandler =
                new ExecutionGraphHandler(
                        executionGraph,
                        log,
                        ctx.getMainThreadExecutor(),
                        ctx.getMainThreadExecutor());
        final OperatorCoordinatorHandler operatorCoordinatorHandler =
                new OperatorCoordinatorHandler(
                        executionGraph,
                        (throwable) -> {
                            throw new RuntimeException("Error in test", throwable);
                        });
        executionGraph.transitionToRunning();
        return new Failing(
                ctx,
                executionGraph,
                executionGraphHandler,
                operatorCoordinatorHandler,
                log,
                testFailureCause);
    }

    private static class MockFailingContext extends MockStateWithExecutionGraphContext
            implements Failing.Context {

        private final StateValidator<ExecutingTest.CancellingArguments> cancellingStateValidator =
                new StateValidator<>("cancelling");

        public void setExpectCanceling(Consumer<ExecutingTest.CancellingArguments> asserter) {
            cancellingStateValidator.expectInput(asserter);
        }

        @Override
        public void goToCanceling(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler) {
            cancellingStateValidator.validateInput(
                    new ExecutingTest.CancellingArguments(
                            executionGraph, executionGraphHandler, operatorCoordinatorHandler));
            hadStateTransition = true;
        }

        @Override
        public void close() throws Exception {
            super.close();
            cancellingStateValidator.close();
        }
    }
}
