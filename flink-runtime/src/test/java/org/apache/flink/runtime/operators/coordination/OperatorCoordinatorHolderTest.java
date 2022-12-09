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

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.EventReceivingTasks.EventWithSubtask;
import org.apache.flink.runtime.scheduler.GlobalFailureHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A test that ensures the before/after conditions around event sending and checkpoint are met.
 * concurrency
 */
public class OperatorCoordinatorHolderTest extends TestLogger {

    private final GlobalFailureHandler globalFailureHandler = (t) -> globalFailure = t;
    private Throwable globalFailure;

    @After
    public void checkNoGlobalFailure() throws Exception {
        if (globalFailure != null) {
            ExceptionUtils.rethrowException(globalFailure);
        }
    }

    // ------------------------------------------------------------------------

    @Test
    public void checkpointFutureInitiallyNotDone() throws Exception {
        final EventReceivingTasks tasks = EventReceivingTasks.createForRunningTasks();
        final OperatorCoordinatorHolder holder =
                createCoordinatorHolder(tasks, TestingOperatorCoordinator::new);

        final CompletableFuture<byte[]> checkpointFuture = new CompletableFuture<>();
        holder.checkpointCoordinator(1L, checkpointFuture);

        assertThat(checkpointFuture).isNotDone();
    }

    @Test
    public void completedCheckpointFuture() throws Exception {
        final EventReceivingTasks tasks = EventReceivingTasks.createForRunningTasks();
        final OperatorCoordinatorHolder holder =
                createCoordinatorHolder(tasks, TestingOperatorCoordinator::new);

        final byte[] testData = new byte[] {11, 22, 33, 44};

        final CompletableFuture<byte[]> checkpointFuture = new CompletableFuture<>();
        holder.checkpointCoordinator(9L, checkpointFuture);
        sendAcknowledgeCloseGatewayEvents(holder, 9L);
        getCoordinator(holder).getLastTriggeredCheckpoint().complete(testData);

        assertThat(checkpointFuture).isDone();
        assertThat(checkpointFuture.get()).containsExactly(testData);
    }

    @Test
    public void blockCheckpointAtAcknowledgeCloseGatewayEvent() throws Exception {
        final EventReceivingTasks tasks = EventReceivingTasks.createForRunningTasks();
        final OperatorCoordinatorHolder holder =
                createCoordinatorHolder(tasks, TestingOperatorCoordinator::new);

        final byte[] testData = new byte[] {11, 22, 33, 44};

        final CompletableFuture<byte[]> checkpointFuture = new CompletableFuture<>();
        holder.checkpointCoordinator(1L, checkpointFuture);
        assertThat(getCoordinator(holder).hasTriggeredCheckpoint()).isFalse();

        sendAcknowledgeCloseGatewayEvents(holder, 1L);
        assertThat(getCoordinator(holder).hasTriggeredCheckpoint()).isTrue();

        getCoordinator(holder).getLastTriggeredCheckpoint().complete(testData);
        assertThat(checkpointFuture).isDone();
        assertThat(checkpointFuture.get()).containsExactly(testData);
    }

    @Test
    public void eventsBeforeCheckpointFutureCompletionPassThrough() throws Exception {
        final EventReceivingTasks tasks = EventReceivingTasks.createForRunningTasks();
        final OperatorCoordinatorHolder holder =
                createCoordinatorHolder(tasks, TestingOperatorCoordinator::new);

        holder.checkpointCoordinator(1L, new CompletableFuture<>());
        getCoordinator(holder).getSubtaskGateway(1).sendEvent(new TestOperatorEvent(1));
        holder.handleEventFromOperator(1, 0, new OpenGatewayEvent(1L, 1));

        assertThat(tasks.getSentEventsForSubtask(1))
                .containsExactly(new CloseGatewayEvent(1L, 1), new TestOperatorEvent(1));
    }

    @Test
    public void eventsAreBlockedAfterCheckpointFutureCompletes() throws Exception {
        final EventReceivingTasks tasks = EventReceivingTasks.createForRunningTasks();
        final OperatorCoordinatorHolder holder =
                createCoordinatorHolder(tasks, TestingOperatorCoordinator::new);

        triggerAndCompleteCheckpoint(holder, 10L);
        getCoordinator(holder).getSubtaskGateway(0).sendEvent(new TestOperatorEvent(1337));

        assertThat(tasks.getNumberOfSentEvents()).isEqualTo(holder.currentParallelism());
    }

    @Test
    public void abortedCheckpointReleasesBlockedEvents() throws Exception {
        final EventReceivingTasks tasks = EventReceivingTasks.createForRunningTasks();
        final OperatorCoordinatorHolder holder =
                createCoordinatorHolder(tasks, TestingOperatorCoordinator::new);

        triggerAndCompleteCheckpoint(holder, 123L);
        getCoordinator(holder).getSubtaskGateway(0).sendEvent(new TestOperatorEvent(1337));
        holder.abortCurrentTriggering();

        assertThat(tasks.getSentEventsForSubtask(0))
                .containsExactly(
                        new CloseGatewayEvent(123L, 0),
                        new OpenGatewayEvent(123L, 0),
                        new TestOperatorEvent(1337));
    }

    @Test
    public void openGatewayEventReleasesBlockedEvents() throws Exception {
        final EventReceivingTasks tasks = EventReceivingTasks.createForRunningTasks();
        final OperatorCoordinatorHolder holder =
                createCoordinatorHolder(tasks, TestingOperatorCoordinator::new);

        triggerAndCompleteCheckpoint(holder, 1111L);
        getCoordinator(holder).getSubtaskGateway(0).sendEvent(new TestOperatorEvent(1337));
        holder.handleEventFromOperator(0, 0, new OpenGatewayEvent(1111L, 0));

        assertThat(tasks.getSentEventsForSubtask(0))
                .containsExactly(new CloseGatewayEvent(1111L, 0), new TestOperatorEvent(1337));
    }

    @Test
    public void restoreOpensGatewayEvents() throws Exception {
        final EventReceivingTasks tasks = EventReceivingTasks.createForRunningTasks();
        final OperatorCoordinatorHolder holder =
                createCoordinatorHolder(tasks, TestingOperatorCoordinator::new);

        triggerAndCompleteCheckpoint(holder, 1000L);
        sendOpenGatewayEvents(holder, 1000L);
        holder.notifyCheckpointComplete(1000L);
        holder.resetToCheckpoint(1L, new byte[0]);
        getCoordinator(holder).getSubtaskGateway(1).sendEvent(new TestOperatorEvent(999));

        assertThat(tasks.getSentEventsForSubtask(1))
                .containsExactly(new CloseGatewayEvent(1000L, 1), new TestOperatorEvent(999));
    }

    @Test
    public void lateCompleteCheckpointFutureDoesNotBlockEvents() throws Exception {
        final EventReceivingTasks tasks = EventReceivingTasks.createForRunningTasks();
        final OperatorCoordinatorHolder holder =
                createCoordinatorHolder(tasks, TestingOperatorCoordinator::new);

        final CompletableFuture<byte[]> holderFuture = new CompletableFuture<>();
        holder.checkpointCoordinator(1000L, holderFuture);
        sendAcknowledgeCloseGatewayEvents(holder, 1000L);

        final CompletableFuture<byte[]> future1 =
                getCoordinator(holder).getLastTriggeredCheckpoint();
        holder.abortCurrentTriggering();

        triggerAndCompleteCheckpoint(holder, 1010L);
        sendOpenGatewayEvents(holder, 1010L);
        holder.notifyCheckpointComplete(1010L);

        future1.complete(new byte[0]);

        getCoordinator(holder).getSubtaskGateway(0).sendEvent(new TestOperatorEvent(123));

        assertThat(tasks.events)
                .containsExactly(
                        new EventWithSubtask(new CloseGatewayEvent(1000L, 0), 0),
                        new EventWithSubtask(new CloseGatewayEvent(1000L, 1), 1),
                        new EventWithSubtask(new CloseGatewayEvent(1000L, 2), 2),
                        new EventWithSubtask(new OpenGatewayEvent(1000L, 0), 0),
                        new EventWithSubtask(new OpenGatewayEvent(1000L, 1), 1),
                        new EventWithSubtask(new OpenGatewayEvent(1000L, 2), 2),
                        new EventWithSubtask(new CloseGatewayEvent(1010L, 0), 0),
                        new EventWithSubtask(new CloseGatewayEvent(1010L, 1), 1),
                        new EventWithSubtask(new CloseGatewayEvent(1010L, 2), 2),
                        new EventWithSubtask(new TestOperatorEvent(123), 0));
    }

    @Test
    public void triggerConcurrentCheckpoints() throws Exception {
        final EventReceivingTasks tasks = EventReceivingTasks.createForRunningTasks();
        final OperatorCoordinatorHolder holder =
                createCoordinatorHolder(tasks, TestingOperatorCoordinator::new);

        triggerAndCompleteCheckpoint(holder, 1111L);
        getCoordinator(holder).getSubtaskGateway(0).sendEvent(new TestOperatorEvent(1337));
        triggerAndCompleteCheckpoint(holder, 1112L);
        getCoordinator(holder).getSubtaskGateway(0).sendEvent(new TestOperatorEvent(1338));
        assertThat(tasks.getSentEventsForSubtask(0))
                .containsExactly(new CloseGatewayEvent(1111L, 0), new CloseGatewayEvent(1112L, 0));

        holder.handleEventFromOperator(0, 0, new OpenGatewayEvent(1111L, 0));
        assertThat(tasks.getSentEventsForSubtask(0))
                .containsExactly(
                        new CloseGatewayEvent(1111L, 0),
                        new CloseGatewayEvent(1112L, 0),
                        new TestOperatorEvent(1337));

        holder.handleEventFromOperator(0, 0, new OpenGatewayEvent(1112L, 0));
        assertThat(tasks.getSentEventsForSubtask(0))
                .containsExactly(
                        new CloseGatewayEvent(1111L, 0),
                        new CloseGatewayEvent(1112L, 0),
                        new TestOperatorEvent(1337),
                        new TestOperatorEvent(1338));
    }

    @Test
    public void takeCheckpointAfterSuccessfulCheckpoint() throws Exception {
        final EventReceivingTasks tasks = EventReceivingTasks.createForRunningTasks();
        final OperatorCoordinatorHolder holder =
                createCoordinatorHolder(tasks, TestingOperatorCoordinator::new);

        getCoordinator(holder).getSubtaskGateway(0).sendEvent(new TestOperatorEvent(0));

        triggerAndCompleteCheckpoint(holder, 22L);
        getCoordinator(holder).getSubtaskGateway(0).sendEvent(new TestOperatorEvent(1));
        sendOpenGatewayEvents(holder, 22L);

        getCoordinator(holder).getSubtaskGateway(0).sendEvent(new TestOperatorEvent(2));

        triggerAndCompleteCheckpoint(holder, 23L);
        getCoordinator(holder).getSubtaskGateway(0).sendEvent(new TestOperatorEvent(3));
        sendOpenGatewayEvents(holder, 23L);

        assertThat(tasks.getSentEventsForSubtask(0))
                .containsExactly(
                        new TestOperatorEvent(0),
                        new CloseGatewayEvent(22L, 0),
                        new TestOperatorEvent(1),
                        new TestOperatorEvent(2),
                        new CloseGatewayEvent(23L, 0),
                        new TestOperatorEvent(3));
    }

    @Test
    public void takeCheckpointAfterAbortedCheckpoint() throws Exception {
        final EventReceivingTasks tasks = EventReceivingTasks.createForRunningTasks();
        final OperatorCoordinatorHolder holder =
                createCoordinatorHolder(tasks, TestingOperatorCoordinator::new);

        getCoordinator(holder).getSubtaskGateway(0).sendEvent(new TestOperatorEvent(0));

        triggerAndCompleteCheckpoint(holder, 22L);
        getCoordinator(holder).getSubtaskGateway(0).sendEvent(new TestOperatorEvent(1));
        holder.abortCurrentTriggering();

        getCoordinator(holder).getSubtaskGateway(0).sendEvent(new TestOperatorEvent(2));

        triggerAndCompleteCheckpoint(holder, 23L);
        getCoordinator(holder).getSubtaskGateway(0).sendEvent(new TestOperatorEvent(3));
        sendOpenGatewayEvents(holder, 23L);

        assertThat(tasks.getSentEventsForSubtask(0))
                .containsExactly(
                        new TestOperatorEvent(0),
                        new CloseGatewayEvent(22L, 0),
                        new OpenGatewayEvent(22L, 0),
                        new TestOperatorEvent(1),
                        new TestOperatorEvent(2),
                        new CloseGatewayEvent(23L, 0),
                        new TestOperatorEvent(3));
    }

    @Test
    public void testFailingJobMultipleTimesNotCauseCascadingJobFailure() throws Exception {
        Function<OperatorCoordinator.Context, OperatorCoordinator> coordinatorProvider =
                context ->
                        new TestingOperatorCoordinator(context) {
                            @Override
                            public void handleEventFromOperator(
                                    int subtask, int attemptNumber, OperatorEvent event) {
                                context.failJob(new RuntimeException("Artificial Exception"));
                            }
                        };
        final EventReceivingTasks tasks = EventReceivingTasks.createForRunningTasks();
        final OperatorCoordinatorHolder holder =
                createCoordinatorHolder(tasks, coordinatorProvider);

        holder.handleEventFromOperator(0, 0, new TestOperatorEvent());
        assertThat(globalFailure).isNotNull();
        final Throwable firstGlobalFailure = globalFailure;

        holder.handleEventFromOperator(1, 0, new TestOperatorEvent());
        assertThat(firstGlobalFailure)
                .as(
                        "The global failure should be the same instance because the context"
                                + "should only take the first request from the coordinator to fail the job.")
                .isEqualTo(globalFailure);

        holder.resetToCheckpoint(0L, new byte[0]);
        holder.handleEventFromOperator(1, 1, new TestOperatorEvent());
        assertThat(firstGlobalFailure)
                .as("The new failures should be propagated after the coordinator " + "is reset.")
                .isNotEqualTo(globalFailure);
        // Reset global failure to null to make the after method check happy.
        globalFailure = null;
    }

    @Test
    public void checkpointCompletionWaitsForEventFutures() throws Exception {
        final CompletableFuture<Acknowledge> ackFuture = new CompletableFuture<>();
        final EventReceivingTasks tasks =
                EventReceivingTasks.createForRunningTasksWithRpcResult(ackFuture);
        final OperatorCoordinatorHolder holder =
                createCoordinatorHolder(tasks, TestingOperatorCoordinator::new);

        getCoordinator(holder).getSubtaskGateway(0).sendEvent(new TestOperatorEvent(0));

        final CompletableFuture<?> checkpointFuture = triggerAndCompleteCheckpoint(holder, 22L);
        assertThat(checkpointFuture).isNotDone();

        ackFuture.complete(Acknowledge.get());
        assertThat(checkpointFuture).isDone();
    }

    /**
     * This test verifies that the order of Checkpoint Completion and Event Sending observed from
     * the outside matches that from within the OperatorCoordinator.
     *
     * <p>Extreme case 1: The coordinator immediately completes the checkpoint future and sends an
     * event directly after that.
     */
    @Test
    public void verifyCheckpointEventOrderWhenCheckpointFutureCompletedImmediately()
            throws Exception {
        checkpointEventValueAtomicity(FutureCompletedInstantlyTestCoordinator::new);
    }

    /**
     * This test verifies that the order of Checkpoint Completion and Event Sending observed from
     * the outside matches that from within the OperatorCoordinator.
     *
     * <p>Extreme case 2: After the checkpoint triggering, the coordinator flushes a bunch of events
     * before completing the checkpoint future.
     */
    @Test
    public void verifyCheckpointEventOrderWhenCheckpointFutureCompletesLate() throws Exception {
        checkpointEventValueAtomicity(FutureCompletedAfterSendingEventsCoordinator::new);
    }

    private void checkpointEventValueAtomicity(
            final Function<OperatorCoordinator.Context, OperatorCoordinator> coordinatorCtor)
            throws Exception {

        final ManuallyTriggeredScheduledExecutorService executor =
                new ManuallyTriggeredScheduledExecutorService();
        final ComponentMainThreadExecutor mainThreadExecutor =
                new ComponentMainThreadExecutorServiceAdapter(
                        (ScheduledExecutorService) executor, Thread.currentThread());

        final EventReceivingTasks sender = EventReceivingTasks.createForRunningTasks();
        final OperatorCoordinatorHolder holder =
                createCoordinatorHolder(sender, coordinatorCtor, mainThreadExecutor);

        // give the coordinator some time to emit some events. This isn't strictly necessary,
        // but it randomly alters the timings between the coordinator's thread (event sender) and
        // the main thread (holder). This should produce a flaky test if we missed some corner
        // cases.
        Thread.sleep(new Random().nextInt(10));
        executor.triggerAll();

        // trigger the checkpoint - this should also close the gateway as soon as the future is
        // completed
        final CompletableFuture<byte[]> checkpointFuture = new CompletableFuture<>();
        holder.checkpointCoordinator(0L, checkpointFuture);
        executor.triggerAll();
        sendAcknowledgeCloseGatewayEvents(holder, 0L);
        executor.triggerAll();

        // give the coordinator some time to emit some events. Same as above, this adds some
        // randomization
        Thread.sleep(new Random().nextInt(10));
        holder.close();
        executor.triggerAll();

        assertThat(checkpointFuture).isDone();
        final int checkpointedNumber = bytesToInt(checkpointFuture.get());

        assertThat(sender.getNumberOfSentEvents())
                .isEqualTo(checkpointedNumber + holder.currentParallelism());

        Integer[] expectedIntegers = new Integer[checkpointedNumber];
        for (int i = 0; i < checkpointedNumber; i++) {
            expectedIntegers[i] = i;
        }

        List<Integer> sentIntegers = new ArrayList<>();
        List<Long> sentCloseGatewayEventCheckpointIds = new ArrayList<>();
        for (EventReceivingTasks.EventWithSubtask eventWithSubtask : sender.getAllSentEvents()) {
            OperatorEvent event = eventWithSubtask.event;
            if (event instanceof CloseGatewayEvent) {
                sentCloseGatewayEventCheckpointIds.add(
                        ((CloseGatewayEvent) event).getCheckpointID());
            } else {
                sentIntegers.add(((TestOperatorEvent) event).getValue());
            }
        }

        assertThat(sentIntegers).containsExactly(expectedIntegers);
        assertThat(sentCloseGatewayEventCheckpointIds)
                .hasSize(holder.currentParallelism())
                .containsOnly(0L);
    }

    @Test
    public void testCheckpointFailsIfSendingEventFailedAfterTrigger() throws Exception {
        CompletableFuture<Acknowledge> eventSendingResult = new CompletableFuture<>();
        final EventReceivingTasks tasks =
                EventReceivingTasks.createForRunningTasksWithRpcResult(eventSendingResult);
        final OperatorCoordinatorHolder holder =
                createCoordinatorHolder(tasks, TestingOperatorCoordinator::new);

        // Send one event without finishing it.
        getCoordinator(holder).getSubtaskGateway(0).sendEvent(new TestOperatorEvent(0));

        // Trigger one checkpoint.
        CompletableFuture<byte[]> checkpointResult = new CompletableFuture<>();
        holder.checkpointCoordinator(1, checkpointResult);
        sendAcknowledgeCloseGatewayEvents(holder, 1L);
        getCoordinator(holder).getLastTriggeredCheckpoint().complete(new byte[0]);

        // Fail the event sending.
        eventSendingResult.completeExceptionally(new RuntimeException("Artificial"));

        assertThat(checkpointResult).isCompletedExceptionally();
    }

    @Test
    public void testCheckpointFailsIfSendingEventFailedBeforeTrigger() throws Exception {
        final ReorderableManualExecutorService executor = new ReorderableManualExecutorService();
        final ComponentMainThreadExecutor mainThreadExecutor =
                new ComponentMainThreadExecutorServiceAdapter(
                        (ScheduledExecutorService) executor, Thread.currentThread());

        CompletableFuture<Acknowledge> eventSendingResult = new CompletableFuture<>();
        final EventReceivingTasks tasks =
                EventReceivingTasks.createForRunningTasksWithRpcResult(eventSendingResult);

        final OperatorCoordinatorHolder holder =
                createCoordinatorHolder(tasks, TestingOperatorCoordinator::new, mainThreadExecutor);

        // Send one event without finishing it.
        getCoordinator(holder).getSubtaskGateway(0).sendEvent(new TestOperatorEvent(0));
        executor.triggerAll();

        // Finish the event sending. This will insert one runnable that handles
        // failed events to the executor. And we delay this runnable to
        // simulates checkpoints triggered before the failure get processed.
        executor.setDelayNewRunnables(true);
        eventSendingResult.completeExceptionally(new RuntimeException("Artificial"));
        executor.setDelayNewRunnables(false);

        // Trigger one checkpoint, the checkpoint should not be confirmed
        // before the failure get triggered.
        CompletableFuture<byte[]> checkpointResult = new CompletableFuture<>();
        holder.checkpointCoordinator(1, checkpointResult);
        executor.triggerAll();
        sendAcknowledgeCloseGatewayEvents(holder, 1L);
        executor.triggerAll();
        getCoordinator(holder).getLastTriggeredCheckpoint().complete(new byte[0]);
        executor.triggerAll();
        assertThat(checkpointResult).isNotDone();

        // Then the failure finally get processed by fail the corresponding tasks.
        executor.executeAllDelayedRunnables();
        executor.triggerAll();

        // The checkpoint would be finally confirmed.
        assertThat(checkpointResult).isCompletedExceptionally();
    }

    @Test
    public void testControlGatewayAtSubtaskGranularity() throws Exception {
        final EventReceivingTasks tasks = EventReceivingTasks.createForRunningTasks();
        final OperatorCoordinatorHolder holder =
                createCoordinatorHolder(tasks, TestingOperatorCoordinator::new);

        holder.checkpointCoordinator(1L, new CompletableFuture<>());
        sendAcknowledgeCloseGatewayEvents(holder, 1L);
        getCoordinator(holder).getLastTriggeredCheckpoint().complete(new byte[0]);

        getCoordinator(holder).getSubtaskGateway(0).sendEvent(new TestOperatorEvent(0));
        getCoordinator(holder).getSubtaskGateway(1).sendEvent(new TestOperatorEvent(1));
        holder.handleEventFromOperator(1, 0, new OpenGatewayEvent(1L, 1));

        assertThat(tasks.getSentEventsForSubtask(0)).containsExactly(new CloseGatewayEvent(1L, 0));
        assertThat(tasks.getSentEventsForSubtask(1))
                .containsExactly(new CloseGatewayEvent(1L, 1), new TestOperatorEvent(1));

        holder.handleEventFromOperator(0, 0, new OpenGatewayEvent(1L, 0));

        assertThat(tasks.getSentEventsForSubtask(0))
                .containsExactly(new CloseGatewayEvent(1L, 0), new TestOperatorEvent(0));
        assertThat(tasks.getSentEventsForSubtask(1))
                .containsExactly(new CloseGatewayEvent(1L, 1), new TestOperatorEvent(1));
    }

    // ------------------------------------------------------------------------
    //   test actions
    // ------------------------------------------------------------------------

    private CompletableFuture<byte[]> triggerAndCompleteCheckpoint(
            OperatorCoordinatorHolder holder, long checkpointId) throws Exception {

        final CompletableFuture<byte[]> future = new CompletableFuture<>();
        holder.checkpointCoordinator(checkpointId, future);
        sendAcknowledgeCloseGatewayEvents(holder, checkpointId);
        getCoordinator(holder).getLastTriggeredCheckpoint().complete(new byte[0]);
        return future;
    }

    // ------------------------------------------------------------------------
    //   miscellaneous helpers
    // ------------------------------------------------------------------------

    static byte[] intToBytes(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    static int bytesToInt(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }

    private static void sendAcknowledgeCloseGatewayEvents(
            OperatorCoordinatorHolder holder, long checkpointId) throws Exception {
        for (int i = 0; i < holder.currentParallelism(); i++) {
            holder.handleEventFromOperator(i, 0, new AcknowledgeCloseGatewayEvent(checkpointId, i));
        }
    }

    private static void sendOpenGatewayEvents(OperatorCoordinatorHolder holder, long checkpointId)
            throws Exception {
        for (int i = 0; i < holder.currentParallelism(); i++) {
            holder.handleEventFromOperator(i, 0, new OpenGatewayEvent(checkpointId, i));
        }
    }

    private static TestingOperatorCoordinator getCoordinator(OperatorCoordinatorHolder holder) {
        return (TestingOperatorCoordinator) holder.coordinator();
    }

    private OperatorCoordinatorHolder createCoordinatorHolder(
            final SubtaskAccess.SubtaskAccessFactory eventTarget,
            final Function<OperatorCoordinator.Context, OperatorCoordinator> coordinatorCtor)
            throws Exception {

        return createCoordinatorHolder(
                eventTarget,
                coordinatorCtor,
                ComponentMainThreadExecutorServiceAdapter.forMainThread());
    }

    private OperatorCoordinatorHolder createCoordinatorHolder(
            final SubtaskAccess.SubtaskAccessFactory eventTarget,
            final Function<OperatorCoordinator.Context, OperatorCoordinator> coordinatorCtor,
            final ComponentMainThreadExecutor mainThreadExecutor)
            throws Exception {

        final OperatorID opId = new OperatorID();
        final OperatorCoordinator.Provider provider =
                new OperatorCoordinator.Provider() {
                    @Override
                    public OperatorID getOperatorId() {
                        return opId;
                    }

                    @Override
                    public OperatorCoordinator create(OperatorCoordinator.Context context) {
                        return coordinatorCtor.apply(context);
                    }
                };

        final OperatorCoordinatorHolder holder =
                OperatorCoordinatorHolder.create(
                        opId,
                        provider,
                        new CoordinatorStoreImpl(),
                        "test-coordinator-name",
                        getClass().getClassLoader(),
                        3,
                        1775,
                        eventTarget,
                        false);

        holder.lazyInitialize(globalFailureHandler, mainThreadExecutor);
        holder.start();

        return holder;
    }

    private static class ReorderableManualExecutorService
            extends ManuallyTriggeredScheduledExecutorService {

        private boolean delayNewRunnables;

        private final Queue<Runnable> delayedRunnables = new ArrayDeque<>();

        public void setDelayNewRunnables(boolean delayNewRunnables) {
            this.delayNewRunnables = delayNewRunnables;
        }

        @Override
        public void execute(@Nonnull Runnable command) {
            if (delayNewRunnables) {
                delayedRunnables.add(command);
            } else {
                super.execute(command);
            }
        }

        public void executeAllDelayedRunnables() {
            while (!delayedRunnables.isEmpty()) {
                super.execute(delayedRunnables.poll());
            }
        }
    }

    // ------------------------------------------------------------------------
    //   test implementations
    // ------------------------------------------------------------------------

    private static final class FutureCompletedInstantlyTestCoordinator
            extends CheckpointEventOrderTestBaseCoordinator {

        private final ReentrantLock lock = new ReentrantLock(true);
        private final Condition condition = lock.newCondition();

        @Nullable
        @GuardedBy("lock")
        private CompletableFuture<byte[]> checkpoint;

        private int num;

        FutureCompletedInstantlyTestCoordinator(Context context) {
            super(context);
        }

        @Override
        public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result)
                throws Exception {
            // before returning from this method, we wait on a condition.
            // that way, we simulate a "context switch" just at the time when the
            // future would be returned and make the other thread complete the future and send an
            // event before this method returns
            lock.lock();
            try {
                checkpoint = result;
                condition.await();
            } finally {
                lock.unlock();
            }
        }

        @Override
        protected void step() throws Exception {
            lock.lock();
            try {
                // if there is a checkpoint to complete, we complete it and immediately
                // try to send another event, without releasing the lock. that way we
                // force the situation as if the checkpoint get completed and an event gets
                // sent while the triggering thread is stalled
                if (checkpoint != null) {
                    checkpoint.complete(intToBytes(num));
                    checkpoint = null;
                }
                subtaskGateways[0].sendEvent(new TestOperatorEvent(num++));
                condition.signalAll();
            } finally {
                lock.unlock();
            }

            Thread.sleep(2);
        }
    }

    private static final class FutureCompletedAfterSendingEventsCoordinator
            extends CheckpointEventOrderTestBaseCoordinator {

        private final OneShotLatch checkpointCompleted = new OneShotLatch();

        @Nullable private volatile CompletableFuture<byte[]> checkpoint;

        private int num;

        FutureCompletedAfterSendingEventsCoordinator(Context context) {
            super(context);
        }

        @Override
        public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result)
                throws Exception {
            checkpoint = result;
        }

        @Override
        protected void step() throws Exception {
            Thread.sleep(2);

            subtaskGateways[0].sendEvent(new TestOperatorEvent(num++));
            subtaskGateways[1].sendEvent(new TestOperatorEvent(num++));
            subtaskGateways[2].sendEvent(new TestOperatorEvent(num++));

            final CompletableFuture<byte[]> chkpnt = this.checkpoint;
            if (chkpnt != null) {
                chkpnt.complete(intToBytes(num));
                checkpointCompleted.trigger();
                this.checkpoint = null;
            }
        }

        @Override
        public void close() throws Exception {
            // we need to ensure that we don't close this before we have actually completed the
            // triggered checkpoint, to ensure the test conditions are robust.
            checkpointCompleted.await();
            super.close();
        }
    }

    private abstract static class CheckpointEventOrderTestBaseCoordinator
            implements OperatorCoordinator, Runnable {

        private final Thread coordinatorThread;

        protected final Context context;
        protected final SubtaskGateway[] subtaskGateways;

        private volatile boolean closed;

        CheckpointEventOrderTestBaseCoordinator(Context context) {
            this.context = context;
            this.subtaskGateways = new SubtaskGateway[context.currentParallelism()];
            this.coordinatorThread = new Thread(this);
        }

        @Override
        public void start() throws Exception {}

        @Override
        public void close() throws Exception {
            closed = true;
            coordinatorThread.interrupt();
            coordinatorThread.join();
        }

        @Override
        public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) {}

        @Override
        public void executionAttemptFailed(
                int subtask, int attemptNumber, @Nullable Throwable reason) {}

        @Override
        public void subtaskReset(int subtask, long checkpointId) {}

        @Override
        public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
            subtaskGateways[subtask] = gateway;

            for (SubtaskGateway subtaskGateway : subtaskGateways) {
                if (subtaskGateway == null) {
                    return;
                }
            }

            // start only once all tasks are ready
            coordinatorThread.start();
        }

        @Override
        public abstract void checkpointCoordinator(
                long checkpointId, CompletableFuture<byte[]> result) throws Exception;

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}

        @Override
        public void resetToCheckpoint(long checkpointId, byte[] checkpointData) throws Exception {}

        @Override
        public void run() {
            try {
                while (!closed) {
                    step();
                }
            } catch (Throwable t) {
                if (closed) {
                    return;
                }

                // this should never happen, but just in case, print and crash the test
                //noinspection CallToPrintStackTrace
                t.printStackTrace();
                System.exit(-1);
            }
        }

        protected abstract void step() throws Exception;
    }
}
