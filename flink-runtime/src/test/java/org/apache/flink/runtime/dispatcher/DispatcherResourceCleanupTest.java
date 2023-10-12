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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.BlobUtils;
import org.apache.flink.runtime.blob.TestingBlobStoreBuilder;
import org.apache.flink.runtime.client.DuplicateJobSubmissionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.dispatcher.cleanup.TestingResourceCleanerFactory;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.JobResultEntry;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.TestingJobManagerRunner;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.testutils.TestingJobResultStore;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.Reference;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.runtime.dispatcher.AbstractDispatcherTest.awaitStatus;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

/** Tests the resource cleanup by the {@link Dispatcher}. */
@ExtendWith(ParameterizedTestExtension.class)
public class DispatcherResourceCleanupTest extends TestLogger {

    @TempDir static Path temporaryFolder;

    @RegisterExtension
    private final TestingFatalErrorHandlerExtension testingFatalErrorHandlerResource =
            new TestingFatalErrorHandlerExtension();

    private static final Time timeout = Time.seconds(10L);

    private static TestingRpcService rpcService;

    private final String restartStrategy;

    private JobID jobId;

    private JobGraph jobGraph;

    private TestingDispatcher dispatcher;

    private DispatcherGateway dispatcherGateway;

    private BlobServer blobServer;

    private CompletableFuture<JobID> localCleanupFuture;
    private CompletableFuture<JobID> globalCleanupFuture;

    DispatcherResourceCleanupTest(String restartStrategy) {
        this.restartStrategy = restartStrategy;
    }

    @Parameters(name = "{0}")
    public static List<String> getRestartStrategy() {
        return Arrays.asList("none", "fixeddelay");
    }

    @BeforeAll
    static void setupClass() {
        rpcService = new TestingRpcService();
    }

    @BeforeEach
    void setup() throws Exception {
        jobGraph = JobGraphTestUtils.singleNoOpJobGraph();
        jobGraph.getJobConfiguration()
                .set(RestartStrategyOptions.RESTART_STRATEGY, restartStrategy);
        jobId = jobGraph.getJobID();

        globalCleanupFuture = new CompletableFuture<>();
        localCleanupFuture = new CompletableFuture<>();

        blobServer =
                BlobUtils.createBlobServer(
                        new Configuration(),
                        Reference.owned(temporaryFolder.toFile()),
                        new TestingBlobStoreBuilder().createTestingBlobStore());
    }

    private TestingJobManagerRunnerFactory startDispatcherAndSubmitJob() throws Exception {
        return startDispatcherAndSubmitJob(0);
    }

    private TestingJobManagerRunnerFactory startDispatcherAndSubmitJob(
            int numBlockingJobManagerRunners) throws Exception {
        return startDispatcherAndSubmitJob(
                createTestingDispatcherBuilder(), numBlockingJobManagerRunners);
    }

    private TestingJobManagerRunnerFactory startDispatcherAndSubmitJob(
            TestingDispatcher.Builder dispatcherBuilder, int numBlockingJobManagerRunners)
            throws Exception {
        final TestingJobMasterServiceLeadershipRunnerFactory testingJobManagerRunnerFactoryNG =
                new TestingJobMasterServiceLeadershipRunnerFactory(numBlockingJobManagerRunners);
        startDispatcher(dispatcherBuilder, testingJobManagerRunnerFactoryNG);
        submitJobAndWait();

        return testingJobManagerRunnerFactoryNG;
    }

    private void startDispatcher(JobManagerRunnerFactory jobManagerRunnerFactory) throws Exception {
        startDispatcher(createTestingDispatcherBuilder(), jobManagerRunnerFactory);
    }

    private void startDispatcher(
            TestingDispatcher.Builder dispatcherBuilder,
            JobManagerRunnerFactory jobManagerRunnerFactory)
            throws Exception {
        dispatcher =
                dispatcherBuilder
                        .setJobManagerRunnerFactory(jobManagerRunnerFactory)
                        .build(rpcService);

        dispatcher.start();

        dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);
    }

    private TestingDispatcher.Builder createTestingDispatcherBuilder() {
        final JobManagerRunnerRegistry jobManagerRunnerRegistry =
                new DefaultJobManagerRunnerRegistry(2);
        return TestingDispatcher.builder()
                .setBlobServer(blobServer)
                .setJobManagerRunnerRegistry(jobManagerRunnerRegistry)
                .setFatalErrorHandler(
                        testingFatalErrorHandlerResource.getTestingFatalErrorHandler())
                .setResourceCleanerFactory(
                        TestingResourceCleanerFactory.builder()
                                // JobManagerRunnerRegistry needs to be added explicitly
                                // because cleaning it will trigger the closeAsync latch
                                // provided by TestingJobManagerRunner
                                .withLocallyCleanableResource(jobManagerRunnerRegistry)
                                .withGloballyCleanableResource(
                                        (jobId, ignoredExecutor) -> {
                                            globalCleanupFuture.complete(jobId);
                                            return FutureUtils.completedVoidFuture();
                                        })
                                .withLocallyCleanableResource(
                                        (jobId, ignoredExecutor) -> {
                                            localCleanupFuture.complete(jobId);
                                            return FutureUtils.completedVoidFuture();
                                        })
                                .build());
    }

    @AfterEach
    void teardown() throws Exception {
        if (dispatcher != null) {
            dispatcher.close();
        }

        if (blobServer != null) {
            blobServer.close();
        }
    }

    @AfterAll
    static void teardownClass() throws ExecutionException, InterruptedException {
        if (rpcService != null) {
            rpcService.closeAsync().get();
        }
    }

    @TestTemplate
    void testGlobalCleanupWhenJobFinished() throws Exception {
        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitJob();

        // complete the job
        finishJob(jobManagerRunnerFactory.takeCreatedJobManagerRunner());

        assertGlobalCleanupTriggered(jobId);
    }

    @TestTemplate
    void testGlobalCleanupWhenJobCanceled() throws Exception {
        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitJob();

        // complete the job
        cancelJob(jobManagerRunnerFactory.takeCreatedJobManagerRunner());

        assertGlobalCleanupTriggered(jobId);
    }

    private CompletableFuture<Acknowledge> submitJob() {
        return dispatcherGateway.submitJob(jobGraph, timeout);
    }

    private void submitJobAndWait() {
        submitJob().join();
    }

    @TestTemplate
    void testLocalCleanupWhenJobNotFinished() throws Exception {
        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitJob();

        // job not finished
        final TestingJobManagerRunner testingJobManagerRunner =
                jobManagerRunnerFactory.takeCreatedJobManagerRunner();
        suspendJob(testingJobManagerRunner);

        assertLocalCleanupTriggered(jobId);
    }

    @TestTemplate
    void testGlobalCleanupWhenJobSubmissionFails() throws Exception {
        startDispatcher(new FailingJobManagerRunnerFactory(new FlinkException("Test exception")));
        final CompletableFuture<Acknowledge> submissionFuture = submitJob();

        try {
            submissionFuture.get();
            fail("Job submission was expected to fail.");
        } catch (ExecutionException ee) {
            assertThat(ee).hasCauseInstanceOf(JobSubmissionException.class);
        }

        assertGlobalCleanupTriggered(jobId);
    }

    @TestTemplate
    void testLocalCleanupWhenClosingDispatcher() throws Exception {
        startDispatcherAndSubmitJob();

        dispatcher.closeAsync().get();

        assertLocalCleanupTriggered(jobId);
    }

    @TestTemplate
    void testGlobalCleanupWhenJobFinishedWhileClosingDispatcher() throws Exception {
        final TestingJobManagerRunner testingJobManagerRunner =
                TestingJobManagerRunner.newBuilder()
                        .setBlockingTermination(true)
                        .setJobId(jobId)
                        .build();

        final Queue<JobManagerRunner> jobManagerRunners =
                new ArrayDeque<>(Arrays.asList(testingJobManagerRunner));

        startDispatcher(new QueueJobManagerRunnerFactory(jobManagerRunners));
        submitJobAndWait();

        final CompletableFuture<Void> dispatcherTerminationFuture = dispatcher.closeAsync();

        testingJobManagerRunner.getCloseAsyncCalledLatch().await();
        testingJobManagerRunner.completeResultFuture(
                new ExecutionGraphInfo(
                        new ArchivedExecutionGraphBuilder()
                                .setJobID(jobId)
                                .setState(JobStatus.FINISHED)
                                .build()));

        testingJobManagerRunner.completeTerminationFuture();

        // check that no exceptions have been thrown
        dispatcherTerminationFuture.get();

        assertGlobalCleanupTriggered(jobId);
    }

    @TestTemplate
    void testJobBeingMarkedAsDirtyBeforeCleanup() throws Exception {
        final OneShotLatch markAsDirtyLatch = new OneShotLatch();

        final TestingDispatcher.Builder dispatcherBuilder =
                createTestingDispatcherBuilder()
                        .setJobResultStore(
                                TestingJobResultStore.builder()
                                        .withCreateDirtyResultConsumer(
                                                ignoredJobResultEntry -> {
                                                    try {
                                                        markAsDirtyLatch.await();
                                                    } catch (InterruptedException e) {
                                                        Thread.currentThread().interrupt();
                                                        return FutureUtils.completedExceptionally(
                                                                e);
                                                    }
                                                    return FutureUtils.completedVoidFuture();
                                                })
                                        .build());

        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitJob(dispatcherBuilder, 0);

        finishJob(jobManagerRunnerFactory.takeCreatedJobManagerRunner());

        assertThatNoCleanupWasTriggered();

        markAsDirtyLatch.trigger();

        assertGlobalCleanupTriggered(jobId);
    }

    @TestTemplate
    void testJobBeingMarkedAsCleanAfterCleanup() throws Exception {
        final CompletableFuture<JobID> markAsCleanFuture = new CompletableFuture<>();

        final JobResultStore jobResultStore =
                TestingJobResultStore.builder()
                        .withMarkResultAsCleanConsumer(
                                jobID -> {
                                    markAsCleanFuture.complete(jobID);
                                    return FutureUtils.completedVoidFuture();
                                })
                        .build();
        final OneShotLatch localCleanupLatch = new OneShotLatch();
        final OneShotLatch globalCleanupLatch = new OneShotLatch();
        final TestingResourceCleanerFactory resourceCleanerFactory =
                TestingResourceCleanerFactory.builder()
                        .withLocallyCleanableResource(
                                (ignoredJobId, ignoredExecutor) -> {
                                    try {
                                        localCleanupLatch.await();
                                    } catch (InterruptedException e) {
                                        throw new RuntimeException(e);
                                    }

                                    return FutureUtils.completedVoidFuture();
                                })
                        .withGloballyCleanableResource(
                                (ignoredJobId, ignoredExecutor) -> {
                                    try {
                                        globalCleanupLatch.await();
                                    } catch (InterruptedException e) {
                                        throw new RuntimeException(e);
                                    }

                                    return FutureUtils.completedVoidFuture();
                                })
                        .build();

        final TestingDispatcher.Builder dispatcherBuilder =
                createTestingDispatcherBuilder()
                        .setJobResultStore(jobResultStore)
                        .setResourceCleanerFactory(resourceCleanerFactory);

        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitJob(dispatcherBuilder, 0);

        finishJob(jobManagerRunnerFactory.takeCreatedJobManagerRunner());

        assertThat(markAsCleanFuture.isDone()).isFalse();

        localCleanupLatch.trigger();
        assertThat(markAsCleanFuture.isDone()).isFalse();
        globalCleanupLatch.trigger();

        assertThat(markAsCleanFuture.get()).isEqualTo(jobId);
    }

    /**
     * Tests that the previous JobManager needs to be completely terminated before a new job with
     * the same {@link JobID} is started.
     */
    @TestTemplate
    void testJobSubmissionUnderSameJobId() throws Exception {
        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitJob(1);

        final TestingJobManagerRunner testingJobManagerRunner =
                jobManagerRunnerFactory.takeCreatedJobManagerRunner();
        suspendJob(testingJobManagerRunner);

        // wait until termination JobManagerRunner closeAsync has been called.
        // this is necessary to avoid race conditions with completion of the 1st job and the
        // submission of the 2nd job (DuplicateJobSubmissionException).
        testingJobManagerRunner.getCloseAsyncCalledLatch().await();

        final CompletableFuture<Acknowledge> submissionFuture =
                dispatcherGateway.submitJob(jobGraph, timeout);

        try {
            submissionFuture.get(10L, TimeUnit.MILLISECONDS);
            fail(
                    "The job submission future should not complete until the previous JobManager "
                            + "termination future has been completed.");
        } catch (TimeoutException ignored) {
            // expected
        } finally {
            testingJobManagerRunner.completeTerminationFuture();
        }

        assertThat(submissionFuture.get()).isEqualTo(Acknowledge.get());
    }

    /**
     * Tests that a duplicate job submission won't delete any job meta data (submitted job graphs,
     * blobs, etc.).
     */
    @TestTemplate
    void testDuplicateJobSubmissionDoesNotDeleteJobMetaData() throws Exception {
        final TestingJobManagerRunnerFactory testingJobManagerRunnerFactoryNG =
                startDispatcherAndSubmitJob();

        final CompletableFuture<Acknowledge> submissionFuture =
                dispatcherGateway.submitJob(jobGraph, timeout);

        try {
            try {
                submissionFuture.get();
                fail("Expected a DuplicateJobSubmissionFailure.");
            } catch (ExecutionException ee) {
                assertThat(
                                ExceptionUtils.findThrowable(
                                                ee, DuplicateJobSubmissionException.class)
                                        .isPresent())
                        .isTrue();
            }

            assertThatNoCleanupWasTriggered();
        } finally {
            finishJob(testingJobManagerRunnerFactoryNG.takeCreatedJobManagerRunner());
        }

        assertGlobalCleanupTriggered(jobId);
    }

    private void finishJob(TestingJobManagerRunner takeCreatedJobManagerRunner) {
        terminateJobWithState(takeCreatedJobManagerRunner, JobStatus.FINISHED);
    }

    private void suspendJob(TestingJobManagerRunner takeCreatedJobManagerRunner) {
        terminateJobWithState(takeCreatedJobManagerRunner, JobStatus.SUSPENDED);
    }

    private void cancelJob(TestingJobManagerRunner takeCreatedJobManagerRunner) {
        terminateJobWithState(takeCreatedJobManagerRunner, JobStatus.CANCELED);
    }

    private void terminateJobWithState(
            TestingJobManagerRunner takeCreatedJobManagerRunner, JobStatus state) {
        takeCreatedJobManagerRunner.completeResultFuture(
                new ExecutionGraphInfo(
                        new ArchivedExecutionGraphBuilder()
                                .setJobID(jobId)
                                .setState(state)
                                .build()));
    }

    private void assertThatNoCleanupWasTriggered() {
        assertThat(globalCleanupFuture.isDone()).isFalse();
        assertThat(localCleanupFuture.isDone()).isFalse();
    }

    @TestTemplate
    void testDispatcherTerminationTerminatesRunningJobMasters() throws Exception {
        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitJob();

        dispatcher.closeAsync().get();

        final TestingJobManagerRunner jobManagerRunner =
                jobManagerRunnerFactory.takeCreatedJobManagerRunner();
        assertThat(jobManagerRunner.getTerminationFuture().isDone()).isTrue();
    }

    /** Tests that terminating the Dispatcher will wait for all JobMasters to be terminated. */
    @TestTemplate
    void testDispatcherTerminationWaitsForJobMasterTerminations() throws Exception {
        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitJob(1);

        final CompletableFuture<Void> dispatcherTerminationFuture = dispatcher.closeAsync();

        try {
            dispatcherTerminationFuture.get(10L, TimeUnit.MILLISECONDS);
            fail("We should not terminate before all running JobMasters have terminated.");
        } catch (TimeoutException ignored) {
            // expected
        } finally {
            jobManagerRunnerFactory.takeCreatedJobManagerRunner().completeTerminationFuture();
        }

        dispatcherTerminationFuture.get();
    }

    private void assertLocalCleanupTriggered(JobID jobId)
            throws ExecutionException, InterruptedException, TimeoutException {
        assertThat(localCleanupFuture.get()).isEqualTo(jobId);
        assertThat(globalCleanupFuture.isDone()).isFalse();
    }

    private void assertGlobalCleanupTriggered(JobID jobId)
            throws ExecutionException, InterruptedException, TimeoutException {
        assertThat(localCleanupFuture.isDone()).isFalse();
        assertThat(globalCleanupFuture.get()).isEqualTo(jobId);
    }

    @TestTemplate
    void testFatalErrorIfJobCannotBeMarkedDirtyInJobResultStore() throws Exception {
        final JobResultStore jobResultStore =
                TestingJobResultStore.builder()
                        .withCreateDirtyResultConsumer(
                                jobResult ->
                                        FutureUtils.completedExceptionally(
                                                new IOException("Expected IOException.")))
                        .build();

        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitJob(
                        createTestingDispatcherBuilder().setJobResultStore(jobResultStore), 0);

        ArchivedExecutionGraph executionGraph =
                new ArchivedExecutionGraphBuilder()
                        .setJobID(jobId)
                        .setState(JobStatus.FINISHED)
                        .build();

        final TestingJobManagerRunner testingJobManagerRunner =
                jobManagerRunnerFactory.takeCreatedJobManagerRunner();
        testingJobManagerRunner.completeResultFuture(new ExecutionGraphInfo(executionGraph));

        final CompletableFuture<? extends Throwable> errorFuture =
                this.testingFatalErrorHandlerResource
                        .getTestingFatalErrorHandler()
                        .getErrorFuture();
        assertThat(errorFuture.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(FlinkException.class);
        testingFatalErrorHandlerResource.getTestingFatalErrorHandler().clearError();
    }

    @TestTemplate
    void testErrorHandlingIfJobCannotBeMarkedAsCleanInJobResultStore() throws Exception {
        final CompletableFuture<JobResultEntry> dirtyJobFuture = new CompletableFuture<>();
        final JobResultStore jobResultStore =
                TestingJobResultStore.builder()
                        .withCreateDirtyResultConsumer(
                                jobResultEntry -> {
                                    dirtyJobFuture.complete(jobResultEntry);
                                    return FutureUtils.completedVoidFuture();
                                })
                        .withMarkResultAsCleanConsumer(
                                jobId ->
                                        FutureUtils.completedExceptionally(
                                                new IOException("Expected IOException.")))
                        .build();

        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitJob(
                        createTestingDispatcherBuilder().setJobResultStore(jobResultStore), 0);

        ArchivedExecutionGraph executionGraph =
                new ArchivedExecutionGraphBuilder()
                        .setJobID(jobId)
                        .setState(JobStatus.FINISHED)
                        .build();

        final TestingJobManagerRunner testingJobManagerRunner =
                jobManagerRunnerFactory.takeCreatedJobManagerRunner();
        testingJobManagerRunner.completeResultFuture(new ExecutionGraphInfo(executionGraph));

        final CompletableFuture<? extends Throwable> errorFuture =
                this.testingFatalErrorHandlerResource
                        .getTestingFatalErrorHandler()
                        .getErrorFuture();
        try {
            final Throwable unexpectedError = errorFuture.get(100, TimeUnit.MILLISECONDS);
            fail(
                    "No error should have been reported but an "
                            + unexpectedError.getClass()
                            + " was handled.");
        } catch (TimeoutException e) {
            // expected
        }

        assertThat(dirtyJobFuture.get().getJobId()).isEqualTo(jobId);
    }

    /** Tests that a failing {@link JobManagerRunner} will be properly cleaned up. */
    @TestTemplate
    void testFailingJobManagerRunnerCleanup() throws Exception {
        final FlinkException testException = new FlinkException("Test exception.");
        final ArrayBlockingQueue<Optional<Exception>> queue = new ArrayBlockingQueue<>(2);

        final BlockingJobManagerRunnerFactory blockingJobManagerRunnerFactory =
                new BlockingJobManagerRunnerFactory(
                        () -> {
                            final Optional<Exception> maybeException = queue.take();
                            if (maybeException.isPresent()) {
                                throw maybeException.get();
                            }
                        });

        startDispatcher(blockingJobManagerRunnerFactory);

        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);

        // submit and fail during job master runner construction
        queue.offer(Optional.of(testException));
        try {
            dispatcherGateway.submitJob(jobGraph, Time.minutes(1)).get();
            fail("A FlinkException is expected");
        } catch (Throwable expectedException) {
            assertThat(expectedException).hasRootCause(testException);
            // make sure we've cleaned up in correct order (including HA)
            assertGlobalCleanupTriggered(jobId);
        }

        // don't fail this time
        queue.offer(Optional.empty());
        // submit job again
        dispatcherGateway.submitJob(jobGraph, Time.minutes(1L)).get();
        blockingJobManagerRunnerFactory.setJobStatus(JobStatus.RUNNING);

        // Ensure job is running
        awaitStatus(dispatcherGateway, jobId, JobStatus.RUNNING);
    }

    @TestTemplate
    void testArchivingFinishedJobToHistoryServer() throws Exception {

        final CompletableFuture<Acknowledge> archiveFuture = new CompletableFuture<>();

        final TestingDispatcher.Builder testingDispatcherBuilder =
                createTestingDispatcherBuilder()
                        .setHistoryServerArchivist(executionGraphInfo -> archiveFuture);

        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitJob(testingDispatcherBuilder, 0);

        finishJob(jobManagerRunnerFactory.takeCreatedJobManagerRunner());

        // Before the archiving is finished, the cleanup is not finished and the job is not
        // terminated
        assertThatNoCleanupWasTriggered();
        final CompletableFuture<Void> jobTerminationFuture =
                dispatcher.getJobTerminationFuture(jobId, Time.hours(1));
        assertFalse(jobTerminationFuture.isDone());

        archiveFuture.complete(Acknowledge.get());

        // Once the archive is finished, the cleanup is finished and the job is terminated.
        assertGlobalCleanupTriggered(jobId);
        jobTerminationFuture.join();
    }

    @TestTemplate
    void testNotArchivingSuspendedJobToHistoryServer() throws Exception {

        final AtomicBoolean isArchived = new AtomicBoolean(false);

        final TestingDispatcher.Builder testingDispatcherBuilder =
                createTestingDispatcherBuilder()
                        .setHistoryServerArchivist(
                                executionGraphInfo -> {
                                    isArchived.set(true);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                });

        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitJob(testingDispatcherBuilder, 0);

        suspendJob(jobManagerRunnerFactory.takeCreatedJobManagerRunner());

        assertLocalCleanupTriggered(jobId);
        dispatcher.getJobTerminationFuture(jobId, Time.hours(1)).join();

        assertFalse(isArchived.get());
    }

    private static final class BlockingJobManagerRunnerFactory
            extends TestingJobMasterServiceLeadershipRunnerFactory {

        private final ThrowingRunnable<Exception> jobManagerRunnerCreationLatch;
        private TestingJobManagerRunner testingRunner;

        BlockingJobManagerRunnerFactory(ThrowingRunnable<Exception> jobManagerRunnerCreationLatch) {
            this.jobManagerRunnerCreationLatch = jobManagerRunnerCreationLatch;
        }

        @Override
        public TestingJobManagerRunner createJobManagerRunner(
                JobGraph jobGraph,
                Configuration configuration,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                JobManagerSharedServices jobManagerSharedServices,
                JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
                FatalErrorHandler fatalErrorHandler,
                Collection<FailureEnricher> failureEnrichers,
                long initializationTimestamp)
                throws Exception {
            jobManagerRunnerCreationLatch.run();

            this.testingRunner =
                    super.createJobManagerRunner(
                            jobGraph,
                            configuration,
                            rpcService,
                            highAvailabilityServices,
                            heartbeatServices,
                            jobManagerSharedServices,
                            jobManagerJobMetricGroupFactory,
                            fatalErrorHandler,
                            failureEnrichers,
                            initializationTimestamp);

            TestingJobMasterGateway testingJobMasterGateway =
                    new TestingJobMasterGatewayBuilder()
                            .setRequestJobSupplier(
                                    () ->
                                            CompletableFuture.completedFuture(
                                                    new ExecutionGraphInfo(
                                                            ArchivedExecutionGraph
                                                                    .createSparseArchivedExecutionGraph(
                                                                            jobGraph.getJobID(),
                                                                            jobGraph.getName(),
                                                                            JobStatus.RUNNING,
                                                                            null,
                                                                            null,
                                                                            1337))))
                            .build();
            testingRunner.completeJobMasterGatewayFuture(testingJobMasterGateway);
            return testingRunner;
        }

        void setJobStatus(JobStatus newStatus) {
            Preconditions.checkState(
                    testingRunner != null,
                    "JobManagerRunner must be created before this method is available");
            this.testingRunner.setJobStatus(newStatus);
        }
    }

    private static final class QueueJobManagerRunnerFactory implements JobManagerRunnerFactory {
        private final Queue<? extends JobManagerRunner> jobManagerRunners;

        private QueueJobManagerRunnerFactory(Queue<? extends JobManagerRunner> jobManagerRunners) {
            this.jobManagerRunners = jobManagerRunners;
        }

        @Override
        public JobManagerRunner createJobManagerRunner(
                JobGraph jobGraph,
                Configuration configuration,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                JobManagerSharedServices jobManagerServices,
                JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
                FatalErrorHandler fatalErrorHandler,
                Collection<FailureEnricher> failureEnrichers,
                long initializationTimestamp) {
            return Optional.ofNullable(jobManagerRunners.poll())
                    .orElseThrow(
                            () ->
                                    new IllegalStateException(
                                            "Cannot create more JobManagerRunners."));
        }
    }

    private class FailingJobManagerRunnerFactory implements JobManagerRunnerFactory {
        private final Exception testException;

        FailingJobManagerRunnerFactory(FlinkException testException) {
            this.testException = testException;
        }

        @Override
        public JobManagerRunner createJobManagerRunner(
                JobGraph jobGraph,
                Configuration configuration,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                JobManagerSharedServices jobManagerServices,
                JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
                FatalErrorHandler fatalErrorHandler,
                Collection<FailureEnricher> failureEnrichers,
                long initializationTimestamp)
                throws Exception {
            throw testException;
        }
    }
}
