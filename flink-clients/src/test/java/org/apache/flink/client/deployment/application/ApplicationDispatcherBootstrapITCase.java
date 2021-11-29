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

package org.apache.flink.client.deployment.application;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.cli.ClientOptions;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutor;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.testjar.BlockingJob;
import org.apache.flink.client.testjar.ErrorHandlingSubmissionJob;
import org.apache.flink.client.testjar.FailingJob;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.runtime.client.DuplicateJobSubmissionException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.SessionDispatcherFactory;
import org.apache.flink.runtime.dispatcher.runner.DefaultDispatcherRunnerFactory;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.highavailability.JobResultEntry;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServicesWithLeadershipControl;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedJobResultStore;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniClusterConfiguration;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.rest.JobRestEndpointFactory;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.TestingJobResultStore;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Integration tests related to {@link ApplicationDispatcherBootstrap}. */
public class ApplicationDispatcherBootstrapITCase extends TestLogger {

    private static final Duration TIMEOUT = Duration.ofMinutes(10);

    private static Supplier<DispatcherResourceManagerComponentFactory>
            createApplicationModeDispatcherResourceManagerComponentFactorySupplier(
                    Configuration configuration, PackagedProgram program) {
        return () -> {
            final ApplicationDispatcherLeaderProcessFactoryFactory
                    applicationDispatcherLeaderProcessFactoryFactory =
                            ApplicationDispatcherLeaderProcessFactoryFactory.create(
                                    new Configuration(configuration),
                                    SessionDispatcherFactory.INSTANCE,
                                    program);
            return new DefaultDispatcherResourceManagerComponentFactory(
                    new DefaultDispatcherRunnerFactory(
                            applicationDispatcherLeaderProcessFactoryFactory),
                    StandaloneResourceManagerFactory.getInstance(),
                    JobRestEndpointFactory.INSTANCE);
        };
    }

    @Test
    public void testDispatcherRecoversAfterLosingAndRegainingLeadership() throws Exception {
        final String blockId = UUID.randomUUID().toString();
        final Deadline deadline = Deadline.fromNow(TIMEOUT);
        final Configuration configuration = new Configuration();
        configuration.set(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());
        configuration.set(DeploymentOptions.TARGET, EmbeddedExecutor.NAME);
        configuration.set(ClientOptions.CLIENT_RETRY_PERIOD, Duration.ofMillis(100));
        final TestingMiniClusterConfiguration clusterConfiguration =
                TestingMiniClusterConfiguration.newBuilder()
                        .setConfiguration(configuration)
                        .build();
        final EmbeddedHaServicesWithLeadershipControl haServices =
                new EmbeddedHaServicesWithLeadershipControl(TestingUtils.defaultExecutor());
        final TestingMiniCluster.Builder clusterBuilder =
                TestingMiniCluster.newBuilder(clusterConfiguration)
                        .setHighAvailabilityServicesSupplier(() -> haServices)
                        .setDispatcherResourceManagerComponentFactorySupplier(
                                createApplicationModeDispatcherResourceManagerComponentFactorySupplier(
                                        clusterConfiguration.getConfiguration(),
                                        BlockingJob.getProgram(blockId)));
        try (final MiniCluster cluster = clusterBuilder.build()) {

            // start mini cluster and submit the job
            cluster.start();

            // wait until job is running
            awaitJobStatus(
                    cluster,
                    ApplicationDispatcherBootstrap.ZERO_JOB_ID,
                    JobStatus.RUNNING,
                    deadline);

            // make sure the operator is actually running
            BlockingJob.awaitRunning(blockId);

            final CompletableFuture<JobResult> firstJobResult =
                    cluster.requestJobResult(ApplicationDispatcherBootstrap.ZERO_JOB_ID);
            haServices.revokeDispatcherLeadership();
            // make sure the leadership is revoked to avoid race conditions
            assertEquals(ApplicationStatus.UNKNOWN, firstJobResult.get().getApplicationStatus());
            haServices.grantDispatcherLeadership();

            // job is suspended, wait until it's running
            awaitJobStatus(
                    cluster,
                    ApplicationDispatcherBootstrap.ZERO_JOB_ID,
                    JobStatus.RUNNING,
                    deadline);

            // unblock processing so the job can finish
            BlockingJob.unblock(blockId);

            // and wait for it to actually finish
            final CompletableFuture<JobResult> secondJobResult =
                    cluster.requestJobResult(ApplicationDispatcherBootstrap.ZERO_JOB_ID);
            assertTrue(secondJobResult.get().isSuccess());
            assertEquals(ApplicationStatus.SUCCEEDED, secondJobResult.get().getApplicationStatus());

            // the cluster should shut down automatically once the application completes
            awaitClusterStopped(cluster, deadline);
        } finally {
            BlockingJob.cleanUp(blockId);
        }
    }

    @Test
    public void testDirtyJobResultRecoveryInApplicationMode() throws Exception {
        final Deadline deadline = Deadline.fromNow(TIMEOUT);
        final Configuration configuration = new Configuration();
        configuration.set(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());
        configuration.set(DeploymentOptions.TARGET, EmbeddedExecutor.NAME);
        configuration.set(ClientOptions.CLIENT_RETRY_PERIOD, Duration.ofMillis(100));
        final TestingMiniClusterConfiguration clusterConfiguration =
                TestingMiniClusterConfiguration.newBuilder()
                        .setConfiguration(configuration)
                        .build();

        // having a dirty entry in the JobResultStore should make the ApplicationDispatcherBootstrap
        // implementation fail to submit the job
        final JobResultStore jobResultStore = new EmbeddedJobResultStore();
        jobResultStore.createDirtyResult(
                new JobResultEntry(
                        TestingJobResultStore.createSuccessfulJobResult(
                                ApplicationDispatcherBootstrap.ZERO_JOB_ID)));
        final EmbeddedHaServicesWithLeadershipControl haServices =
                new EmbeddedHaServicesWithLeadershipControl(TestingUtils.defaultExecutor()) {

                    @Override
                    public JobResultStore getJobResultStore() {
                        return jobResultStore;
                    }
                };

        final TestingMiniCluster.Builder clusterBuilder =
                TestingMiniCluster.newBuilder(clusterConfiguration)
                        .setHighAvailabilityServicesSupplier(() -> haServices)
                        .setDispatcherResourceManagerComponentFactorySupplier(
                                createApplicationModeDispatcherResourceManagerComponentFactorySupplier(
                                        clusterConfiguration.getConfiguration(),
                                        ErrorHandlingSubmissionJob.createPackagedProgram()));
        try (final MiniCluster cluster = clusterBuilder.build()) {
            // start mini cluster and submit the job
            cluster.start();

            // the cluster should shut down automatically once the application completes
            awaitClusterStopped(cluster, deadline);
        }

        FlinkAssertions.assertThatChainOfCauses(ErrorHandlingSubmissionJob.getSubmissionException())
                .as(
                        "The job's main method shouldn't have been succeeded due to a DuplicateJobSubmissionException.")
                .hasAtLeastOneElementOfType(DuplicateJobSubmissionException.class);

        assertThat(
                        jobResultStore.hasDirtyJobResultEntry(
                                ApplicationDispatcherBootstrap.ZERO_JOB_ID))
                .isTrue();
        assertThat(
                        jobResultStore.hasCleanJobResultEntry(
                                ApplicationDispatcherBootstrap.ZERO_JOB_ID))
                .isFalse();
    }

    @Test
    public void testSubmitFailedJobOnApplicationError() throws Exception {
        final Deadline deadline = Deadline.fromNow(TIMEOUT);
        final JobID jobId = new JobID();
        final Configuration configuration = new Configuration();
        configuration.set(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());
        configuration.set(DeploymentOptions.TARGET, EmbeddedExecutor.NAME);
        configuration.set(ClientOptions.CLIENT_RETRY_PERIOD, Duration.ofMillis(100));
        configuration.set(DeploymentOptions.SHUTDOWN_ON_APPLICATION_FINISH, false);
        configuration.set(DeploymentOptions.SUBMIT_FAILED_JOB_ON_APPLICATION_ERROR, true);
        configuration.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobId.toHexString());
        final TestingMiniClusterConfiguration clusterConfiguration =
                TestingMiniClusterConfiguration.newBuilder()
                        .setConfiguration(configuration)
                        .build();
        final EmbeddedHaServicesWithLeadershipControl haServices =
                new EmbeddedHaServicesWithLeadershipControl(TestingUtils.defaultExecutor());
        final TestingMiniCluster.Builder clusterBuilder =
                TestingMiniCluster.newBuilder(clusterConfiguration)
                        .setHighAvailabilityServicesSupplier(() -> haServices)
                        .setDispatcherResourceManagerComponentFactorySupplier(
                                createApplicationModeDispatcherResourceManagerComponentFactorySupplier(
                                        clusterConfiguration.getConfiguration(),
                                        FailingJob.getProgram()));
        try (final MiniCluster cluster = clusterBuilder.build()) {

            // start mini cluster and submit the job
            cluster.start();

            // wait until the failed job has been submitted
            awaitJobStatus(cluster, jobId, JobStatus.FAILED, deadline);

            final ArchivedExecutionGraph graph = cluster.getArchivedExecutionGraph(jobId).get();

            assertThat(graph.getJobID()).isEqualTo(jobId);
            assertThat(graph.getJobName())
                    .isEqualTo(ApplicationDispatcherBootstrap.FAILED_JOB_NAME);
            assertThat(graph.getFailureInfo())
                    .isNotNull()
                    .extracting(ErrorInfo::getException)
                    .extracting(
                            e -> e.deserializeError(Thread.currentThread().getContextClassLoader()))
                    .satisfies(
                            e ->
                                    assertThat(e)
                                            .isInstanceOf(ProgramInvocationException.class)
                                            .hasRootCauseInstanceOf(RuntimeException.class)
                                            .hasRootCauseMessage(FailingJob.EXCEPTION_MESSAGE));
        }
    }

    private static void awaitClusterStopped(MiniCluster cluster, Deadline deadline)
            throws Exception {
        CommonTestUtils.waitUntilCondition(() -> !cluster.isRunning(), deadline);
    }

    private static void awaitJobStatus(
            MiniCluster cluster, JobID jobId, JobStatus status, Deadline deadline)
            throws Exception {
        CommonTestUtils.waitUntilCondition(
                () -> {
                    try {
                        return cluster.getJobStatus(jobId).get() == status;
                    } catch (ExecutionException e) {
                        if (ExceptionUtils.findThrowable(e, FlinkJobNotFoundException.class)
                                .isPresent()) {
                            // job may not be yet submitted
                            return false;
                        }
                        throw e;
                    }
                },
                deadline);
    }
}
