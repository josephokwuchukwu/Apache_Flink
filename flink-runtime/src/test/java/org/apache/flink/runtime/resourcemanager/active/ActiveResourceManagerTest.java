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

package org.apache.flink.runtime.resourcemanager.active;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.io.network.partition.NoOpResourceManagerPartitionTracker;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.TestingSlotManagerBuilder;
import org.apache.flink.runtime.resourcemanager.utils.MockResourceManagerRuntimeServices;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.rpc.TestingRpcServiceResource;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorMemoryConfiguration;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/** Tests for {@link ActiveResourceManager}. */
public class ActiveResourceManagerTest extends TestLogger {

    @ClassRule
    public static final TestingRpcServiceResource RPC_SERVICE_RESOURCE =
            new TestingRpcServiceResource();

    private static final long TIMEOUT_SEC = 5L;
    private static final Time TIMEOUT_TIME = Time.seconds(TIMEOUT_SEC);
    private static final Time TESTING_START_WORKER_INTERVAL = Time.milliseconds(50);

    private static final WorkerResourceSpec WORKER_RESOURCE_SPEC = WorkerResourceSpec.ZERO;

    /** Tests worker successfully requested, started and registered. */
    @Test
    public void testStartNewWorker() throws Exception {
        new Context() {
            {
                final TestingWorkerNode workerNode = new TestingWorkerNode();
                final CompletableFuture<TaskExecutorProcessSpec> requestWorkerFromDriverFuture =
                        new CompletableFuture<>();

                driverBuilder.setRequestResourceFunction(
                        taskExecutorProcessSpec -> {
                            requestWorkerFromDriverFuture.complete(taskExecutorProcessSpec);
                            return CompletableFuture.completedFuture(workerNode);
                        });

                runTest(
                        () -> {
                            // received worker request, verify requesting from driver
                            CompletableFuture<Boolean> startNewWorkerFuture =
                                    runInMainThread(
                                            () ->
                                                    getResourceManager()
                                                            .startNewWorker(WORKER_RESOURCE_SPEC));
                            TaskExecutorProcessSpec taskExecutorProcessSpec =
                                    requestWorkerFromDriverFuture.get(
                                            TIMEOUT_SEC, TimeUnit.SECONDS);

                            assertThat(
                                    startNewWorkerFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    is(true));
                            assertThat(
                                    taskExecutorProcessSpec,
                                    is(
                                            TaskExecutorProcessUtils
                                                    .processSpecFromWorkerResourceSpec(
                                                            flinkConfig, WORKER_RESOURCE_SPEC)));

                            // worker registered, verify registration succeeded
                            CompletableFuture<RegistrationResponse> registerTaskExecutorFuture =
                                    registerTaskExecutor(workerNode.getResourceID());
                            assertThat(
                                    registerTaskExecutorFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    instanceOf(RegistrationResponse.Success.class));
                        });
            }
        };
    }

    /** Tests worker failed while requesting. */
    @Test
    public void testStartNewWorkerFailedRequesting() throws Exception {
        new Context() {
            {
                final TestingWorkerNode workerNode = new TestingWorkerNode();
                final AtomicInteger requestCount = new AtomicInteger(0);

                final List<CompletableFuture<TestingWorkerNode>> workerNodeFutures =
                        new ArrayList<>();
                workerNodeFutures.add(new CompletableFuture<>());
                workerNodeFutures.add(new CompletableFuture<>());

                final List<CompletableFuture<TaskExecutorProcessSpec>>
                        requestWorkerFromDriverFutures = new ArrayList<>();
                requestWorkerFromDriverFutures.add(new CompletableFuture<>());
                requestWorkerFromDriverFutures.add(new CompletableFuture<>());

                driverBuilder.setRequestResourceFunction(
                        taskExecutorProcessSpec -> {
                            int idx = requestCount.getAndIncrement();
                            assertThat(idx, lessThan(2));

                            requestWorkerFromDriverFutures
                                    .get(idx)
                                    .complete(taskExecutorProcessSpec);
                            return workerNodeFutures.get(idx);
                        });

                slotManagerBuilder.setGetRequiredResourcesSupplier(
                        () -> Collections.singletonMap(WORKER_RESOURCE_SPEC, 1));

                runTest(
                        () -> {
                            // received worker request, verify requesting from driver
                            CompletableFuture<Boolean> startNewWorkerFuture =
                                    runInMainThread(
                                            () ->
                                                    getResourceManager()
                                                            .startNewWorker(WORKER_RESOURCE_SPEC));
                            TaskExecutorProcessSpec taskExecutorProcessSpec1 =
                                    requestWorkerFromDriverFutures
                                            .get(0)
                                            .get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            assertThat(
                                    startNewWorkerFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    is(true));
                            assertThat(
                                    taskExecutorProcessSpec1,
                                    is(
                                            TaskExecutorProcessUtils
                                                    .processSpecFromWorkerResourceSpec(
                                                            flinkConfig, WORKER_RESOURCE_SPEC)));

                            // first request failed, verify requesting another worker from driver
                            runInMainThread(
                                    () ->
                                            workerNodeFutures
                                                    .get(0)
                                                    .completeExceptionally(
                                                            new Throwable("testing error")));
                            TaskExecutorProcessSpec taskExecutorProcessSpec2 =
                                    requestWorkerFromDriverFutures
                                            .get(1)
                                            .get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            assertThat(taskExecutorProcessSpec2, is(taskExecutorProcessSpec1));

                            // second request allocated, verify registration succeed
                            runInMainThread(() -> workerNodeFutures.get(1).complete(workerNode));
                            CompletableFuture<RegistrationResponse> registerTaskExecutorFuture =
                                    registerTaskExecutor(workerNode.getResourceID());
                            assertThat(
                                    registerTaskExecutorFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    instanceOf(RegistrationResponse.Success.class));
                        });
            }
        };
    }

    /** Tests worker terminated after requested before registered. */
    @Test
    public void testWorkerTerminatedBeforeRegister() throws Exception {
        new Context() {
            {
                final AtomicInteger requestCount = new AtomicInteger(0);

                final List<TestingWorkerNode> workerNodes = new ArrayList<>();
                workerNodes.add(new TestingWorkerNode());
                workerNodes.add(new TestingWorkerNode());

                final List<CompletableFuture<TaskExecutorProcessSpec>>
                        requestWorkerFromDriverFutures = new ArrayList<>();
                requestWorkerFromDriverFutures.add(new CompletableFuture<>());
                requestWorkerFromDriverFutures.add(new CompletableFuture<>());

                driverBuilder.setRequestResourceFunction(
                        taskExecutorProcessSpec -> {
                            int idx = requestCount.getAndIncrement();
                            assertThat(idx, lessThan(2));

                            requestWorkerFromDriverFutures
                                    .get(idx)
                                    .complete(taskExecutorProcessSpec);
                            return CompletableFuture.completedFuture(workerNodes.get(idx));
                        });

                slotManagerBuilder.setGetRequiredResourcesSupplier(
                        () -> Collections.singletonMap(WORKER_RESOURCE_SPEC, 1));

                runTest(
                        () -> {
                            // received worker request, verify requesting from driver
                            CompletableFuture<Boolean> startNewWorkerFuture =
                                    runInMainThread(
                                            () ->
                                                    getResourceManager()
                                                            .startNewWorker(WORKER_RESOURCE_SPEC));
                            TaskExecutorProcessSpec taskExecutorProcessSpec1 =
                                    requestWorkerFromDriverFutures
                                            .get(0)
                                            .get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            assertThat(
                                    startNewWorkerFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    is(true));
                            assertThat(
                                    taskExecutorProcessSpec1,
                                    is(
                                            TaskExecutorProcessUtils
                                                    .processSpecFromWorkerResourceSpec(
                                                            flinkConfig, WORKER_RESOURCE_SPEC)));

                            // first worker failed before register, verify requesting another worker
                            // from driver
                            runInMainThread(
                                    () ->
                                            getResourceManager()
                                                    .onWorkerTerminated(
                                                            workerNodes.get(0).getResourceID(),
                                                            "terminate for testing"));
                            TaskExecutorProcessSpec taskExecutorProcessSpec2 =
                                    requestWorkerFromDriverFutures
                                            .get(1)
                                            .get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            assertThat(taskExecutorProcessSpec2, is(taskExecutorProcessSpec1));

                            // second worker registered, verify registration succeed
                            CompletableFuture<RegistrationResponse> registerTaskExecutorFuture =
                                    registerTaskExecutor(workerNodes.get(1).getResourceID());
                            assertThat(
                                    registerTaskExecutorFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    instanceOf(RegistrationResponse.Success.class));
                        });
            }
        };
    }

    /** Tests worker terminated after registered. */
    @Test
    public void testWorkerTerminatedAfterRegister() throws Exception {
        new Context() {
            {
                final AtomicInteger requestCount = new AtomicInteger(0);

                final List<TestingWorkerNode> workerNodes = new ArrayList<>();
                workerNodes.add(new TestingWorkerNode());
                workerNodes.add(new TestingWorkerNode());

                final List<CompletableFuture<TaskExecutorProcessSpec>>
                        requestWorkerFromDriverFutures = new ArrayList<>();
                requestWorkerFromDriverFutures.add(new CompletableFuture<>());
                requestWorkerFromDriverFutures.add(new CompletableFuture<>());

                driverBuilder.setRequestResourceFunction(
                        taskExecutorProcessSpec -> {
                            int idx = requestCount.getAndIncrement();
                            assertThat(idx, lessThan(2));

                            requestWorkerFromDriverFutures
                                    .get(idx)
                                    .complete(taskExecutorProcessSpec);
                            return CompletableFuture.completedFuture(workerNodes.get(idx));
                        });

                slotManagerBuilder.setGetRequiredResourcesSupplier(
                        () -> Collections.singletonMap(WORKER_RESOURCE_SPEC, 1));

                runTest(
                        () -> {
                            // received worker request, verify requesting from driver
                            CompletableFuture<Boolean> startNewWorkerFuture =
                                    runInMainThread(
                                            () ->
                                                    getResourceManager()
                                                            .startNewWorker(WORKER_RESOURCE_SPEC));
                            TaskExecutorProcessSpec taskExecutorProcessSpec1 =
                                    requestWorkerFromDriverFutures
                                            .get(0)
                                            .get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            assertThat(
                                    startNewWorkerFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    is(true));
                            assertThat(
                                    taskExecutorProcessSpec1,
                                    is(
                                            TaskExecutorProcessUtils
                                                    .processSpecFromWorkerResourceSpec(
                                                            flinkConfig, WORKER_RESOURCE_SPEC)));

                            // first worker registered, verify registration succeed
                            CompletableFuture<RegistrationResponse> registerTaskExecutorFuture1 =
                                    registerTaskExecutor(workerNodes.get(0).getResourceID());
                            assertThat(
                                    registerTaskExecutorFuture1.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    instanceOf(RegistrationResponse.Success.class));

                            // first worker terminated, verify requesting another worker from driver
                            runInMainThread(
                                    () ->
                                            getResourceManager()
                                                    .onWorkerTerminated(
                                                            workerNodes.get(0).getResourceID(),
                                                            "terminate for testing"));
                            TaskExecutorProcessSpec taskExecutorProcessSpec2 =
                                    requestWorkerFromDriverFutures
                                            .get(1)
                                            .get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            assertThat(taskExecutorProcessSpec2, is(taskExecutorProcessSpec1));

                            // second worker registered, verify registration succeed
                            CompletableFuture<RegistrationResponse> registerTaskExecutorFuture2 =
                                    registerTaskExecutor(workerNodes.get(1).getResourceID());
                            assertThat(
                                    registerTaskExecutorFuture2.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    instanceOf(RegistrationResponse.Success.class));
                        });
            }
        };
    }

    /** Tests worker terminated and is no longer required. */
    @Test
    public void testWorkerTerminatedNoLongerRequired() throws Exception {
        new Context() {
            {
                final TestingWorkerNode workerNode = new TestingWorkerNode();
                final AtomicInteger requestCount = new AtomicInteger(0);

                final List<CompletableFuture<TaskExecutorProcessSpec>>
                        requestWorkerFromDriverFutures = new ArrayList<>();
                requestWorkerFromDriverFutures.add(new CompletableFuture<>());
                requestWorkerFromDriverFutures.add(new CompletableFuture<>());

                driverBuilder.setRequestResourceFunction(
                        taskExecutorProcessSpec -> {
                            int idx = requestCount.getAndIncrement();
                            assertThat(idx, lessThan(2));

                            requestWorkerFromDriverFutures
                                    .get(idx)
                                    .complete(taskExecutorProcessSpec);
                            return CompletableFuture.completedFuture(workerNode);
                        });

                runTest(
                        () -> {
                            // received worker request, verify requesting from driver
                            CompletableFuture<Boolean> startNewWorkerFuture =
                                    runInMainThread(
                                            () ->
                                                    getResourceManager()
                                                            .startNewWorker(WORKER_RESOURCE_SPEC));
                            TaskExecutorProcessSpec taskExecutorProcessSpec =
                                    requestWorkerFromDriverFutures
                                            .get(0)
                                            .get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            assertThat(
                                    startNewWorkerFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    is(true));
                            assertThat(
                                    taskExecutorProcessSpec,
                                    is(
                                            TaskExecutorProcessUtils
                                                    .processSpecFromWorkerResourceSpec(
                                                            flinkConfig, WORKER_RESOURCE_SPEC)));

                            // worker registered, verify registration succeed
                            CompletableFuture<RegistrationResponse> registerTaskExecutorFuture =
                                    registerTaskExecutor(workerNode.getResourceID());
                            assertThat(
                                    registerTaskExecutorFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    instanceOf(RegistrationResponse.Success.class));

                            // worker terminated, verify not requesting new worker
                            runInMainThread(
                                            () -> {
                                                getResourceManager()
                                                        .onWorkerTerminated(
                                                                workerNode.getResourceID(),
                                                                "terminate for testing");
                                                // needs to return something, so that we can use
                                                // `get()` to make sure the main thread processing
                                                // finishes before the assertions
                                                return null;
                                            })
                                    .get(TIMEOUT_SEC, TimeUnit.SECONDS);
                            assertFalse(requestWorkerFromDriverFutures.get(1).isDone());
                        });
            }
        };
    }

    @Test
    public void testCloseTaskManagerConnectionOnWorkerTerminated() throws Exception {
        new Context() {
            {
                final TestingWorkerNode workerNode = new TestingWorkerNode();
                final CompletableFuture<TaskExecutorProcessSpec> requestWorkerFromDriverFuture =
                        new CompletableFuture<>();
                final CompletableFuture<Void> disconnectResourceManagerFuture =
                        new CompletableFuture<>();

                final TestingTaskExecutorGateway taskExecutorGateway =
                        new TestingTaskExecutorGatewayBuilder()
                                .setDisconnectResourceManagerConsumer(
                                        (ignore) -> disconnectResourceManagerFuture.complete(null))
                                .createTestingTaskExecutorGateway();

                driverBuilder.setRequestResourceFunction(
                        taskExecutorProcessSpec -> {
                            requestWorkerFromDriverFuture.complete(taskExecutorProcessSpec);
                            return CompletableFuture.completedFuture(workerNode);
                        });

                runTest(
                        () -> {
                            // request a new worker, terminate it after registered
                            runInMainThread(
                                            () ->
                                                    getResourceManager()
                                                            .startNewWorker(WORKER_RESOURCE_SPEC))
                                    .thenCompose(
                                            (ignore) ->
                                                    registerTaskExecutor(
                                                            workerNode.getResourceID(),
                                                            taskExecutorGateway))
                                    .thenRun(
                                            () ->
                                                    runInMainThread(
                                                            () ->
                                                                    getResourceManager()
                                                                            .onWorkerTerminated(
                                                                                    workerNode
                                                                                            .getResourceID(),
                                                                                    "terminate for testing")));
                            // verify task manager connection is closed
                            disconnectResourceManagerFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);
                        });
            }
        };
    }

    @Test
    public void testStartWorkerIntervalOnWorkerTerminationExceedFailureRate() throws Exception {
        new Context() {
            {
                flinkConfig.setDouble(ResourceManagerOptions.START_WORKER_MAX_FAILURE_RATE, 1);
                flinkConfig.set(
                        ResourceManagerOptions.START_WORKER_RETRY_INTERVAL,
                        Duration.ofMillis(TESTING_START_WORKER_INTERVAL.toMilliseconds()));

                final AtomicInteger requestCount = new AtomicInteger(0);

                final List<TestingWorkerNode> workerNodes = new ArrayList<>();
                workerNodes.add(new TestingWorkerNode());
                workerNodes.add(new TestingWorkerNode());

                final List<CompletableFuture<Long>> requestWorkerFromDriverFutures =
                        new ArrayList<>();
                requestWorkerFromDriverFutures.add(new CompletableFuture<>());
                requestWorkerFromDriverFutures.add(new CompletableFuture<>());

                driverBuilder.setRequestResourceFunction(
                        taskExecutorProcessSpec -> {
                            int idx = requestCount.getAndIncrement();
                            assertThat(idx, lessThan(2));

                            requestWorkerFromDriverFutures
                                    .get(idx)
                                    .complete(System.currentTimeMillis());
                            return CompletableFuture.completedFuture(workerNodes.get(idx));
                        });

                slotManagerBuilder.setGetRequiredResourcesSupplier(
                        () -> Collections.singletonMap(WORKER_RESOURCE_SPEC, 1));

                runTest(
                        () -> {
                            // received worker request, verify requesting from driver
                            CompletableFuture<Boolean> startNewWorkerFuture =
                                    runInMainThread(
                                            () ->
                                                    getResourceManager()
                                                            .startNewWorker(WORKER_RESOURCE_SPEC));
                            long t1 =
                                    requestWorkerFromDriverFutures
                                            .get(0)
                                            .get(TIMEOUT_SEC, TimeUnit.SECONDS);
                            assertThat(
                                    startNewWorkerFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    is(true));

                            // first worker failed before register, verify requesting another worker
                            // from driver
                            runInMainThread(
                                    () ->
                                            getResourceManager()
                                                    .onWorkerTerminated(
                                                            workerNodes.get(0).getResourceID(),
                                                            "terminate for testing"));
                            long t2 =
                                    requestWorkerFromDriverFutures
                                            .get(1)
                                            .get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            // validate trying creating worker twice, with proper interval
                            assertThat(
                                    (t2 - t1),
                                    greaterThanOrEqualTo(
                                            TESTING_START_WORKER_INTERVAL.toMilliseconds()));
                            // second worker registered, verify registration succeed
                            CompletableFuture<RegistrationResponse> registerTaskExecutorFuture =
                                    registerTaskExecutor(workerNodes.get(1).getResourceID());
                            assertThat(
                                    registerTaskExecutorFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    instanceOf(RegistrationResponse.Success.class));
                        });
            }
        };
    }

    @Test
    public void testStartWorkerIntervalOnRequestWorkerFailure() throws Exception {
        new Context() {
            {
                flinkConfig.setDouble(ResourceManagerOptions.START_WORKER_MAX_FAILURE_RATE, 1);
                flinkConfig.set(
                        ResourceManagerOptions.START_WORKER_RETRY_INTERVAL,
                        Duration.ofMillis(TESTING_START_WORKER_INTERVAL.toMilliseconds()));

                final AtomicInteger requestCount = new AtomicInteger(0);
                final TestingWorkerNode workerNode = new TestingWorkerNode();

                final List<CompletableFuture<TestingWorkerNode>> workerNodeFutures =
                        new ArrayList<>();
                workerNodeFutures.add(new CompletableFuture<>());
                workerNodeFutures.add(new CompletableFuture<>());

                final List<CompletableFuture<Long>> requestWorkerFromDriverFutures =
                        new ArrayList<>();
                requestWorkerFromDriverFutures.add(new CompletableFuture<>());
                requestWorkerFromDriverFutures.add(new CompletableFuture<>());

                driverBuilder.setRequestResourceFunction(
                        taskExecutorProcessSpec -> {
                            int idx = requestCount.getAndIncrement();
                            assertThat(idx, lessThan(2));

                            requestWorkerFromDriverFutures
                                    .get(idx)
                                    .complete(System.currentTimeMillis());
                            return workerNodeFutures.get(idx);
                        });

                slotManagerBuilder.setGetRequiredResourcesSupplier(
                        () -> Collections.singletonMap(WORKER_RESOURCE_SPEC, 1));

                runTest(
                        () -> {
                            // received worker request, verify requesting from driver
                            CompletableFuture<Boolean> startNewWorkerFuture =
                                    runInMainThread(
                                            () ->
                                                    getResourceManager()
                                                            .startNewWorker(WORKER_RESOURCE_SPEC));
                            assertThat(
                                    startNewWorkerFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    is(true));

                            long t1 =
                                    requestWorkerFromDriverFutures
                                            .get(0)
                                            .get(TIMEOUT_SEC, TimeUnit.SECONDS);
                            // first request failed, verify requesting another worker from driver
                            runInMainThread(
                                    () ->
                                            workerNodeFutures
                                                    .get(0)
                                                    .completeExceptionally(
                                                            new Throwable("testing error")));
                            long t2 =
                                    requestWorkerFromDriverFutures
                                            .get(1)
                                            .get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            // validate trying creating worker twice, with proper interval
                            assertThat(
                                    (t2 - t1),
                                    greaterThanOrEqualTo(
                                            TESTING_START_WORKER_INTERVAL.toMilliseconds()));

                            // second worker registered, verify registration succeed
                            workerNodeFutures.get(1).complete(workerNode);
                            CompletableFuture<RegistrationResponse> registerTaskExecutorFuture =
                                    registerTaskExecutor(workerNode.getResourceID());
                            assertThat(
                                    registerTaskExecutorFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    instanceOf(RegistrationResponse.Success.class));
                        });
            }
        };
    }

    /** Tests workers from previous attempt successfully recovered and registered. */
    @Test
    public void testRecoverWorkerFromPreviousAttemptWithoutResourceSpec() throws Exception {
        new Context() {
            {
                final TestingWorkerNode workerNode = new TestingWorkerNode();
                final CompletableFuture<Map<WorkerResourceSpec, Integer>>
                        notifyPendingWorkersFuture = new CompletableFuture<>();

                slotManagerBuilder.setNotifyPendingWorkersConsumer(
                        notifyPendingWorkersFuture::complete);

                runTest(
                        () -> {
                            runInMainThread(
                                    () ->
                                            getResourceManager()
                                                    .onPreviousAttemptWorkersRecovered(
                                                            Collections.singleton(workerNode)));
                            CompletableFuture<RegistrationResponse> registerTaskExecutorFuture =
                                    registerTaskExecutor(workerNode.getResourceID());
                            assertThat(
                                    registerTaskExecutorFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    instanceOf(RegistrationResponse.Success.class));
                            assertFalse(notifyPendingWorkersFuture.isDone());
                        });
            }
        };
    }

    @Test
    public void testRecoverWorkerFromPreviousAttemptWithResourceSpec() throws Exception {
        new Context() {
            {
                final TaskExecutorProcessSpec taskExecutorProcessSpec =
                        TaskExecutorProcessUtils.processSpecFromWorkerResourceSpec(
                                flinkConfig, WORKER_RESOURCE_SPEC);
                final TestingWorkerNode workerNode = new TestingWorkerNode(taskExecutorProcessSpec);
                final CompletableFuture<Map<WorkerResourceSpec, Integer>>
                        notifyPendingWorkersFuture = new CompletableFuture<>();

                slotManagerBuilder.setNotifyPendingWorkersConsumer(
                        notifyPendingWorkersFuture::complete);

                runTest(
                        () -> {
                            runInMainThread(
                                    () ->
                                            getResourceManager()
                                                    .onPreviousAttemptWorkersRecovered(
                                                            Collections.singleton(workerNode)));

                            final Map<WorkerResourceSpec, Integer> notifiedPendingWorkers =
                                    notifyPendingWorkersFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);
                            assertThat(notifiedPendingWorkers.size(), is(1));
                            assertThat(notifiedPendingWorkers.get(WORKER_RESOURCE_SPEC), is(1));
                        });
            }
        };
    }

    /** Tests decline unknown worker registration. */
    @Test
    public void testRegisterUnknownWorker() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            CompletableFuture<RegistrationResponse> registerTaskExecutorFuture =
                                    registerTaskExecutor(ResourceID.generate());
                            assertThat(
                                    registerTaskExecutorFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    instanceOf(RegistrationResponse.Decline.class));
                        });
            }
        };
    }

    @Test
    public void testOnError() throws Exception {
        new Context() {
            {
                final Throwable fatalError = new Throwable("Testing fatal error");
                runTest(
                        () -> {
                            runInMainThread(() -> getResourceManager().onError(fatalError));
                            final Throwable reportedError =
                                    getFatalErrorHandler()
                                            .getErrorFuture()
                                            .get(TIMEOUT_SEC, TimeUnit.SECONDS);
                            assertThat(reportedError, is(fatalError));
                        });
            }
        };
    }

    private static class Context {

        final Configuration flinkConfig = new Configuration();
        final TestingResourceManagerDriver.Builder driverBuilder =
                new TestingResourceManagerDriver.Builder();
        final TestingSlotManagerBuilder slotManagerBuilder = new TestingSlotManagerBuilder();

        private ActiveResourceManager<TestingWorkerNode> resourceManager;
        private TestingFatalErrorHandler fatalErrorHandler;

        ActiveResourceManager<TestingWorkerNode> getResourceManager() {
            return resourceManager;
        }

        TestingFatalErrorHandler getFatalErrorHandler() {
            return fatalErrorHandler;
        }

        void runTest(RunnableWithException testMethod) throws Exception {
            fatalErrorHandler = new TestingFatalErrorHandler();
            resourceManager =
                    createAndStartResourceManager(
                            flinkConfig,
                            driverBuilder.build(),
                            slotManagerBuilder.createSlotManager());

            try {
                testMethod.run();
            } finally {
                resourceManager.close();
            }
        }

        private ActiveResourceManager<TestingWorkerNode> createAndStartResourceManager(
                Configuration configuration,
                ResourceManagerDriver<TestingWorkerNode> driver,
                SlotManager slotManager)
                throws Exception {
            final TestingRpcService rpcService = RPC_SERVICE_RESOURCE.getTestingRpcService();
            final MockResourceManagerRuntimeServices rmServices =
                    new MockResourceManagerRuntimeServices(rpcService, TIMEOUT_TIME, slotManager);
            final Duration retryInterval =
                    configuration.get(ResourceManagerOptions.START_WORKER_RETRY_INTERVAL);

            final ActiveResourceManager<TestingWorkerNode> activeResourceManager =
                    new ActiveResourceManager<>(
                            driver,
                            configuration,
                            rpcService,
                            ResourceID.generate(),
                            rmServices.highAvailabilityServices,
                            rmServices.heartbeatServices,
                            rmServices.slotManager,
                            NoOpResourceManagerPartitionTracker::get,
                            rmServices.jobLeaderIdService,
                            new ClusterInformation("localhost", 1234),
                            fatalErrorHandler,
                            UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup(),
                            ActiveResourceManagerFactory.createStartWorkerFailureRater(
                                    configuration),
                            retryInterval,
                            ForkJoinPool.commonPool());

            activeResourceManager.start();
            rmServices.grantLeadership();

            return activeResourceManager;
        }

        void runInMainThread(Runnable runnable) {
            resourceManager.runInMainThread(
                    () -> {
                        runnable.run();
                        return null;
                    },
                    TIMEOUT_TIME);
        }

        <T> CompletableFuture<T> runInMainThread(Callable<T> callable) {
            return resourceManager.runInMainThread(callable, TIMEOUT_TIME);
        }

        CompletableFuture<RegistrationResponse> registerTaskExecutor(ResourceID resourceID) {
            final TaskExecutorGateway taskExecutorGateway =
                    new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
            return registerTaskExecutor(resourceID, taskExecutorGateway);
        }

        CompletableFuture<RegistrationResponse> registerTaskExecutor(
                ResourceID resourceID, TaskExecutorGateway taskExecutorGateway) {
            RPC_SERVICE_RESOURCE
                    .getTestingRpcService()
                    .registerGateway(resourceID.toString(), taskExecutorGateway);

            final TaskExecutorRegistration taskExecutorRegistration =
                    new TaskExecutorRegistration(
                            resourceID.toString(),
                            resourceID,
                            1234,
                            23456,
                            new HardwareDescription(1, 2L, 3L, 4L),
                            TaskExecutorMemoryConfiguration.create(flinkConfig),
                            ResourceProfile.ZERO,
                            ResourceProfile.ZERO);

            return resourceManager
                    .getSelfGateway(ResourceManagerGateway.class)
                    .registerTaskExecutor(taskExecutorRegistration, TIMEOUT_TIME);
        }
    }
}
