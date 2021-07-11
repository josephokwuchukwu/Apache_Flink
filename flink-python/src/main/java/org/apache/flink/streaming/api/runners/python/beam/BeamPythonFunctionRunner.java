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

package org.apache.flink.streaming.api.runners.python.beam;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.PythonConfig;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.python.env.ProcessPythonEnvironment;
import org.apache.flink.python.env.PythonEnvironment;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.LongFunctionWithException;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.ModelCoders;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.ImmutableExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.SideInputReference;
import org.apache.beam.runners.core.construction.graph.TimerReference;
import org.apache.beam.runners.core.construction.graph.UserStateReference;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.DefaultJobBundleFactory;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Struct;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.beam.runners.core.construction.BeamUrns.getUrn;

/** A {@link BeamPythonFunctionRunner} used to execute Python functions. */
@Internal
public abstract class BeamPythonFunctionRunner implements PythonFunctionRunner {
    protected static final Logger LOG = LoggerFactory.getLogger(BeamPythonFunctionRunner.class);

    private static final String INPUT_ID = "input";
    private static final String OUTPUT_ID = "output";
    private static final String TRANSFORM_ID = "transform";

    private static final String MAIN_INPUT_NAME = "input";
    private static final String MAIN_OUTPUT_NAME = "output";

    private static final String INPUT_CODER_ID = "input_coder";
    private static final String OUTPUT_CODER_ID = "output_coder";
    private static final String WINDOW_CODER_ID = "window_coder";

    private static final String WINDOW_STRATEGY = "windowing_strategy";

    private static final String MANAGED_MEMORY_RESOURCE_ID = "python-process-managed-memory";
    private static final String PYTHON_WORKER_MEMORY_LIMIT = "_PYTHON_WORKER_MEMORY_LIMIT";

    private final String taskName;

    /** The Python execution environment manager. */
    private final PythonEnvironmentManager environmentManager;

    /** The urn which represents the function kind to be executed. */
    private final String functionUrn;

    /** The options used to configure the Python worker process. */
    private final Map<String, String> jobOptions;

    /** The flinkMetricContainer will be set to null if metric is configured to be turned off. */
    @Nullable private FlinkMetricContainer flinkMetricContainer;

    private final MemoryManager memoryManager;

    /** The fraction of total managed memory in the slot that the Python worker could use. */
    private final double managedMemoryFraction;

    protected final FlinkFnApi.CoderParam.DataType inputDataType;
    protected final FlinkFnApi.CoderParam.DataType outputDataType;

    protected final FlinkFnApi.CoderParam.OutputMode outputMode;

    // ------------------------------------------------------------------------

    private transient boolean bundleStarted;

    /**
     * The bundle factory which has all job-scoped information and can be used to create a {@link
     * StageBundleFactory}.
     */
    private transient JobBundleFactory jobBundleFactory;

    /**
     * The bundle factory which has all of the resources it needs to provide new {@link
     * RemoteBundle}.
     */
    private transient StageBundleFactory stageBundleFactory;

    /** Handler for state requests. */
    private final StateRequestHandler stateRequestHandler;

    /** Handler for bundle progress messages, both during bundle execution and on its completion. */
    private transient BundleProgressHandler progressHandler;

    /**
     * A bundle handler for handling input elements by forwarding them to a remote environment for
     * processing. It holds a collection of {@link FnDataReceiver}s which actually perform the data
     * forwarding work.
     *
     * <p>When a RemoteBundle is closed, it will block until bundle processing is finished on remote
     * resources, and throw an exception if bundle processing has failed.
     */
    private transient RemoteBundle remoteBundle;

    /** The Python function execution result tuple: (raw bytes, length). */
    private transient Tuple2<byte[], Integer> reusableResultTuple;

    /** Buffers the Python function execution result which has still not been processed. */
    @VisibleForTesting protected transient LinkedBlockingQueue<byte[]> resultBuffer;

    /** The receiver which forwards the input elements to a remote environment for processing. */
    @VisibleForTesting protected transient FnDataReceiver<WindowedValue<byte[]>> mainInputReceiver;

    /** The shared resource among Python operators of the same slot. */
    private transient OpaqueMemoryResource<PythonSharedResources> sharedResources;

    public BeamPythonFunctionRunner(
            String taskName,
            PythonEnvironmentManager environmentManager,
            String functionUrn,
            Map<String, String> jobOptions,
            @Nullable FlinkMetricContainer flinkMetricContainer,
            @Nullable KeyedStateBackend keyedStateBackend,
            @Nullable TypeSerializer keySerializer,
            @Nullable TypeSerializer namespaceSerializer,
            MemoryManager memoryManager,
            double managedMemoryFraction,
            FlinkFnApi.CoderParam.DataType inputDataType,
            FlinkFnApi.CoderParam.DataType outputDataType,
            FlinkFnApi.CoderParam.OutputMode outputMode) {
        this.taskName = Preconditions.checkNotNull(taskName);
        this.environmentManager = Preconditions.checkNotNull(environmentManager);
        this.functionUrn = Preconditions.checkNotNull(functionUrn);
        this.jobOptions = Preconditions.checkNotNull(jobOptions);
        this.flinkMetricContainer = flinkMetricContainer;
        this.stateRequestHandler =
                getStateRequestHandler(
                        keyedStateBackend, keySerializer, namespaceSerializer, jobOptions);
        this.memoryManager = memoryManager;
        this.managedMemoryFraction = managedMemoryFraction;
        this.inputDataType = Preconditions.checkNotNull(inputDataType);
        this.outputDataType = Preconditions.checkNotNull(outputDataType);
        this.outputMode = Preconditions.checkNotNull(outputMode);
    }

    // ------------------------------------------------------------------------

    @Override
    public void open(PythonConfig config) throws Exception {
        this.bundleStarted = false;
        this.resultBuffer = new LinkedBlockingQueue<>();
        this.reusableResultTuple = new Tuple2<>();

        // The creation of stageBundleFactory depends on the initialized environment manager.
        environmentManager.open();

        PortablePipelineOptions portableOptions =
                PipelineOptionsFactory.as(PortablePipelineOptions.class);

        if (jobOptions.containsKey(PythonOptions.STATE_CACHE_SIZE.key())) {
            portableOptions
                    .as(ExperimentalOptions.class)
                    .setExperiments(
                            Collections.singletonList(
                                    ExperimentalOptions.STATE_CACHE_SIZE
                                            + "="
                                            + jobOptions.get(
                                                    PythonOptions.STATE_CACHE_SIZE.key())));
        }

        Struct pipelineOptions = PipelineOptionsTranslation.toProto(portableOptions);

        if (memoryManager != null && config.isUsingManagedMemory()) {
            Preconditions.checkArgument(
                    managedMemoryFraction > 0 && managedMemoryFraction <= 1.0,
                    "The configured managed memory fraction for Python worker process must be within (0, 1], was: %s. "
                            + "It may be because the consumer type \"Python\" was missing or set to 0 for the config option \"taskmanager.memory.managed.consumer-weights\"."
                            + managedMemoryFraction);

            final LongFunctionWithException<PythonSharedResources, Exception> initializer =
                    (size) ->
                            new PythonSharedResources(
                                    createJobBundleFactory(pipelineOptions),
                                    createPythonExecutionEnvironment(size));

            sharedResources =
                    memoryManager.getSharedMemoryResourceForManagedMemory(
                            MANAGED_MEMORY_RESOURCE_ID, initializer, managedMemoryFraction);
            LOG.info("Obtained shared Python process of size {} bytes", sharedResources.getSize());
            sharedResources.getResourceHandle().addPythonEnvironmentManager(environmentManager);

            JobBundleFactory jobBundleFactory =
                    sharedResources.getResourceHandle().getJobBundleFactory();
            RunnerApi.Environment environment =
                    sharedResources.getResourceHandle().getEnvironment();
            stageBundleFactory = createStageBundleFactory(jobBundleFactory, environment);
        } else {
            // there is no way to access the MemoryManager for the batch job of old planner,
            // fallback to the way that spawning a Python process for each Python operator
            jobBundleFactory = createJobBundleFactory(pipelineOptions);
            stageBundleFactory =
                    createStageBundleFactory(
                            jobBundleFactory, createPythonExecutionEnvironment(-1));
        }
        progressHandler = getProgressHandler(flinkMetricContainer);
    }

    @Override
    public void close() throws Exception {
        try {
            if (jobBundleFactory != null) {
                jobBundleFactory.close();
            }
        } finally {
            jobBundleFactory = null;
        }

        try {
            if (sharedResources != null) {
                sharedResources.close();
            } else {
                // if sharedResources is not null, the close of environmentManager will be managed
                // in sharedResources,
                // otherwise, we need to close the environmentManager explicitly
                environmentManager.close();
            }
        } finally {
            sharedResources = null;
        }
    }

    @Override
    public void process(byte[] data) throws Exception {
        checkInvokeStartBundle();
        mainInputReceiver.accept(WindowedValue.valueInGlobalWindow(data));
    }

    /** Checks whether to invoke startBundle. */
    private void checkInvokeStartBundle() {
        if (!bundleStarted) {
            startBundle();
            bundleStarted = true;
        }
    }

    @VisibleForTesting
    protected void startBundle() {
        try {
            remoteBundle =
                    stageBundleFactory.getBundle(
                            createOutputReceiverFactory(), stateRequestHandler, progressHandler);
            mainInputReceiver =
                    Preconditions.checkNotNull(
                            Iterables.getOnlyElement(remoteBundle.getInputReceivers().values()),
                            "Failed to retrieve main input receiver.");
        } catch (Throwable t) {
            throw new RuntimeException("Failed to start remote bundle", t);
        }
    }

    @Override
    public Tuple2<byte[], Integer> pollResult() throws Exception {
        byte[] result = resultBuffer.poll();
        if (result == null) {
            return null;
        } else {
            this.reusableResultTuple.f0 = result;
            this.reusableResultTuple.f1 = result.length;
            return this.reusableResultTuple;
        }
    }

    @Override
    public void flush() throws Exception {
        if (bundleStarted) {
            try {
                finishBundle();
            } finally {
                bundleStarted = false;
            }
        }
    }

    private void finishBundle() {
        try {
            remoteBundle.close();
        } catch (Throwable t) {
            throw new RuntimeException("Failed to close remote bundle", t);
        } finally {
            remoteBundle = null;
        }
    }

    // ------------------------------------------------------------------------
    // Python Execution Environment Management
    // ------------------------------------------------------------------------

    /**
     * Creates a specification which specifies the portability Python execution environment. It's
     * used by Beam's portability framework to creates the actual Python execution environment.
     */
    private RunnerApi.Environment createPythonExecutionEnvironment(long memoryLimitBytes)
            throws Exception {
        PythonEnvironment environment = environmentManager.createEnvironment();
        if (environment instanceof ProcessPythonEnvironment) {
            ProcessPythonEnvironment processEnvironment = (ProcessPythonEnvironment) environment;
            Map<String, String> env = processEnvironment.getEnv();
            env.putAll(jobOptions);
            env.put(PYTHON_WORKER_MEMORY_LIMIT, String.valueOf(memoryLimitBytes));
            return Environments.createProcessEnvironment(
                    "", "", processEnvironment.getCommand(), env);
        }
        throw new RuntimeException("Currently only ProcessPythonEnvironment is supported.");
    }

    // ------------------------------------------------------------------------
    // Construct ExecutableStage
    // ------------------------------------------------------------------------

    /**
     * Creates a {@link ExecutableStage} which contains the Python user-defined functions to be
     * executed and all the other information needed to execute them, such as the execution
     * environment, the input and output coder, etc.
     */
    @SuppressWarnings("unchecked")
    private ExecutableStage createExecutableStage(RunnerApi.Environment environment)
            throws Exception {
        RunnerApi.Components components =
                RunnerApi.Components.newBuilder()
                        .putPcollections(
                                INPUT_ID,
                                RunnerApi.PCollection.newBuilder()
                                        .setWindowingStrategyId(WINDOW_STRATEGY)
                                        .setCoderId(INPUT_CODER_ID)
                                        .build())
                        .putPcollections(
                                OUTPUT_ID,
                                RunnerApi.PCollection.newBuilder()
                                        .setWindowingStrategyId(WINDOW_STRATEGY)
                                        .setCoderId(OUTPUT_CODER_ID)
                                        .build())
                        .putTransforms(
                                TRANSFORM_ID,
                                RunnerApi.PTransform.newBuilder()
                                        .setUniqueName(TRANSFORM_ID)
                                        .setSpec(
                                                RunnerApi.FunctionSpec.newBuilder()
                                                        .setUrn(functionUrn)
                                                        .setPayload(
                                                                org.apache.beam.vendor.grpc.v1p26p0
                                                                        .com.google.protobuf
                                                                        .ByteString.copyFrom(
                                                                        getUserDefinedFunctionsProtoBytes()))
                                                        .build())
                                        .putInputs(MAIN_INPUT_NAME, INPUT_ID)
                                        .putOutputs(MAIN_OUTPUT_NAME, OUTPUT_ID)
                                        .build())
                        .putWindowingStrategies(
                                WINDOW_STRATEGY,
                                RunnerApi.WindowingStrategy.newBuilder()
                                        .setWindowCoderId(WINDOW_CODER_ID)
                                        .build())
                        .putCoders(INPUT_CODER_ID, getInputCoderProto())
                        .putCoders(OUTPUT_CODER_ID, getOutputCoderProto())
                        .putCoders(WINDOW_CODER_ID, getWindowCoderProto())
                        .build();

        PipelineNode.PCollectionNode input =
                PipelineNode.pCollection(INPUT_ID, components.getPcollectionsOrThrow(INPUT_ID));
        List<SideInputReference> sideInputs = Collections.EMPTY_LIST;
        List<UserStateReference> userStates = Collections.EMPTY_LIST;
        List<TimerReference> timers = Collections.EMPTY_LIST;
        List<PipelineNode.PTransformNode> transforms =
                Collections.singletonList(
                        PipelineNode.pTransform(
                                TRANSFORM_ID, components.getTransformsOrThrow(TRANSFORM_ID)));
        List<PipelineNode.PCollectionNode> outputs =
                Collections.singletonList(
                        PipelineNode.pCollection(
                                OUTPUT_ID, components.getPcollectionsOrThrow(OUTPUT_ID)));
        return ImmutableExecutableStage.of(
                components,
                environment,
                input,
                sideInputs,
                userStates,
                timers,
                transforms,
                outputs,
                createValueOnlyWireCoderSetting());
    }

    private Collection<RunnerApi.ExecutableStagePayload.WireCoderSetting>
            createValueOnlyWireCoderSetting() throws IOException {
        WindowedValue<byte[]> value = WindowedValue.valueInGlobalWindow(new byte[0]);
        Coder<? extends BoundedWindow> windowCoder = GlobalWindow.Coder.INSTANCE;
        WindowedValue.FullWindowedValueCoder<byte[]> windowedValueCoder =
                WindowedValue.FullWindowedValueCoder.of(ByteArrayCoder.of(), windowCoder);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        windowedValueCoder.encode(value, baos);

        return Arrays.asList(
                RunnerApi.ExecutableStagePayload.WireCoderSetting.newBuilder()
                        .setUrn(getUrn(RunnerApi.StandardCoders.Enum.PARAM_WINDOWED_VALUE))
                        .setPayload(
                                org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString
                                        .copyFrom(baos.toByteArray()))
                        .setInputOrOutputId(INPUT_ID)
                        .build(),
                RunnerApi.ExecutableStagePayload.WireCoderSetting.newBuilder()
                        .setUrn(getUrn(RunnerApi.StandardCoders.Enum.PARAM_WINDOWED_VALUE))
                        .setPayload(
                                org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString
                                        .copyFrom(baos.toByteArray()))
                        .setInputOrOutputId(OUTPUT_ID)
                        .build());
    }

    /** Gets the proto representation of the window coder. */
    private RunnerApi.Coder getWindowCoderProto() {
        return RunnerApi.Coder.newBuilder()
                .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                                .setUrn(ModelCoders.GLOBAL_WINDOW_CODER_URN)
                                .build())
                .build();
    }

    protected abstract byte[] getUserDefinedFunctionsProtoBytes();

    protected abstract RunnerApi.Coder getInputCoderProto();

    protected abstract RunnerApi.Coder getOutputCoderProto();

    // ------------------------------------------------------------------------
    // Construct RemoteBundler
    // ------------------------------------------------------------------------

    private OutputReceiverFactory createOutputReceiverFactory() {
        return new OutputReceiverFactory() {

            // the input value type is always byte array
            @SuppressWarnings("unchecked")
            @Override
            public FnDataReceiver<WindowedValue<byte[]>> create(String pCollectionId) {
                return input -> resultBuffer.add(input.getValue());
            }
        };
    }

    @VisibleForTesting
    public JobBundleFactory createJobBundleFactory(Struct pipelineOptions) throws Exception {
        return DefaultJobBundleFactory.create(
                JobInfo.create(
                        taskName,
                        taskName,
                        environmentManager.createRetrievalToken(),
                        pipelineOptions));
    }

    /** To make the error messages more user friendly, throws an exception with the boot logs. */
    private StageBundleFactory createStageBundleFactory(
            JobBundleFactory jobBundleFactory, RunnerApi.Environment environment) throws Exception {
        try {
            return jobBundleFactory.forStage(createExecutableStage(environment));
        } catch (Throwable e) {
            throw new RuntimeException(environmentManager.getBootLog(), e);
        }
    }

    /**
     * Ignore bundle progress if flinkMetricContainer is null. The flinkMetricContainer will be set
     * to null if metric is configured to be turned off.
     */
    private BundleProgressHandler getProgressHandler(FlinkMetricContainer flinkMetricContainer) {
        if (flinkMetricContainer == null) {
            return BundleProgressHandler.ignored();
        } else {
            return new BundleProgressHandler() {
                @Override
                public void onProgress(BeamFnApi.ProcessBundleProgressResponse progress) {
                    flinkMetricContainer.updateMetrics(taskName, progress.getMonitoringInfosList());
                }

                @Override
                public void onCompleted(BeamFnApi.ProcessBundleResponse response) {
                    flinkMetricContainer.updateMetrics(taskName, response.getMonitoringInfosList());
                }
            };
        }
    }

    private static StateRequestHandler getStateRequestHandler(
            KeyedStateBackend keyedStateBackend,
            TypeSerializer keySerializer,
            TypeSerializer namespaceSerializer,
            Map<String, String> jobOptions) {
        if (keyedStateBackend == null) {
            return StateRequestHandler.unsupported();
        } else {
            assert keySerializer != null;
            return new SimpleStateRequestHandler(
                    keyedStateBackend, keySerializer, namespaceSerializer, jobOptions);
        }
    }
}
