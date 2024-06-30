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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Predicate;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link org.apache.flink.streaming.api.transformations.SinkTransformation}.
 *
 * <p>ATTENTION: This test is extremely brittle. Do NOT remove, add or re-order test cases.
 */
@RunWith(Parameterized.class)
public abstract class SinkTransformationTranslatorITCaseBase<SinkT> extends TestLogger {

    @Parameterized.Parameters(name = "Execution Mode: {0}")
    public static Collection<Object> data() {
        return Arrays.asList(RuntimeExecutionMode.STREAMING, RuntimeExecutionMode.BATCH);
    }

    @Parameterized.Parameter() public RuntimeExecutionMode runtimeExecutionMode;

    static final String NAME = "FileSink";
    static final String SLOT_SHARE_GROUP = "FileGroup";
    static final String UID = "FileUid";
    static final int PARALLELISM = 2;

    abstract SinkT simpleSink();

    abstract SinkT sinkWithCommitter();

    abstract DataStreamSink<Integer> sinkTo(DataStream<Integer> stream, SinkT sink);

    @Test
    public void generateWriterTopology() {
        final StreamGraph streamGraph = buildGraph(simpleSink(), runtimeExecutionMode);

        final StreamNode sourceNode = findNodeName(streamGraph, node -> node.contains("Source"));
        final StreamNode writerNode = findWriter(streamGraph);

        assertThat(streamGraph.getStreamNodes().size(), equalTo(2));

        validateTopology(
                sourceNode,
                IntSerializer.class,
                writerNode,
                SinkWriterOperatorFactory.class,
                PARALLELISM,
                -1);
    }

    @Test
    public void generateWriterCommitterTopology() {

        final StreamGraph streamGraph = buildGraph(sinkWithCommitter(), runtimeExecutionMode);

        final StreamNode sourceNode = findNodeName(streamGraph, node -> node.contains("Source"));
        final StreamNode writerNode = findWriter(streamGraph);

        validateTopology(
                sourceNode,
                IntSerializer.class,
                writerNode,
                SinkWriterOperatorFactory.class,
                PARALLELISM,
                -1);

        final StreamNode committerNode =
                findNodeName(streamGraph, name -> name.contains("Committer"));

        assertThat(streamGraph.getStreamNodes().size(), equalTo(3));

        validateTopology(
                writerNode,
                SimpleVersionedSerializerTypeSerializerProxy.class,
                committerNode,
                CommitterOperatorFactory.class,
                PARALLELISM,
                -1);
    }

    @Test
    public void testParallelismConfigured() {
        testParallelismConfiguredInternal(true);

        testParallelismConfiguredInternal(false);
    }

    private void testParallelismConfiguredInternal(boolean setSinkParallelism) {
        final StreamGraph streamGraph =
                buildGraph(sinkWithCommitter(), runtimeExecutionMode, setSinkParallelism);

        final StreamNode writerNode = findWriter(streamGraph);
        final StreamNode committerNode = findCommitter(streamGraph);

        assertThat(writerNode.isParallelismConfigured(), equalTo(setSinkParallelism));
        assertThat(committerNode.isParallelismConfigured(), equalTo(setSinkParallelism));
    }

    StreamNode findWriter(StreamGraph streamGraph) {
        return findNodeName(
                streamGraph, name -> name.contains("Writer") && !name.contains("Committer"));
    }

    StreamNode findCommitter(StreamGraph streamGraph) {
        return findNodeName(
                streamGraph,
                name -> name.contains("Committer") && !name.contains("Global Committer"));
    }

    StreamNode findGlobalCommitter(StreamGraph streamGraph) {
        return findNodeName(streamGraph, name -> name.contains("Global Committer"));
    }

    @Test(expected = IllegalStateException.class)
    public void throwExceptionWithoutSettingUid() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, runtimeExecutionMode);
        env.configure(config, getClass().getClassLoader());
        // disable auto generating uid
        env.getConfig().disableAutoGeneratedUIDs();
        sinkTo(env.fromElements(1, 2), simpleSink());
        env.getStreamGraph();
    }

    @Test
    public void disableOperatorChain() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStreamSource<Integer> src = env.fromElements(1, 2);
        final DataStreamSink<Integer> dataStreamSink = sinkTo(src, sinkWithCommitter()).name(NAME);
        dataStreamSink.disableChaining();

        final StreamGraph streamGraph = env.getStreamGraph();
        final StreamNode writer = findWriter(streamGraph);
        final StreamNode committer = findCommitter(streamGraph);

        assertThat(writer.getOperatorFactory().getChainingStrategy(), is(ChainingStrategy.NEVER));
        assertThat(
                committer.getOperatorFactory().getChainingStrategy(), is(ChainingStrategy.NEVER));
    }

    void validateTopology(
            StreamNode src,
            Class<?> srcOutTypeInfo,
            StreamNode dest,
            Class<? extends StreamOperatorFactory> operatorFactoryClass,
            int expectedParallelism,
            int expectedMaxParallelism) {

        // verify src node
        final StreamEdge srcOutEdge = src.getOutEdges().get(0);
        assertThat(srcOutEdge.getTargetId(), equalTo(dest.getId()));
        assertThat(src.getTypeSerializerOut(), instanceOf(srcOutTypeInfo));

        // verify dest node input
        final StreamEdge destInputEdge = dest.getInEdges().get(0);
        assertThat(destInputEdge.getSourceId(), equalTo(src.getId()));
        assertThat(dest.getTypeSerializersIn()[0], instanceOf(srcOutTypeInfo));

        // make sure 2 sink operators have different names/uid
        assertThat(dest.getOperatorName(), not(equalTo(src.getOperatorName())));
        assertThat(dest.getTransformationUID(), not(equalTo(src.getTransformationUID())));

        assertThat(dest.getOperatorFactory(), instanceOf(operatorFactoryClass));
        assertThat(dest.getParallelism(), equalTo(expectedParallelism));
        assertThat(dest.getMaxParallelism(), equalTo(expectedMaxParallelism));
        assertThat(dest.getOperatorFactory().getChainingStrategy(), is(ChainingStrategy.ALWAYS));
        assertThat(dest.getSlotSharingGroup(), equalTo(SLOT_SHARE_GROUP));
    }

    StreamGraph buildGraph(SinkT sink, RuntimeExecutionMode runtimeExecutionMode) {
        return buildGraph(sink, runtimeExecutionMode, true);
    }

    StreamGraph buildGraph(
            SinkT sink, RuntimeExecutionMode runtimeExecutionMode, boolean setSinkParallelism) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, runtimeExecutionMode);
        env.configure(config, getClass().getClassLoader());
        final DataStreamSource<Integer> src = env.fromElements(1, 2);
        final DataStreamSink<Integer> dataStreamSink = sinkTo(src.rebalance(), sink);
        setSinkProperty(dataStreamSink, setSinkParallelism);
        // Trigger the plan generation but do not clear the transformations
        env.getExecutionPlan();
        return env.getStreamGraph();
    }

    private void setSinkProperty(
            DataStreamSink<Integer> dataStreamSink, boolean setSinkParallelism) {
        dataStreamSink.name(NAME);
        dataStreamSink.uid(UID);
        if (setSinkParallelism) {
            dataStreamSink.setParallelism(SinkTransformationTranslatorITCaseBase.PARALLELISM);
        }
        dataStreamSink.slotSharingGroup(SLOT_SHARE_GROUP);
    }

    StreamNode findNodeName(StreamGraph streamGraph, Predicate<String> predicate) {
        return streamGraph.getStreamNodes().stream()
                .filter(node -> predicate.test(node.getOperatorName()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Can not find the node"));
    }
}
