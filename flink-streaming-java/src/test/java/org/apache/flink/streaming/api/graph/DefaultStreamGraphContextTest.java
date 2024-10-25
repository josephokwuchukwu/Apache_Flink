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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.forwardgroup.StreamNodeForwardGroup;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.util.StreamEdgeUpdateRequestInfo;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Unit tests for {@link DefaultStreamGraphContextTest}. */
class DefaultStreamGraphContextTest {
    @Test
    void testModifyStreamEdge() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // fromElements -> Map -> Print
        DataStream<Integer> sourceDataStream = env.fromData(1, 2, 3).setParallelism(1);

        DataStream<Integer> partitionAfterSourceDataStream =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                sourceDataStream.getTransformation(),
                                new RescalePartitioner<>(),
                                StreamExchangeMode.PIPELINED));

        DataStream<Integer> mapDataStream =
                partitionAfterSourceDataStream.map(value -> value).setParallelism(2);

        DataStream<Integer> partitionAfterMapDataStream =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                mapDataStream.getTransformation(),
                                new RescalePartitioner<>(),
                                StreamExchangeMode.PIPELINED));

        partitionAfterMapDataStream.print();

        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setDynamic(true);

        Map<Integer, StreamNodeForwardGroup> forwardGroupsByEndpointNodeIdCache = new HashMap<>();
        Map<Integer, Integer> frozenNodeToStartNodeMap = new HashMap<>();
        Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputsCaches =
                new HashMap<>();

        StreamGraphContext streamGraphContext =
                new DefaultStreamGraphContext(
                        streamGraph,
                        forwardGroupsByEndpointNodeIdCache,
                        frozenNodeToStartNodeMap,
                        opIntermediateOutputsCaches);

        StreamNode sourceNode =
                streamGraph.getStreamNode(streamGraph.getSourceIDs().iterator().next());

        StreamNode targetNode =
                streamGraph.getStreamNode(sourceNode.getOutEdges().get(0).getTargetId());

        StreamEdge targetEdge = sourceNode.getOutEdges().get(0);

        StreamNodeForwardGroup forwardGroup1 =
                new StreamNodeForwardGroup(Map.of(sourceNode, List.of(sourceNode)));
        StreamNodeForwardGroup forwardGroup2 =
                new StreamNodeForwardGroup(Map.of(targetNode, List.of(targetNode)));
        forwardGroupsByEndpointNodeIdCache.put(sourceNode.getId(), forwardGroup1);
        forwardGroupsByEndpointNodeIdCache.put(targetNode.getId(), forwardGroup2);

        StreamEdgeUpdateRequestInfo streamEdgeUpdateRequestInfo =
                new StreamEdgeUpdateRequestInfo(
                                targetEdge.getEdgeId(),
                                targetEdge.getSourceId(),
                                targetEdge.getTargetId())
                        .outputPartitioner(new ForwardPartitioner<>());

        // Modify rescale partitioner to forward partitioner.
        // 1. If the upstream job vertex is created.
        frozenNodeToStartNodeMap.put(
                streamGraph.getSourceIDs().iterator().next(),
                streamGraph.getSourceIDs().iterator().next());
        assertThat(
                        streamGraphContext.modifyStreamEdge(
                                Collections.singletonList(streamEdgeUpdateRequestInfo)))
                .isTrue();
        assertThat(targetEdge.getPartitioner() instanceof RescalePartitioner).isTrue();

        // 2. If the source and target are non-chainable.
        frozenNodeToStartNodeMap.remove(streamGraph.getSourceIDs().iterator().next());
        assertThat(
                        streamGraphContext.modifyStreamEdge(
                                Collections.singletonList(streamEdgeUpdateRequestInfo)))
                .isTrue();
        assertThat(targetEdge.getPartitioner() instanceof RescalePartitioner).isTrue();

        // 3. If the forward group cannot be merged.
        targetNode.setParallelism(1);
        assertThat(
                        streamGraphContext.modifyStreamEdge(
                                Collections.singletonList(streamEdgeUpdateRequestInfo)))
                .isTrue();
        assertThat(targetEdge.getPartitioner() instanceof RescalePartitioner).isTrue();

        // 4. Other wise.
        forwardGroup2 = new StreamNodeForwardGroup(Map.of(targetNode, List.of(targetNode)));
        forwardGroupsByEndpointNodeIdCache.put(targetNode.getId(), forwardGroup2);
        assertThat(
                        streamGraphContext.modifyStreamEdge(
                                Collections.singletonList(streamEdgeUpdateRequestInfo)))
                .isTrue();
        assertThat(targetEdge.getPartitioner() instanceof ForwardPartitioner).isTrue();

        // We cannot modify when target node job vertex is created.
        frozenNodeToStartNodeMap.put(targetEdge.getTargetId(), targetEdge.getTargetId());
        assertThat(
                        streamGraphContext.modifyStreamEdge(
                                Collections.singletonList(streamEdgeUpdateRequestInfo)))
                .isEqualTo(false);
        frozenNodeToStartNodeMap.remove(targetEdge.getTargetId());

        // We cannot modify when edge's subscribing output is reused.
        StreamEdge streamEdge =
                streamGraph
                        .getStreamNode(sourceNode.getOutEdges().get(0).getTargetId())
                        .getOutEdges()
                        .get(0);
        NonChainedOutput output =
                new NonChainedOutput(
                        true,
                        targetEdge.getSourceId(),
                        1,
                        1,
                        100,
                        false,
                        new IntermediateDataSetID(),
                        null,
                        new BroadcastPartitioner<>(),
                        ResultPartitionType.PIPELINED_BOUNDED);
        opIntermediateOutputsCaches.put(
                targetEdge.getSourceId(), Map.of(targetEdge, output, streamEdge, output));
        assertThat(
                        streamGraphContext.modifyStreamEdge(
                                Collections.singletonList(streamEdgeUpdateRequestInfo)))
                .isEqualTo(false);
        opIntermediateOutputsCaches.clear();

        // We cannot modify when edge's partitioner is forward.
        assertThat(
                        streamGraphContext.modifyStreamEdge(
                                Collections.singletonList(streamEdgeUpdateRequestInfo)))
                .isEqualTo(false);
    }
}
