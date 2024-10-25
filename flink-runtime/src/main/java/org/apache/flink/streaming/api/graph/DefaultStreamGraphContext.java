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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.jobgraph.forwardgroup.ForwardGroupComputeUtil;
import org.apache.flink.runtime.jobgraph.forwardgroup.StreamNodeForwardGroup;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamGraph;
import org.apache.flink.streaming.api.graph.util.StreamEdgeUpdateRequestInfo;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/** Default implementation for {@link StreamGraphContext}. */
@Internal
public class DefaultStreamGraphContext implements StreamGraphContext {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultStreamGraphContext.class);

    private final StreamGraph streamGraph;
    private final ImmutableStreamGraph immutableStreamGraph;
    private final Map<Integer, StreamNodeForwardGroup> startAndEndNodeIdToForwardGroupMap;
    private final Map<Integer, Integer> frozenNodeToStartNodeMap;
    private final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputsCaches;

    public DefaultStreamGraphContext(
            StreamGraph streamGraph,
            Map<Integer, StreamNodeForwardGroup> startAndEndNodeIdToForwardGroupMap,
            Map<Integer, Integer> frozenNodeToStartNodeMap,
            Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputsCaches) {
        this.streamGraph = streamGraph;
        this.immutableStreamGraph = new ImmutableStreamGraph(streamGraph);
        this.startAndEndNodeIdToForwardGroupMap = startAndEndNodeIdToForwardGroupMap;
        this.frozenNodeToStartNodeMap = frozenNodeToStartNodeMap;
        this.opIntermediateOutputsCaches = opIntermediateOutputsCaches;
    }

    @Override
    public ImmutableStreamGraph getStreamGraph() {
        return immutableStreamGraph;
    }

    @Override
    public boolean modifyStreamEdge(List<StreamEdgeUpdateRequestInfo> requestInfos) {
        // We first verify the legality of all requestInfos to ensure that all requests can be
        // modified atomically.
        for (StreamEdgeUpdateRequestInfo requestInfo : requestInfos) {
            if (!modifyStreamEdgeValidate(requestInfo)) {
                return false;
            }
        }

        for (StreamEdgeUpdateRequestInfo requestInfo : requestInfos) {
            StreamEdge targetEdge =
                    getStreamEdge(
                            requestInfo.getSourceId(),
                            requestInfo.getTargetId(),
                            requestInfo.getEdgeId());
            StreamPartitioner<?> newPartitioner = requestInfo.getOutputPartitioner();
            if (newPartitioner != null) {
                modifyOutputPartitioner(targetEdge, newPartitioner);
            }
        }

        return true;
    }

    private boolean modifyStreamEdgeValidate(StreamEdgeUpdateRequestInfo requestInfo) {
        Integer sourceNodeId = requestInfo.getSourceId();
        Integer targetNodeId = requestInfo.getTargetId();

        StreamEdge targetEdge = getStreamEdge(sourceNodeId, targetNodeId, requestInfo.getEdgeId());

        if (targetEdge == null) {
            return false;
        }

        // Modification is not allowed when the subscribing output is reused.
        Map<StreamEdge, NonChainedOutput> opIntermediateOutputs =
                opIntermediateOutputsCaches.get(sourceNodeId);
        NonChainedOutput output =
                opIntermediateOutputs != null ? opIntermediateOutputs.get(targetEdge) : null;
        if (output != null) {
            Set<StreamEdge> consumerStreamEdges =
                    opIntermediateOutputs.entrySet().stream()
                            .filter(entry -> entry.getValue().equals(output))
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toSet());
            if (consumerStreamEdges.size() != 1) {
                LOG.info(
                        "Modification for edge {} is not allowed as the subscribing output is reused.",
                        targetEdge);
                return false;
            }
        }

        if (frozenNodeToStartNodeMap.containsKey(targetNodeId)) {
            LOG.info(
                    "Modification for edge {} is not allowed as the target node with id {} is in frozen list.",
                    targetEdge,
                    targetNodeId);
            return false;
        }

        StreamPartitioner<?> newPartitioner = requestInfo.getOutputPartitioner();
        if (newPartitioner != null
                && targetEdge.getPartitioner().getClass().equals(ForwardPartitioner.class)) {
            LOG.info(
                    "Modification for edge {} is not allowed as the origin partitioner is ForwardPartitioner.",
                    targetEdge);
            return false;
        }

        return true;
    }

    private void modifyOutputPartitioner(
            StreamEdge targetEdge, StreamPartitioner<?> newPartitioner) {
        if (newPartitioner == null || targetEdge == null) {
            return;
        }
        Integer sourceNodeId = targetEdge.getSourceId();
        Integer targetNodeId = targetEdge.getTargetId();

        StreamPartitioner<?> oldPartitioner = targetEdge.getPartitioner();

        targetEdge.setPartitioner(newPartitioner);

        // For non-chainable edges, we change the ForwardPartitioner to RescalePartitioner to avoid
        // limiting the parallelism of the downstream node by the forward edge.
        // 1. If the upstream job vertex is created.
        if (targetEdge.getPartitioner() instanceof ForwardPartitioner
                && frozenNodeToStartNodeMap.containsKey(sourceNodeId)) {
            targetEdge.setPartitioner(new RescalePartitioner<>());
        }
        // 2. If the source and target are non-chainable.
        if (targetEdge.getPartitioner() instanceof ForwardPartitioner
                && !StreamingJobGraphGenerator.isChainable(targetEdge, streamGraph)) {
            targetEdge.setPartitioner(new RescalePartitioner<>());
        }
        // 3. If the forward group cannot be merged.
        if (targetEdge.getPartitioner() instanceof ForwardPartitioner
                && !mergeForwardGroups(sourceNodeId, targetNodeId)) {
            targetEdge.setPartitioner(new RescalePartitioner<>());
        }

        Map<StreamEdge, NonChainedOutput> opIntermediateOutputs =
                opIntermediateOutputsCaches.get(sourceNodeId);
        NonChainedOutput output =
                opIntermediateOutputs != null ? opIntermediateOutputs.get(targetEdge) : null;
        if (output != null) {
            output.setPartitioner(targetEdge.getPartitioner());
        }
        LOG.info(
                "The original partitioner of the edge {} is: {} , requested change to: {} , and finally modified to: {}.",
                targetEdge,
                oldPartitioner,
                newPartitioner,
                targetEdge.getPartitioner());
    }

    private boolean mergeForwardGroups(Integer sourceNodeId, Integer targetNodeId) {
        StreamNodeForwardGroup sourceForwardGroup =
                startAndEndNodeIdToForwardGroupMap.get(sourceNodeId);
        StreamNodeForwardGroup targetForwardGroup =
                startAndEndNodeIdToForwardGroupMap.get(targetNodeId);
        if (sourceForwardGroup == null || targetForwardGroup == null) {
            return false;
        }

        if (!ForwardGroupComputeUtil.canTargetMergeIntoSourceForwardGroup(
                sourceForwardGroup, targetForwardGroup)) {
            return false;
        }

        // sanity check
        checkState(sourceForwardGroup.mergeForwardGroup(targetForwardGroup));
        // Update forwardGroupsByStartNodeIdCache.
        targetForwardGroup
                .getStartNodes()
                .forEach(
                        startNode ->
                                startAndEndNodeIdToForwardGroupMap.put(
                                        startNode.getId(), sourceForwardGroup));
        return true;
    }

    private StreamEdge getStreamEdge(Integer sourceId, Integer targetId, String edgeId) {
        for (StreamEdge edge : streamGraph.getStreamEdges(sourceId, targetId)) {
            if (edge.getEdgeId().equals(edgeId)) {
                return edge;
            }
        }
        return null;
    }
}
