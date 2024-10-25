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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.forwardgroup.ForwardGroupComputeUtil;
import org.apache.flink.runtime.jobgraph.forwardgroup.StreamNodeForwardGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.streaming.api.graph.util.JobVertexBuildContext;
import org.apache.flink.streaming.api.graph.util.OperatorChainInfo;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.addVertexIndexPrefixInVertexName;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.connect;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.createAndInitializeJobGraph;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.createChain;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.createSourceChainInfo;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.isChainable;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.isSourceChainable;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.markSupportingConcurrentExecutionAttempts;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.preValidate;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.serializeOperatorCoordinatorsAndStreamConfig;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setAllOperatorNonChainedOutputsConfigs;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setManagedMemoryFraction;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setPhysicalEdges;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setSlotSharingAndCoLocation;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setVertexDescription;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.validateHybridShuffleExecuteInBatchMode;

/** Default implementation for {@link AdaptiveGraphGenerator}. */
@Internal
public class AdaptiveGraphManager implements AdaptiveGraphGenerator {

    private final StreamGraph streamGraph;

    private final JobGraph jobGraph;

    private final StreamGraphHasher defaultStreamGraphHasher;

    private final List<StreamGraphHasher> legacyStreamGraphHasher;

    private final Executor serializationExecutor;

    private final AtomicInteger vertexIndexId;

    private final StreamGraphContext streamGraphContext;

    private final Map<Integer, byte[]> hashes;

    private final List<Map<Integer, byte[]>> legacyHashes;

    // Records the id of stream node which job vertex is created.
    private final Map<Integer, Integer> frozenNodeToStartNodeMap;

    // When the downstream vertex is not created, we need to cache the output.
    private final Map<Integer, Map<StreamEdge, NonChainedOutput>> intermediateOutputsCaches;

    // Records the id of the stream node that produces the IntermediateDataSet.
    private final Map<IntermediateDataSetID, Integer> intermediateDataSetIdToProducerMap;

    // Records the ids of the start and end nodes for chained groups in the StreamNodeForwardGroup.
    // When stream edge's partitioner is modified to forward, we need get forward groups by source
    // and target node id.
    private final Map<Integer, StreamNodeForwardGroup> startAndEndNodeIdToForwardGroupMap;

    // Records the chain info that is not ready to create the job vertex, the key is the start node
    // in this chain.
    private final Map<Integer, OperatorChainInfo> pendingChainEntryPoints;

    // The value is the stream node ids belonging to that job vertex.
    private final Map<JobVertexID, Integer> jobVertexToStartNodeMap;

    // The value is the stream node ids belonging to that job vertex.
    private final Map<JobVertexID, List<Integer>> jobVertexToChainedStreamNodeIdsMap;

    // We need cache all job vertices to create JobEdge for downstream vertex.
    private final Map<Integer, JobVertex> jobVerticesCache;

    // Records the ID of the job vertex that has completed execution.
    private final Set<JobVertexID> finishedJobVertices;

    private final AtomicBoolean hasHybridResultPartition;

    private final SlotSharingGroup defaultSlotSharingGroup;

    public AdaptiveGraphManager(
            ClassLoader userClassloader, StreamGraph streamGraph, Executor serializationExecutor) {
        preValidate(streamGraph, userClassloader);
        this.streamGraph = streamGraph;
        this.serializationExecutor = Preconditions.checkNotNull(serializationExecutor);

        this.defaultStreamGraphHasher = new StreamGraphHasherV2();
        this.legacyStreamGraphHasher = Collections.singletonList(new StreamGraphUserHashHasher());

        this.hashes = new HashMap<>();
        this.legacyHashes = Collections.singletonList(new HashMap<>());

        this.jobVerticesCache = new LinkedHashMap<>();
        this.pendingChainEntryPoints = new TreeMap<>();

        this.frozenNodeToStartNodeMap = new HashMap<>();
        this.intermediateOutputsCaches = new HashMap<>();
        this.intermediateDataSetIdToProducerMap = new HashMap<>();
        this.startAndEndNodeIdToForwardGroupMap = new HashMap<>();

        this.vertexIndexId = new AtomicInteger(0);

        this.hasHybridResultPartition = new AtomicBoolean(false);

        this.jobVertexToStartNodeMap = new HashMap<>();
        this.jobVertexToChainedStreamNodeIdsMap = new HashMap<>();

        this.finishedJobVertices = new HashSet<>();

        this.streamGraphContext =
                new DefaultStreamGraphContext(
                        streamGraph,
                        startAndEndNodeIdToForwardGroupMap,
                        frozenNodeToStartNodeMap,
                        intermediateOutputsCaches);

        this.jobGraph = createAndInitializeJobGraph(streamGraph, streamGraph.getJobID());

        this.defaultSlotSharingGroup = new SlotSharingGroup();

        initialization();
    }

    @Override
    public JobGraph getJobGraph() {
        return this.jobGraph;
    }

    @Override
    public StreamGraphContext getStreamGraphContext() {
        return streamGraphContext;
    }

    @Override
    public List<JobVertex> onJobVertexFinished(JobVertexID finishedJobVertexId) {
        this.finishedJobVertices.add(finishedJobVertexId);
        List<StreamNode> streamNodes = new ArrayList<>();
        for (StreamEdge outEdge : getOutputEdgesByVertexId(finishedJobVertexId)) {
            streamNodes.add(streamGraph.getStreamNode(outEdge.getTargetId()));
        }
        return createJobVerticesAndUpdateGraph(streamNodes);
    }

    /**
     * Retrieves the StreamNodeForwardGroup which provides a stream node level ForwardGroup.
     *
     * @param jobVertexId The ID of the JobVertex.
     * @return An instance of {@link StreamNodeForwardGroup}.
     */
    public StreamNodeForwardGroup getStreamNodeForwardGroupByVertexId(JobVertexID jobVertexId) {
        Integer startNodeId = jobVertexToStartNodeMap.get(jobVertexId);
        return startAndEndNodeIdToForwardGroupMap.get(startNodeId);
    }

    /**
     * Retrieves the number of operators that have not yet been converted to job vertex.
     *
     * @return The number of unconverted operators.
     */
    public int getPendingOperatorsCount() {
        return streamGraph.getStreamNodes().size() - frozenNodeToStartNodeMap.size();
    }

    /**
     * Retrieves the IDs of stream nodes that belong to the given job vertex.
     *
     * @param jobVertexId The ID of the JobVertex.
     * @return A list of IDs of stream nodes that belong to the job vertex.
     */
    public List<Integer> getStreamNodeIdsByJobVertexId(JobVertexID jobVertexId) {
        return jobVertexToChainedStreamNodeIdsMap.get(jobVertexId);
    }

    /**
     * Retrieves the ID of the stream node that produces the IntermediateDataSet.
     *
     * @param intermediateDataSetID The ID of the IntermediateDataSet.
     * @return The ID of the stream node that produces the IntermediateDataSet.
     */
    public Integer getProducerStreamNodeId(IntermediateDataSetID intermediateDataSetID) {
        return intermediateDataSetIdToProducerMap.get(intermediateDataSetID);
    }

    private Optional<JobVertexID> findVertexByStreamNodeId(int streamNodeId) {
        if (frozenNodeToStartNodeMap.containsKey(streamNodeId)) {
            Integer startNodeId = frozenNodeToStartNodeMap.get(streamNodeId);
            return Optional.of(jobVerticesCache.get(startNodeId).getID());
        }
        return Optional.empty();
    }

    private List<StreamEdge> getOutputEdgesByVertexId(JobVertexID jobVertexId) {
        JobVertex jobVertex = jobGraph.findVertexByID(jobVertexId);
        List<StreamEdge> outputEdges = new ArrayList<>();
        for (IntermediateDataSet result : jobVertex.getProducedDataSets()) {
            outputEdges.addAll(result.getOutputStreamEdges());
        }
        return outputEdges;
    }

    private void initialization() {
        List<StreamNode> sourceNodes = new ArrayList<>();
        for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
            sourceNodes.add(streamGraph.getStreamNode(sourceNodeId));
        }
        if (jobGraph.isDynamic()) {
            setVertexParallelismsForDynamicGraphIfNecessary(sourceNodes);
        }
        createJobVerticesAndUpdateGraph(sourceNodes);
    }

    private List<JobVertex> createJobVerticesAndUpdateGraph(List<StreamNode> streamNodes) {
        final JobVertexBuildContext jobVertexBuildContext =
                new JobVertexBuildContext(
                        jobGraph,
                        streamGraph,
                        hasHybridResultPartition,
                        hashes,
                        legacyHashes,
                        defaultSlotSharingGroup);

        createOperatorChainInfos(streamNodes, jobVertexBuildContext);

        recordCreatedJobVerticesInfo(jobVertexBuildContext);

        generateConfigForJobVertices(jobVertexBuildContext);

        return new ArrayList<>(jobVertexBuildContext.getJobVerticesInOrder().values());
    }

    private void generateConfigForJobVertices(JobVertexBuildContext jobVertexBuildContext) {
        // this may be used by uncreated down stream vertex.
        final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs =
                new HashMap<>();

        setAllOperatorNonChainedOutputsConfigs(opIntermediateOutputs, jobVertexBuildContext);

        setAllVertexNonChainedOutputsConfigs(opIntermediateOutputs, jobVertexBuildContext);

        connectToFinishedUpStreamVertex(jobVertexBuildContext);

        setPhysicalEdges(jobVertexBuildContext);

        markSupportingConcurrentExecutionAttempts(jobVertexBuildContext);

        validateHybridShuffleExecuteInBatchMode(jobVertexBuildContext);

        // When generating in a single step, there may be differences between the results and the
        // full image generation. We consider this difference to be normal because they do not need
        // to be in the same shared group.
        setSlotSharingAndCoLocation(jobVertexBuildContext);

        setManagedMemoryFraction(jobVertexBuildContext);

        addVertexIndexPrefixInVertexName(jobVertexBuildContext, vertexIndexId);

        setVertexDescription(jobVertexBuildContext);

        serializeOperatorCoordinatorsAndStreamConfig(serializationExecutor, jobVertexBuildContext);
    }

    private void setAllVertexNonChainedOutputsConfigs(
            final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs,
            JobVertexBuildContext jobVertexBuildContext) {
        jobVertexBuildContext
                .getJobVerticesInOrder()
                .keySet()
                .forEach(
                        startNodeId ->
                                setVertexNonChainedOutputsConfig(
                                        startNodeId, opIntermediateOutputs, jobVertexBuildContext));
    }

    private void setVertexNonChainedOutputsConfig(
            Integer startNodeId,
            Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs,
            JobVertexBuildContext jobVertexBuildContext) {

        OperatorChainInfo chainInfo = jobVertexBuildContext.getChainInfo(startNodeId);
        StreamConfig config = chainInfo.getOperatorInfo(startNodeId).getVertexConfig();
        List<StreamEdge> transitiveOutEdges = chainInfo.getTransitiveOutEdges();
        LinkedHashSet<NonChainedOutput> transitiveOutputs = new LinkedHashSet<>();

        for (StreamEdge edge : transitiveOutEdges) {
            NonChainedOutput output = opIntermediateOutputs.get(edge.getSourceId()).get(edge);
            transitiveOutputs.add(output);
            // When a downstream vertex has been created, a connection to the downstream will be
            // created, otherwise only an IntermediateDataSet will be created for it.
            if (jobVertexBuildContext.getJobVerticesInOrder().containsKey(edge.getTargetId())) {
                connect(startNodeId, edge, output, jobVerticesCache, jobVertexBuildContext);
            } else {
                JobVertex jobVertex =
                        jobVertexBuildContext.getJobVerticesInOrder().get(startNodeId);
                IntermediateDataSet dataSet =
                        jobVertex.getOrCreateResultDataSet(
                                output.getDataSetId(), output.getPartitionType());
                dataSet.addOutputStreamEdge(edge);
                // we cache the output here for downstream vertex to create jobEdge.
                intermediateOutputsCaches
                        .computeIfAbsent(edge.getSourceId(), k -> new HashMap<>())
                        .put(edge, output);
            }
            intermediateDataSetIdToProducerMap.put(output.getDataSetId(), edge.getSourceId());
        }
        config.setVertexNonChainedOutputs(new ArrayList<>(transitiveOutputs));
    }

    /**
     * Responds to connect to the upstream job vertex that has completed execution.
     *
     * @param jobVertexBuildContext the job vertex build context.
     */
    private void connectToFinishedUpStreamVertex(JobVertexBuildContext jobVertexBuildContext) {
        Map<Integer, OperatorChainInfo> chainInfos = jobVertexBuildContext.getChainInfosInOrder();
        for (OperatorChainInfo chainInfo : chainInfos.values()) {
            List<StreamEdge> transitiveInEdges = chainInfo.getTransitiveInEdges();
            for (StreamEdge transitiveInEdge : transitiveInEdges) {
                NonChainedOutput output =
                        intermediateOutputsCaches
                                .get(transitiveInEdge.getSourceId())
                                .get(transitiveInEdge);
                Integer sourceStartNodeId =
                        frozenNodeToStartNodeMap.get(transitiveInEdge.getSourceId());
                connect(
                        sourceStartNodeId,
                        transitiveInEdge,
                        output,
                        jobVerticesCache,
                        jobVertexBuildContext);
            }
        }
    }

    private void recordCreatedJobVerticesInfo(JobVertexBuildContext jobVertexBuildContext) {
        Map<Integer, OperatorChainInfo> chainInfos = jobVertexBuildContext.getChainInfosInOrder();
        for (OperatorChainInfo chainInfo : chainInfos.values()) {
            JobVertex jobVertex = jobVertexBuildContext.getJobVertex(chainInfo.getStartNodeId());
            jobVerticesCache.put(chainInfo.getStartNodeId(), jobVertex);
            jobVertexToStartNodeMap.put(jobVertex.getID(), chainInfo.getStartNodeId());
            chainInfo
                    .getAllChainedNodes()
                    .forEach(
                            node -> {
                                frozenNodeToStartNodeMap.put(
                                        node.getId(), chainInfo.getStartNodeId());
                                jobVertexToChainedStreamNodeIdsMap
                                        .computeIfAbsent(
                                                jobVertex.getID(), key -> new ArrayList<>())
                                        .add(node.getId());
                            });
        }
    }

    private void createOperatorChainInfos(
            List<StreamNode> streamNodes, JobVertexBuildContext jobVertexBuildContext) {
        final Map<Integer, OperatorChainInfo> chainEntryPoints =
                buildAndGetChainEntryPoints(streamNodes, jobVertexBuildContext);

        final Collection<OperatorChainInfo> initialEntryPoints =
                chainEntryPoints.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toList());

        for (OperatorChainInfo info : initialEntryPoints) {
            createChain(
                    info.getStartNodeId(),
                    1,
                    info,
                    chainEntryPoints,
                    false,
                    serializationExecutor,
                    jobVertexBuildContext,
                    this::generateHashesByStreamNodeId);

            // We need to record in edges connect to the finished job vertex and create the
            // connection later.
            StreamNode startNode = streamGraph.getStreamNode(info.getStartNodeId());
            for (StreamEdge inEdge : startNode.getInEdges()) {
                if (frozenNodeToStartNodeMap.containsKey(inEdge.getSourceId())) {
                    info.addTransitiveInEdge(inEdge);
                }
            }
        }
    }

    private Map<Integer, OperatorChainInfo> buildAndGetChainEntryPoints(
            List<StreamNode> streamNodes, JobVertexBuildContext jobVertexBuildContext) {
        for (StreamNode streamNode : streamNodes) {
            int streamNodeId = streamNode.getId();
            if (isSourceChainable(streamNode, streamGraph)) {
                generateHashesByStreamNodeId(streamNodeId);
                createSourceChainInfo(streamNode, pendingChainEntryPoints, jobVertexBuildContext);
            } else {
                pendingChainEntryPoints.computeIfAbsent(
                        streamNodeId, ignored -> new OperatorChainInfo(streamNodeId));
            }
        }
        return getChainEntryPoints();
    }

    private Map<Integer, OperatorChainInfo> getChainEntryPoints() {

        final Map<Integer, OperatorChainInfo> chainEntryPoints = new HashMap<>();
        Iterator<Map.Entry<Integer, OperatorChainInfo>> iterator =
                pendingChainEntryPoints.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<Integer, OperatorChainInfo> entry = iterator.next();
            Integer startNodeId = entry.getKey();
            OperatorChainInfo chainInfo = entry.getValue();
            if (!isReadyToCreateJobVertex(chainInfo)) {
                continue;
            }
            chainEntryPoints.put(startNodeId, chainInfo);
            iterator.remove();
        }
        return chainEntryPoints;
    }

    /**
     * Responds to calculating the forward group and reset the parallelism of all nodes in the same
     * forward group to make them the same.
     *
     * @param sourceNodes the source nodes.
     */
    private void setVertexParallelismsForDynamicGraphIfNecessary(List<StreamNode> sourceNodes) {
        Map<Integer, List<StreamEdge>> transitiveNonChainableOutEdgesMap = new HashMap<>();
        Map<StreamNode, List<StreamNode>> topologicallySortedChainedStreamNodesMap =
                new LinkedHashMap<>();
        Set<Integer> visitedStartNodes = new HashSet<>();

        List<StreamNode> enterPoints =
                getEnterPoints(sourceNodes, topologicallySortedChainedStreamNodesMap);

        for (StreamNode streamNode : enterPoints) {
            traverseFullGraph(
                    streamNode.getId(),
                    streamNode.getId(),
                    transitiveNonChainableOutEdgesMap,
                    topologicallySortedChainedStreamNodesMap,
                    visitedStartNodes);
        }
        computeForwardGroupAndSetNodeParallelisms(
                transitiveNonChainableOutEdgesMap, topologicallySortedChainedStreamNodesMap);
    }

    private List<StreamNode> getEnterPoints(
            List<StreamNode> sourceNodes, Map<StreamNode, List<StreamNode>> chainedStreamNodesMap) {
        List<StreamNode> enterPoints = new ArrayList<>();
        for (StreamNode sourceNode : sourceNodes) {
            if (isSourceChainable(sourceNode, streamGraph)) {
                StreamEdge outEdge = sourceNode.getOutEdges().get(0);
                StreamNode startNode = streamGraph.getStreamNode(outEdge.getTargetId());
                chainedStreamNodesMap
                        .computeIfAbsent(startNode, k -> new ArrayList<>())
                        .add(sourceNode);
                enterPoints.add(startNode);
            } else {
                chainedStreamNodesMap.computeIfAbsent(sourceNode, k -> new ArrayList<>());
                enterPoints.add(sourceNode);
            }
        }
        return enterPoints;
    }

    private void traverseFullGraph(
            Integer startNodeId,
            Integer currentNodeId,
            Map<Integer, List<StreamEdge>> transitiveNonChainableOutEdgesMap,
            Map<StreamNode, List<StreamNode>> topologicallySortedChainedStreamNodesMap,
            Set<Integer> visitedStartNodes) {
        if (visitedStartNodes.contains(startNodeId)) {
            return;
        }
        List<StreamEdge> chainableOutputs = new ArrayList<>();
        List<StreamEdge> nonChainableOutputs = new ArrayList<>();
        StreamNode currentNode = streamGraph.getStreamNode(currentNodeId);
        StreamNode startNode = streamGraph.getStreamNode(startNodeId);
        for (StreamEdge streamEdge : currentNode.getOutEdges()) {
            if (isChainable(streamEdge, streamGraph)) {
                chainableOutputs.add(streamEdge);
            } else {
                nonChainableOutputs.add(streamEdge);
                topologicallySortedChainedStreamNodesMap.computeIfAbsent(
                        streamGraph.getStreamNode(streamEdge.getTargetId()),
                        k -> new ArrayList<>());
            }
        }

        transitiveNonChainableOutEdgesMap
                .computeIfAbsent(startNodeId, k -> new ArrayList<>())
                .addAll(nonChainableOutputs);

        topologicallySortedChainedStreamNodesMap.get(startNode).add(currentNode);

        for (StreamEdge chainable : chainableOutputs) {
            traverseFullGraph(
                    startNodeId,
                    chainable.getTargetId(),
                    transitiveNonChainableOutEdgesMap,
                    topologicallySortedChainedStreamNodesMap,
                    visitedStartNodes);
        }
        for (StreamEdge nonChainable : nonChainableOutputs) {
            traverseFullGraph(
                    nonChainable.getTargetId(),
                    nonChainable.getTargetId(),
                    transitiveNonChainableOutEdgesMap,
                    topologicallySortedChainedStreamNodesMap,
                    visitedStartNodes);
        }
        if (currentNodeId.equals(startNodeId)) {
            visitedStartNodes.add(startNodeId);
        }
    }

    private void computeForwardGroupAndSetNodeParallelisms(
            Map<Integer, List<StreamEdge>> transitiveNonChainableOutEdgesMap,
            Map<StreamNode, List<StreamNode>> topologicallySortedChainedStreamNodesMap) {
        // Reset parallelism for chained stream nodes whose parallelism is not configured.
        for (List<StreamNode> chainedStreamNodes :
                topologicallySortedChainedStreamNodesMap.values()) {
            boolean isParallelismConfigured =
                    chainedStreamNodes.stream().anyMatch(StreamNode::isParallelismConfigured);
            if (!isParallelismConfigured && streamGraph.isAutoParallelismEnabled()) {
                chainedStreamNodes.forEach(
                        n -> n.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT, false));
            }
        }
        // We calculate forward group by a set of chained stream nodes, and use the start node to
        // identify the chain group.

        // The value are start nod of upstream chain groups that connect target
        // chain group by forward edge.
        final Map<StreamNode, Set<StreamNode>> chainGroupToProducers = new LinkedHashMap<>();

        for (StreamNode startNode : topologicallySortedChainedStreamNodesMap.keySet()) {
            int startNodeId = startNode.getId();
            // All downstream node connect to current chain group with forward partitioner.
            Set<StreamNode> forwardConsumers =
                    transitiveNonChainableOutEdgesMap.get(startNodeId).stream()
                            .filter(
                                    edge ->
                                            edge.getPartitioner()
                                                    .getClass()
                                                    .equals(ForwardPartitioner.class))
                            .map(StreamEdge::getTargetId)
                            .map(streamGraph::getStreamNode)
                            .collect(Collectors.toSet());
            for (StreamNode forwardConsumer : forwardConsumers) {
                chainGroupToProducers
                        .computeIfAbsent(forwardConsumer, ignored -> new HashSet<>())
                        .add(streamGraph.getStreamNode(startNodeId));
            }
        }

        final Map<Integer, StreamNodeForwardGroup> forwardGroupsByStartNodeId =
                ForwardGroupComputeUtil.computeStreamNodeForwardGroup(
                        topologicallySortedChainedStreamNodesMap,
                        startNode ->
                                chainGroupToProducers.getOrDefault(
                                        startNode, Collections.emptySet()));

        forwardGroupsByStartNodeId.forEach(
                (startNodeId, forwardGroup) -> {
                    this.startAndEndNodeIdToForwardGroupMap.put(startNodeId, forwardGroup);
                    // Gets end node of current chain group by transitiveNonChainableOutEdgesMap
                    transitiveNonChainableOutEdgesMap
                            .get(startNodeId)
                            .forEach(
                                    streamEdge ->
                                            this.startAndEndNodeIdToForwardGroupMap.put(
                                                    streamEdge.getSourceId(), forwardGroup));
                });

        topologicallySortedChainedStreamNodesMap.forEach(this::setParallelismBasedOnForwardGroup);
    }

    private void setParallelismBasedOnForwardGroup(
            StreamNode startNode, List<StreamNode> chainedStreamNodes) {
        StreamNodeForwardGroup streamNodeForwardGroup =
                startAndEndNodeIdToForwardGroupMap.get(startNode.getId());
        // set parallelism for vertices in forward group.
        if (streamNodeForwardGroup != null && streamNodeForwardGroup.isParallelismDecided()) {
            chainedStreamNodes.forEach(
                    streamNode ->
                            streamNode.setParallelism(
                                    streamNodeForwardGroup.getParallelism(), true));
        }
        if (streamNodeForwardGroup != null && streamNodeForwardGroup.isMaxParallelismDecided()) {
            chainedStreamNodes.forEach(
                    streamNode ->
                            streamNode.setMaxParallelism(
                                    streamNodeForwardGroup.getMaxParallelism()));
        }
    }

    private void generateHashesByStreamNodeId(Integer streamNodeId) {
        // Generate deterministic hashes for the nodes in order to identify them across
        // submission if they didn't change.
        if (hashes.containsKey(streamNodeId)) {
            return;
        }
        for (int i = 0; i < legacyStreamGraphHasher.size(); ++i) {
            legacyStreamGraphHasher
                    .get(i)
                    .generateHashesByStreamNodeId(streamNodeId, streamGraph, legacyHashes.get(i));
        }
        Preconditions.checkState(
                defaultStreamGraphHasher.generateHashesByStreamNodeId(
                        streamNodeId, streamGraph, hashes),
                "Failed to generate hash for streamNode with ID '%s'",
                streamNodeId);
    }

    /**
     * Determines whether the chain info is ready to create job vertex based on the following rules:
     *
     * <ul>
     *   <li>The hashes of all upstream nodes of the start node have been generated.
     *   <li>The vertices of all upstream non-source-chained nodes have been created and executed
     *       finished.
     * </ul>
     *
     * @param chainInfo the chain info.
     * @return true if ready, false otherwise.
     */
    private boolean isReadyToCreateJobVertex(OperatorChainInfo chainInfo) {
        Integer startNodeId = chainInfo.getStartNodeId();
        if (frozenNodeToStartNodeMap.containsKey(startNodeId)) {
            return false;
        }
        StreamNode startNode = streamGraph.getStreamNode(startNodeId);
        for (StreamEdge inEdges : startNode.getInEdges()) {
            Integer sourceNodeId = inEdges.getSourceId();
            if (!hashes.containsKey(sourceNodeId)) {
                return false;
            }
            if (chainInfo.getChainedSources().containsKey(sourceNodeId)) {
                continue;
            }
            Optional<JobVertexID> upstreamJobVertex = findVertexByStreamNodeId(sourceNodeId);
            if (upstreamJobVertex.isEmpty()
                    || !finishedJobVertices.contains(upstreamJobVertex.get())) {
                return false;
            }
        }
        return true;
    }
}
