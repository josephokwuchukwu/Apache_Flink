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

package org.apache.flink.runtime.jobgraph.forwardgroup;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.graph.StreamNode;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Stream node level implement for {@link ForwardGroup}. */
public class StreamNodeForwardGroup implements ForwardGroup {

    private int parallelism = ExecutionConfig.PARALLELISM_DEFAULT;

    private int maxParallelism = JobVertex.MAX_PARALLELISM_DEFAULT;

    private final Map<StreamNode, List<StreamNode>> chainedStreamNodeGroupsByStartNode =
            new HashMap<>();

    // For a group of chained stream nodes, their parallelism is consistent. In order to make
    // calculation and usage easier, we only use the start node to calculate forward group.
    public StreamNodeForwardGroup(
            final Map<StreamNode, List<StreamNode>> chainedStreamNodeGroupsByStartNode) {
        checkNotNull(chainedStreamNodeGroupsByStartNode);

        Set<Integer> configuredParallelisms =
                chainedStreamNodeGroupsByStartNode.keySet().stream()
                        .map(StreamNode::getParallelism)
                        .filter(val -> val > 0)
                        .collect(Collectors.toSet());

        checkState(configuredParallelisms.size() <= 1);

        if (configuredParallelisms.size() == 1) {
            this.parallelism = configuredParallelisms.iterator().next();
        }

        Set<Integer> configuredMaxParallelisms =
                chainedStreamNodeGroupsByStartNode.keySet().stream()
                        .map(StreamNode::getMaxParallelism)
                        .filter(val -> val > 0)
                        .collect(Collectors.toSet());

        if (!configuredMaxParallelisms.isEmpty()) {
            this.maxParallelism = Collections.min(configuredMaxParallelisms);
            checkState(
                    parallelism == ExecutionConfig.PARALLELISM_DEFAULT
                            || maxParallelism >= parallelism,
                    "There is a start node in the forward group whose maximum parallelism is smaller than the group's parallelism");
        }

        this.chainedStreamNodeGroupsByStartNode.putAll(chainedStreamNodeGroupsByStartNode);
    }

    @Override
    public void setParallelism(int parallelism) {
        checkState(this.parallelism == ExecutionConfig.PARALLELISM_DEFAULT);
        this.parallelism = parallelism;
    }

    @Override
    public boolean isParallelismDecided() {
        return parallelism > 0;
    }

    @Override
    public int getParallelism() {
        checkState(isParallelismDecided());
        return parallelism;
    }

    @Override
    public boolean isMaxParallelismDecided() {
        return maxParallelism > 0;
    }

    @Override
    public int getMaxParallelism() {
        checkState(isMaxParallelismDecided());
        return maxParallelism;
    }

    @VisibleForTesting
    public int size() {
        return chainedStreamNodeGroupsByStartNode.values().stream().mapToInt(List::size).sum();
    }

    public Iterable<StreamNode> getStartNodes() {
        return chainedStreamNodeGroupsByStartNode.keySet();
    }

    public Iterable<List<StreamNode>> getChainedStreamNodeGroups() {
        return chainedStreamNodeGroupsByStartNode.values();
    }

    /**
     * Responds to merge targetForwardGroup into this and update the parallelism information for
     * stream nodes in merged forward group.
     *
     * @param targetForwardGroup The forward group to be merged.
     * @return whether the merge was successful.
     */
    public boolean mergeForwardGroup(StreamNodeForwardGroup targetForwardGroup) {
        checkNotNull(targetForwardGroup);

        if (targetForwardGroup == this) {
            return true;
        }

        if (!ForwardGroupComputeUtil.canTargetMergeIntoSourceForwardGroup(
                this, targetForwardGroup)) {
            return false;
        }

        this.chainedStreamNodeGroupsByStartNode.putAll(
                targetForwardGroup.chainedStreamNodeGroupsByStartNode);

        Set<Integer> configuredParallelisms = new HashSet<>();
        if (this.isParallelismDecided()) {
            configuredParallelisms.add(this.getParallelism());
        }
        if (targetForwardGroup.isParallelismDecided()) {
            configuredParallelisms.add(targetForwardGroup.getParallelism());
        }

        checkState(configuredParallelisms.size() <= 1);

        if (configuredParallelisms.size() == 1) {
            this.parallelism = configuredParallelisms.iterator().next();
        }

        Set<Integer> configuredMaxParallelisms = new HashSet<>();
        if (this.isMaxParallelismDecided()) {
            configuredMaxParallelisms.add(this.getMaxParallelism());
        }
        if (targetForwardGroup.isMaxParallelismDecided()) {
            configuredMaxParallelisms.add(targetForwardGroup.getMaxParallelism());
        }

        if (!configuredMaxParallelisms.isEmpty()) {
            this.maxParallelism = Collections.min(configuredMaxParallelisms);
            checkState(
                    parallelism == ExecutionConfig.PARALLELISM_DEFAULT
                            || maxParallelism >= parallelism);
        }

        if (this.isParallelismDecided() || this.isMaxParallelismDecided()) {
            chainedStreamNodeGroupsByStartNode
                    .values()
                    .forEach(
                            streamNodes ->
                                    streamNodes.forEach(
                                            streamNode -> {
                                                if (this.isParallelismDecided()
                                                        && streamNode.getParallelism()
                                                                != this.parallelism) {
                                                    streamNode.setParallelism(this.parallelism);
                                                }
                                                if (this.isMaxParallelismDecided()
                                                        && streamNode.getMaxParallelism()
                                                                != this.maxParallelism) {
                                                    streamNode.setMaxParallelism(
                                                            this.maxParallelism);
                                                }
                                            }));
        }

        return true;
    }
}
