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

import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.StreamOperator;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link StreamNodeForwardGroup}. */
class StreamNodeForwardGroupTest {
    @Test
    void testStreamNodeForwardGroup() {
        Map<StreamNode, List<StreamNode>> chainedNodeGroupsByStartNode = new HashMap<>();
        StreamNode streamNode1 = createStreamNode(0, 1, 1);
        StreamNode streamNode2 = createStreamNode(1, 1, 1);

        chainedNodeGroupsByStartNode.put(streamNode1, List.of(streamNode1));
        chainedNodeGroupsByStartNode.put(streamNode2, List.of(streamNode2));

        StreamNodeForwardGroup forwardGroup =
                new StreamNodeForwardGroup(chainedNodeGroupsByStartNode);
        assertThat(forwardGroup.getParallelism()).isEqualTo(1);
        assertThat(forwardGroup.getMaxParallelism()).isEqualTo(1);
        assertThat(forwardGroup.size()).isEqualTo(2);

        StreamNode streamNode3 = createStreamNode(3, 1, 1);
        chainedNodeGroupsByStartNode.put(streamNode2, List.of(streamNode2, streamNode3));

        StreamNodeForwardGroup forwardGroup2 =
                new StreamNodeForwardGroup(chainedNodeGroupsByStartNode);
        assertThat(forwardGroup2.size()).isEqualTo(3);
    }

    @Test
    void testMergeForwardGroup() {
        StreamNodeForwardGroup forwardGroup =
                createForwardGroupWithSingleNode(createStreamNode(0, -1, -1));

        StreamNodeForwardGroup forwardGroupWithUnDecidedParallelism =
                createForwardGroupWithSingleNode(createStreamNode(1, -1, -1));
        forwardGroup.mergeForwardGroup(forwardGroupWithUnDecidedParallelism);
        assertThat(forwardGroup.isParallelismDecided()).isFalse();
        assertThat(forwardGroup.isMaxParallelismDecided()).isFalse();

        StreamNodeForwardGroup forwardGroupWithDecidedParallelism =
                createForwardGroupWithSingleNode(createStreamNode(2, 2, 4));
        forwardGroup.mergeForwardGroup(forwardGroupWithDecidedParallelism);
        assertThat(forwardGroup.getParallelism()).isEqualTo(2);
        assertThat(forwardGroup.getMaxParallelism()).isEqualTo(4);

        StreamNodeForwardGroup forwardGroupWithLargerMaxParallelism =
                createForwardGroupWithSingleNode(createStreamNode(3, 2, 5));

        // The target max parallelism is larger than source.
        assertThat(forwardGroup.mergeForwardGroup(forwardGroupWithLargerMaxParallelism)).isTrue();
        assertThat(forwardGroup.getMaxParallelism()).isEqualTo(4);

        StreamNodeForwardGroup forwardGroupWithSmallerMaxParallelism =
                createForwardGroupWithSingleNode(createStreamNode(4, 2, 3));
        assertThat(forwardGroup.mergeForwardGroup(forwardGroupWithSmallerMaxParallelism)).isTrue();

        StreamNodeForwardGroup forwardGroupWithMaxParallelismSmallerThanSourceParallelism =
                createForwardGroupWithSingleNode(createStreamNode(5, -1, 1));
        assertThat(
                        forwardGroup.mergeForwardGroup(
                                forwardGroupWithMaxParallelismSmallerThanSourceParallelism))
                .isFalse();

        StreamNodeForwardGroup forwardGroupWithDifferentParallelism =
                createForwardGroupWithSingleNode(createStreamNode(6, 1, 3));
        assertThat(forwardGroup.mergeForwardGroup(forwardGroupWithDifferentParallelism)).isFalse();

        StreamNodeForwardGroup forwardGroupWithUndefinedParallelism =
                createForwardGroupWithSingleNode(createStreamNode(7, -1, 2));
        assertThat(forwardGroup.mergeForwardGroup(forwardGroupWithUndefinedParallelism)).isTrue();

        assertThat(forwardGroup.size()).isEqualTo(6);
        assertThat(forwardGroup.getParallelism()).isEqualTo(2);
        assertThat(forwardGroup.getMaxParallelism()).isEqualTo(2);

        forwardGroup
                .getChainedStreamNodeGroups()
                .forEach(
                        nodes ->
                                nodes.forEach(
                                        node -> {
                                            assertThat(node.getParallelism())
                                                    .isEqualTo(forwardGroup.getParallelism());
                                            assertThat(node.getMaxParallelism())
                                                    .isEqualTo(forwardGroup.getMaxParallelism());
                                        }));
    }

    private static StreamNode createStreamNode(int id, int parallelism, int maxParallelism) {
        StreamNode streamNode =
                new StreamNode(id, null, null, (StreamOperator<?>) null, null, null);
        if (parallelism > 0) {
            streamNode.setParallelism(parallelism);
        }
        if (maxParallelism > 0) {
            streamNode.setMaxParallelism(maxParallelism);
        }
        return streamNode;
    }

    private StreamNodeForwardGroup createForwardGroupWithSingleNode(StreamNode streamNode) {
        Map<StreamNode, List<StreamNode>> chainedNodeGroups = new HashMap<>();
        chainedNodeGroups.put(streamNode, Collections.singletonList(streamNode));
        return new StreamNodeForwardGroup(chainedNodeGroups);
    }
}
