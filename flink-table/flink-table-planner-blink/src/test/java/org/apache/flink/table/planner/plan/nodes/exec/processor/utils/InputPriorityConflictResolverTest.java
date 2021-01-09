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

package org.apache.flink.table.planner.plan.nodes.exec.processor.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.TestingBatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Optional;
import java.util.function.Consumer;

/** Tests for {@link InputPriorityConflictResolver}. */
public class InputPriorityConflictResolverTest {

    @Test
    public void testDetectAndResolve() {
        // P = ExecEdge.DamBehavior.PIPELINED, E = ExecEdge.DamBehavior.END_INPUT
        // P100 = PIPELINED + priority 100
        //
        // 0 --------(P0)----> 1 --(P0)-----------> 7
        //  \                    \-(P0)-> 2 -(P0)--/
        //   \-------(P0)----> 3 --(P1)-----------/
        //    \------(P0)----> 4 --(P10)---------/
        //     \              /                 /
        //      \    8 -(P0)-<                 /
        //       \            \               /
        //        \--(E0)----> 5 --(P10)-----/
        // 6 ---------(P100)----------------/
        TestingBatchExecNode[] nodes = new TestingBatchExecNode[9];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = new TestingBatchExecNode();
        }
        nodes[1].addInput(nodes[0], ExecEdge.builder().priority(0).build());
        nodes[2].addInput(nodes[1], ExecEdge.builder().priority(0).build());
        nodes[3].addInput(nodes[0], ExecEdge.builder().priority(0).build());
        nodes[4].addInput(nodes[8], ExecEdge.builder().priority(0).build());
        nodes[4].addInput(nodes[0], ExecEdge.builder().priority(0).build());
        nodes[5].addInput(nodes[8], ExecEdge.builder().priority(0).build());
        nodes[5].addInput(
                nodes[0],
                ExecEdge.builder().damBehavior(ExecEdge.DamBehavior.END_INPUT).priority(0).build());
        nodes[7].addInput(nodes[1], ExecEdge.builder().priority(0).build());
        nodes[7].addInput(nodes[2], ExecEdge.builder().priority(0).build());
        nodes[7].addInput(nodes[3], ExecEdge.builder().priority(1).build());
        nodes[7].addInput(nodes[4], ExecEdge.builder().priority(10).build());
        nodes[7].addInput(nodes[5], ExecEdge.builder().priority(10).build());
        nodes[7].addInput(nodes[6], ExecEdge.builder().priority(100).build());

        InputPriorityConflictResolver resolver =
                new InputPriorityConflictResolver(
                        Collections.singletonList(nodes[7]),
                        ExecEdge.DamBehavior.END_INPUT,
                        ShuffleMode.BATCH,
                        new Configuration());
        resolver.detectAndResolve();
        Assertions.assertEquals(nodes[1], nodes[7].getInputNodes().get(0));
        Assertions.assertEquals(nodes[2], nodes[7].getInputNodes().get(1));
        Assertions.assertTrue(nodes[7].getInputNodes().get(2) instanceof BatchExecExchange);
        Assertions.assertEquals(
                Optional.of(ShuffleMode.BATCH),
                ((BatchExecExchange) nodes[7].getInputNodes().get(2)).getRequiredShuffleMode());
        Assertions.assertEquals(nodes[3], nodes[7].getInputNodes().get(2).getInputNodes().get(0));
        Assertions.assertTrue(nodes[7].getInputNodes().get(3) instanceof BatchExecExchange);
        Assertions.assertEquals(
                Optional.of(ShuffleMode.BATCH),
                ((BatchExecExchange) nodes[7].getInputNodes().get(3)).getRequiredShuffleMode());
        Assertions.assertEquals(nodes[4], nodes[7].getInputNodes().get(3).getInputNodes().get(0));
        Assertions.assertEquals(nodes[5], nodes[7].getInputNodes().get(4));
        Assertions.assertEquals(nodes[6], nodes[7].getInputNodes().get(5));
    }

    @Test
    public void testDeadlockCausedByExchange() {
        // P1 = PIPELINED + priority 1
        //
        // 0 -(P0)-> exchange --(P0)-> 1
        //                    \-(P1)-/
        TestingBatchExecNode[] nodes = new TestingBatchExecNode[2];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = new TestingBatchExecNode();
        }

        BatchExecExchange exchange =
                new BatchExecExchange(
                        ExecEdge.builder().requiredShuffle(ExecEdge.RequiredShuffle.any()).build(),
                        (RowType) nodes[0].getOutputType(),
                        "Exchange");
        exchange.setRequiredShuffleMode(ShuffleMode.BATCH);
        exchange.setInputNodes(Collections.singletonList(nodes[0]));

        nodes[1].addInput(exchange, ExecEdge.builder().priority(0).build());
        nodes[1].addInput(exchange, ExecEdge.builder().priority(1).build());

        InputPriorityConflictResolver resolver =
                new InputPriorityConflictResolver(
                        Collections.singletonList(nodes[1]),
                        ExecEdge.DamBehavior.END_INPUT,
                        ShuffleMode.BATCH,
                        new Configuration());
        resolver.detectAndResolve();

        ExecNode<?> input0 = nodes[1].getInputNodes().get(0);
        ExecNode<?> input1 = nodes[1].getInputNodes().get(1);
        Assertions.assertNotSame(input0, input1);

        Consumer<ExecNode<?>> checkExchange =
                execNode -> {
                    Assertions.assertTrue(execNode instanceof BatchExecExchange);
                    BatchExecExchange e = (BatchExecExchange) execNode;
                    Assertions.assertEquals(
                            Optional.of(ShuffleMode.BATCH), e.getRequiredShuffleMode());
                    Assertions.assertEquals(nodes[0], e.getInputNodes().get(0));
                };
        checkExchange.accept(input0);
        checkExchange.accept(input1);
    }
}
