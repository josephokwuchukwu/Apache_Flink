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

package org.apache.flink.graph.library.clustering.undirected;

import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.graph.library.clustering.undirected.AverageClusteringCoefficient.Result;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.NullValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link AverageClusteringCoefficient}. */
public class AverageClusteringCoefficientTest extends AsmTestBase {

    /**
     * Validate a test result.
     *
     * @param graph input graph
     * @param vertexCount result vertex count
     * @param averageClusteringCoefficient result average clustering coefficient
     * @param <T> graph ID type
     * @throws Exception on error
     */
    private static <T extends Comparable<T> & CopyableValue<T>> void validate(
            Graph<T, NullValue, NullValue> graph,
            long vertexCount,
            double averageClusteringCoefficient)
            throws Exception {
        Result result =
                new AverageClusteringCoefficient<T, NullValue, NullValue>().run(graph).execute();

        assertEquals(vertexCount, result.getNumberOfVertices());
        assertEquals(
                averageClusteringCoefficient, result.getAverageClusteringCoefficient(), ACCURACY);
    }

    @Test
    public void testWithSimpleGraph() throws Exception {
        // see results in LocalClusteringCoefficientTest.testSimpleGraph
        validate(undirectedSimpleGraph, 6, (1.0 / 1 + 2.0 / 3 + 2.0 / 3 + 1.0 / 6) / 6);
    }

    @Test
    public void testWithCompleteGraph() throws Exception {
        validate(completeGraph, completeGraphVertexCount, 1.0);
    }

    @Test
    public void testWithEmptyGraphWithVertices() throws Exception {
        validate(emptyGraphWithVertices, emptyGraphVertexCount, 0);
    }

    @Test
    public void testWithEmptyGraphWithoutVertices() throws Exception {
        validate(emptyGraphWithoutVertices, 0, Double.NaN);
    }

    @Test
    public void testWithRMatGraph() throws Exception {
        validate(undirectedRMatGraph(10, 16), 902, 0.421730);
    }
}
