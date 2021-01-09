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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.runtime.executiongraph.failover.flip1.StronglyConnectedComponentsComputeUtils.computeStronglyConnectedComponents;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/** Unit tests for {@link StronglyConnectedComponentsComputeUtils}. */
public class StronglyConnectedComponentsComputeUtilsTest extends TestLogger {

    @Test
    public void testWithCycles() {
        final List<List<Integer>> edges =
                Arrays.asList(
                        Arrays.asList(2, 3),
                        Arrays.asList(0),
                        Arrays.asList(1),
                        Arrays.asList(4),
                        Collections.emptyList());

        final Set<Set<Integer>> result = computeStronglyConnectedComponents(5, edges);

        final Set<Set<Integer>> expected = new HashSet<>();
        expected.add(new HashSet<>(Arrays.asList(0, 1, 2)));
        expected.add(Collections.singleton(3));
        expected.add(Collections.singleton(4));

        assertThat(result, is(expected));
    }

    @Test
    public void testWithMultipleCycles() {
        final List<List<Integer>> edges =
                Arrays.asList(
                        Arrays.asList(1),
                        Arrays.asList(2),
                        Arrays.asList(0),
                        Arrays.asList(1, 2, 4),
                        Arrays.asList(3, 5),
                        Arrays.asList(2, 6),
                        Arrays.asList(5),
                        Arrays.asList(4, 6, 7));

        final Set<Set<Integer>> result = computeStronglyConnectedComponents(8, edges);

        final Set<Set<Integer>> expected = new HashSet<>();
        expected.add(new HashSet<>(Arrays.asList(0, 1, 2)));
        expected.add(new HashSet<>(Arrays.asList(3, 4)));
        expected.add(new HashSet<>(Arrays.asList(5, 6)));
        expected.add(Collections.singleton(7));

        assertThat(result, is(expected));
    }

    @Test
    public void testWithConnectedCycles() {
        final List<List<Integer>> edges =
                Arrays.asList(
                        Arrays.asList(1),
                        Arrays.asList(2, 4, 5),
                        Arrays.asList(3, 6),
                        Arrays.asList(2, 7),
                        Arrays.asList(0, 5),
                        Arrays.asList(6),
                        Arrays.asList(5),
                        Arrays.asList(3, 6));

        final Set<Set<Integer>> result = computeStronglyConnectedComponents(8, edges);

        final Set<Set<Integer>> expected = new HashSet<>();
        expected.add(new HashSet<>(Arrays.asList(0, 1, 4)));
        expected.add(new HashSet<>(Arrays.asList(2, 3, 7)));
        expected.add(new HashSet<>(Arrays.asList(5, 6)));

        assertThat(result, is(expected));
    }

    @Test
    public void testWithNoEdge() {
        final List<List<Integer>> edges =
                Arrays.asList(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList());

        final Set<Set<Integer>> result = computeStronglyConnectedComponents(5, edges);

        final Set<Set<Integer>> expected = new HashSet<>();
        expected.add(Collections.singleton(0));
        expected.add(Collections.singleton(1));
        expected.add(Collections.singleton(2));
        expected.add(Collections.singleton(3));
        expected.add(Collections.singleton(4));

        assertThat(result, is(expected));
    }

    @Test
    public void testWithNoCycle() {
        final List<List<Integer>> edges =
                Arrays.asList(
                        Arrays.asList(1),
                        Arrays.asList(2),
                        Arrays.asList(3),
                        Arrays.asList(4),
                        Collections.emptyList());

        final Set<Set<Integer>> result = computeStronglyConnectedComponents(5, edges);

        final Set<Set<Integer>> expected = new HashSet<>();
        expected.add(Collections.singleton(0));
        expected.add(Collections.singleton(1));
        expected.add(Collections.singleton(2));
        expected.add(Collections.singleton(3));
        expected.add(Collections.singleton(4));

        assertThat(result, is(expected));
    }

    @Test
    public void testLargeGraph() {
        final int n = 100000;
        final List<List<Integer>> edges = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            edges.add(Collections.singletonList((i + 1) % n));
        }

        final Set<Set<Integer>> result = computeStronglyConnectedComponents(n, edges);

        final Set<Integer> singleComponent =
                IntStream.range(0, n).boxed().collect(Collectors.toSet());

        assertThat(result, is(Collections.singleton(singleComponent)));
    }
}
