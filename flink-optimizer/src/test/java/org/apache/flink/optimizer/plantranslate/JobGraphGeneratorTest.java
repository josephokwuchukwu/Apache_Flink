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

package org.apache.flink.optimizer.plantranslate;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.BlockingShuffleOutputFormat;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.Operator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.testfunctions.IdentityMapper;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphUtils;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.util.AbstractID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class JobGraphGeneratorTest {

    @TempDir private java.nio.file.Path tmp;

    /**
     * Verifies that the resources are merged correctly for chained operators when generating job
     * graph
     */
    @Test
    void testResourcesForChainedOperators() throws Exception {
        ResourceSpec resource1 = ResourceSpec.newBuilder(0.1, 100).build();
        ResourceSpec resource2 = ResourceSpec.newBuilder(0.2, 200).build();
        ResourceSpec resource3 = ResourceSpec.newBuilder(0.3, 300).build();
        ResourceSpec resource4 = ResourceSpec.newBuilder(0.4, 400).build();
        ResourceSpec resource5 = ResourceSpec.newBuilder(0.5, 500).build();
        ResourceSpec resource6 = ResourceSpec.newBuilder(0.6, 600).build();
        ResourceSpec resource7 = ResourceSpec.newBuilder(0.7, 700).build();

        Method opMethod = Operator.class.getDeclaredMethod("setResources", ResourceSpec.class);
        opMethod.setAccessible(true);

        Method sinkMethod = DataSink.class.getDeclaredMethod("setResources", ResourceSpec.class);
        sinkMethod.setAccessible(true);

        MapFunction<Long, Long> mapFunction =
                new MapFunction<Long, Long>() {
                    @Override
                    public Long map(Long value) throws Exception {
                        return value;
                    }
                };

        FilterFunction<Long> filterFunction =
                new FilterFunction<Long>() {
                    @Override
                    public boolean filter(Long value) throws Exception {
                        return false;
                    }
                };

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Long> input = env.fromElements(1L, 2L, 3L);
        opMethod.invoke(input, resource1);

        DataSet<Long> map1 = input.map(mapFunction);
        opMethod.invoke(map1, resource2);

        // CHAIN(Source -> Map -> Filter)
        DataSet<Long> filter1 = map1.filter(filterFunction);
        opMethod.invoke(filter1, resource3);

        IterativeDataSet<Long> startOfIteration = filter1.iterate(10);
        opMethod.invoke(startOfIteration, resource4);

        DataSet<Long> map2 = startOfIteration.map(mapFunction);
        opMethod.invoke(map2, resource5);

        // CHAIN(Map -> Filter)
        DataSet<Long> feedback = map2.filter(filterFunction);
        opMethod.invoke(feedback, resource6);

        DataSink<Long> sink =
                startOfIteration.closeWith(feedback).output(new DiscardingOutputFormat<Long>());
        sinkMethod.invoke(sink, resource7);

        JobGraph jobGraph = compileJob(env);

        JobVertex sourceMapFilterVertex =
                jobGraph.getVerticesSortedTopologicallyFromSources().get(0);
        JobVertex iterationHeadVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(1);
        JobVertex feedbackVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(2);
        JobVertex sinkVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(3);
        JobVertex iterationSyncVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(4);

        assertThat(sourceMapFilterVertex.getMinResources())
                .isEqualTo(resource1.merge(resource2).merge(resource3));
        assertThat(iterationHeadVertex.getPreferredResources()).isEqualTo(resource4);
        assertThat(feedbackVertex.getMinResources()).isEqualTo(resource5.merge(resource6));
        assertThat(sinkVertex.getPreferredResources()).isEqualTo(resource7);
        assertThat(iterationSyncVertex.getMinResources()).isEqualTo(resource4);
    }

    /**
     * Verifies that the resources are set onto each job vertex correctly when generating job graph
     * which covers the delta iteration case
     */
    @Test
    void testResourcesForDeltaIteration() throws Exception {
        ResourceSpec resource1 = ResourceSpec.newBuilder(0.1, 100).build();
        ResourceSpec resource2 = ResourceSpec.newBuilder(0.2, 200).build();
        ResourceSpec resource3 = ResourceSpec.newBuilder(0.3, 300).build();
        ResourceSpec resource4 = ResourceSpec.newBuilder(0.4, 400).build();
        ResourceSpec resource5 = ResourceSpec.newBuilder(0.5, 500).build();
        ResourceSpec resource6 = ResourceSpec.newBuilder(0.6, 600).build();

        Method opMethod = Operator.class.getDeclaredMethod("setResources", ResourceSpec.class);
        opMethod.setAccessible(true);

        Method deltaMethod =
                DeltaIteration.class.getDeclaredMethod("setResources", ResourceSpec.class);
        deltaMethod.setAccessible(true);

        Method sinkMethod = DataSink.class.getDeclaredMethod("setResources", ResourceSpec.class);
        sinkMethod.setAccessible(true);

        MapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> mapFunction =
                new MapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> map(Tuple2<Long, Long> value) throws Exception {
                        return value;
                    }
                };

        FilterFunction<Tuple2<Long, Long>> filterFunction =
                new FilterFunction<Tuple2<Long, Long>>() {
                    @Override
                    public boolean filter(Tuple2<Long, Long> value) throws Exception {
                        return false;
                    }
                };

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Long, Long>> input = env.fromElements(new Tuple2<>(1L, 2L));
        opMethod.invoke(input, resource1);

        // CHAIN(Map -> Filter)
        DataSet<Tuple2<Long, Long>> map = input.map(mapFunction);
        opMethod.invoke(map, resource2);

        DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration =
                map.iterateDelta(map, 100, 0).registerAggregator("test", new LongSumAggregator());
        deltaMethod.invoke(iteration, resource3);

        DataSet<Tuple2<Long, Long>> delta = iteration.getWorkset().map(mapFunction);
        opMethod.invoke(delta, resource4);

        DataSet<Tuple2<Long, Long>> feedback = delta.filter(filterFunction);
        opMethod.invoke(feedback, resource5);

        DataSink<Tuple2<Long, Long>> sink =
                iteration
                        .closeWith(delta, feedback)
                        .output(new DiscardingOutputFormat<Tuple2<Long, Long>>());
        sinkMethod.invoke(sink, resource6);

        JobGraph jobGraph = compileJob(env);

        JobVertex sourceMapVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(0);
        JobVertex iterationHeadVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(1);
        JobVertex deltaVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(2);
        JobVertex iterationTailVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(3);
        JobVertex feedbackVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(4);
        JobVertex sinkVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(5);
        JobVertex iterationSyncVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(6);

        assertThat(sourceMapVertex.getMinResources()).isEqualTo(resource1.merge(resource2));
        assertThat(iterationHeadVertex.getPreferredResources()).isEqualTo(resource3);
        assertThat(deltaVertex.getMinResources()).isEqualTo(resource4);
        // the iteration tail task will be scheduled in the same instance with iteration head, and
        // currently not set resources.
        assertThat(iterationTailVertex.getPreferredResources()).isEqualTo(ResourceSpec.DEFAULT);
        assertThat(feedbackVertex.getMinResources()).isEqualTo(resource5);
        assertThat(sinkVertex.getPreferredResources()).isEqualTo(resource6);
        assertThat(iterationSyncVertex.getMinResources()).isEqualTo(resource3);
    }

    @Test
    void testArtifactCompression() throws IOException {
        Path plainFile1 = Files.createFile(Paths.get(tmp.toString(), "plainFile1"));
        Path plainFile2 = Files.createFile(Paths.get(tmp.toString(), "plainFile2"));

        Path directory1 = Files.createDirectory(Paths.get(tmp.toString(), "directory1"));
        Files.createDirectory(directory1.resolve("containedFile1"));

        Path directory2 = Files.createDirectory(Paths.get(tmp.toString(), "directory2"));
        Files.createDirectory(directory2.resolve("containedFile2"));

        final String executableFileName = "executableFile";
        final String nonExecutableFileName = "nonExecutableFile";
        final String executableDirName = "executableDir";
        final String nonExecutableDirName = "nonExecutableDIr";

        Map<String, DistributedCache.DistributedCacheEntry> originalArtifacts = new HashMap<>();
        originalArtifacts.put(
                executableFileName,
                new DistributedCache.DistributedCacheEntry(plainFile1.toString(), true));
        originalArtifacts.put(
                nonExecutableFileName,
                new DistributedCache.DistributedCacheEntry(plainFile2.toString(), false));
        originalArtifacts.put(
                executableDirName,
                new DistributedCache.DistributedCacheEntry(directory1.toString(), true));
        originalArtifacts.put(
                nonExecutableDirName,
                new DistributedCache.DistributedCacheEntry(directory2.toString(), false));

        final Map<String, DistributedCache.DistributedCacheEntry> submittedArtifacts =
                JobGraphUtils.prepareUserArtifactEntries(originalArtifacts, new JobID());

        DistributedCache.DistributedCacheEntry executableFileEntry =
                submittedArtifacts.get(executableFileName);
        assertState(executableFileEntry, true, false);

        DistributedCache.DistributedCacheEntry nonExecutableFileEntry =
                submittedArtifacts.get(nonExecutableFileName);
        assertState(nonExecutableFileEntry, false, false);

        DistributedCache.DistributedCacheEntry executableDirEntry =
                submittedArtifacts.get(executableDirName);
        assertState(executableDirEntry, true, true);

        DistributedCache.DistributedCacheEntry nonExecutableDirEntry =
                submittedArtifacts.get(nonExecutableDirName);
        assertState(nonExecutableDirEntry, false, true);
    }

    @Test
    void testGeneratedJobsAreBatchJobType() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.fromElements("test").output(new DiscardingOutputFormat<>());

        JobGraph graph = compileJob(env);
        assertThat(graph.getJobType()).isEqualTo(JobType.BATCH);
    }

    @Test
    void testGeneratingJobGraphWithUnconsumedResultPartition() {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Long, Long>> input =
                env.fromElements(new Tuple2<>(1L, 2L)).setParallelism(1);

        DataSet<Tuple2<Long, Long>> ds = input.map(new IdentityMapper<>()).setParallelism(3);

        AbstractID intermediateDataSetID = new AbstractID();

        // this output branch will be excluded.
        ds.output(BlockingShuffleOutputFormat.createOutputFormat(intermediateDataSetID))
                .setParallelism(1);

        // this is the normal output branch.
        ds.output(new DiscardingOutputFormat<>()).setParallelism(1);

        JobGraph jobGraph = compileJob(env);

        assertThat(jobGraph.getVerticesSortedTopologicallyFromSources()).hasSize(3);

        JobVertex mapVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(1);
        assertThat(mapVertex).isInstanceOf(JobVertex.class);

        // there are 2 output result with one of them is ResultPartitionType.BLOCKING_PERSISTENT
        assertThat(mapVertex.getProducedDataSets()).hasSize(2);

        assertThat(
                        mapVertex.getProducedDataSets().stream()
                                .anyMatch(
                                        dataSet ->
                                                dataSet.getId()
                                                                .equals(
                                                                        new IntermediateDataSetID(
                                                                                intermediateDataSetID))
                                                        && dataSet.getResultType()
                                                                == ResultPartitionType
                                                                        .BLOCKING_PERSISTENT))
                .isTrue();
    }

    private static void assertState(
            DistributedCache.DistributedCacheEntry entry, boolean isExecutable, boolean isZipped)
            throws IOException {
        assertThat(entry).isNotNull();
        assertThat(entry.isExecutable).isEqualTo(isExecutable);
        assertThat(entry.isZipped).isEqualTo(isZipped);
        org.apache.flink.core.fs.Path filePath = new org.apache.flink.core.fs.Path(entry.filePath);
        assertThat(filePath.getFileSystem().exists(filePath)).isTrue();
        assertThat(filePath.getFileSystem().getFileStatus(filePath).isDir()).isFalse();
    }

    private static JobGraph compileJob(ExecutionEnvironment env) {
        Plan plan = env.createProgramPlan();
        Optimizer pc = new Optimizer(new Configuration());
        OptimizedPlan op = pc.compile(plan);

        JobGraphGenerator jgg = new JobGraphGenerator();
        return jgg.compileJobGraph(op);
    }
}
