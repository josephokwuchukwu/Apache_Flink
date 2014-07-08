/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.test.iterative.nephele.danglingpagerank;

import eu.stratosphere.api.common.operators.util.UserCodeClassWrapper;
import eu.stratosphere.api.common.typeutils.TypeComparatorFactory;
import eu.stratosphere.api.common.typeutils.TypePairComparatorFactory;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.api.java.record.io.FileOutputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.DistributionPattern;
import eu.stratosphere.runtime.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.pact.runtime.iterative.task.IterationHeadPactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationIntermediatePactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationTailPactTask;
import eu.stratosphere.api.java.typeutils.runtime.record.RecordComparatorFactory;
import eu.stratosphere.api.java.typeutils.runtime.record.RecordPairComparatorFactory;
import eu.stratosphere.api.java.typeutils.runtime.record.RecordSerializerFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.BuildSecondCachedMatchDriver;
import eu.stratosphere.pact.runtime.task.CoGroupDriver;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.task.CollectorMapDriver;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.test.iterative.nephele.JobGraphUtils;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.util.OperatingSystem;

public class CompensatableDanglingPageRank {
	
	private static final TypeSerializerFactory<?> recSerializer = RecordSerializerFactory.get();
	
	@SuppressWarnings("unchecked")
	private static final TypeComparatorFactory<?> fieldZeroComparator = new RecordComparatorFactory(new int[] {0}, new Class[] {LongValue.class}, new boolean[] {true});
	
	private static final TypePairComparatorFactory<?, ?> pairComparatorFactory = new RecordPairComparatorFactory();
	
	
	private static final int NUM_FILE_HANDLES_PER_SORT = 64;
	
	private static final float SORT_SPILL_THRESHOLD = 0.85f;
	
	private static final int ITERATION_ID = 1;
	

//	public static void main(String[] args) throws Exception {
//		String confPath = args.length >= 6 ? confPath = args[5] : PlayConstants.PLAY_DIR + "local-conf";
//		
//		GlobalConfiguration.loadConfiguration(confPath);
//		Configuration conf = GlobalConfiguration.getConfiguration();
//		
//		JobGraph jobGraph = getJobGraph(args);
//		JobGraphUtils.submit(jobGraph, conf);
//	}
		
	public static JobGraph getJobGraph(String[] args) throws Exception {

		int degreeOfParallelism = 2;
		String pageWithRankInputPath = ""; // "file://" + PlayConstants.PLAY_DIR + "test-inputs/danglingpagerank/pageWithRank";
		String adjacencyListInputPath = ""; // "file://" + PlayConstants.PLAY_DIR +
//			"test-inputs/danglingpagerank/adjacencylists";
		String outputPath = OperatingSystem.isWindows() ? "file:/c:/tmp/stratosphere/iterations" : "file:///tmp/stratosphere/iterations";
//		String confPath = PlayConstants.PLAY_DIR + "local-conf";
		int minorConsumer = 25;
		int matchMemory = 50;
		int coGroupSortMemory = 50;
		int numIterations = 25;
		long numVertices = 5;
		long numDanglingVertices = 1;

		String failingWorkers = "1";
		int failingIteration = 2;
		double messageLoss = 0.75;

		if (args.length >= 15) {
			degreeOfParallelism = Integer.parseInt(args[0]);
			pageWithRankInputPath = args[1];
			adjacencyListInputPath = args[2];
			outputPath = args[3];
//			confPath = args[4];
			minorConsumer = Integer.parseInt(args[5]);
			matchMemory = Integer.parseInt(args[6]);
			coGroupSortMemory = Integer.parseInt(args[7]);
			numIterations = Integer.parseInt(args[8]);
			numVertices = Long.parseLong(args[9]);
			numDanglingVertices = Long.parseLong(args[10]);
			failingWorkers = args[11];
			failingIteration = Integer.parseInt(args[12]);
			messageLoss = Double.parseDouble(args[13]);
		}

		int totalMemoryConsumption = 3*minorConsumer + matchMemory + coGroupSortMemory;

		JobGraph jobGraph = new JobGraph("CompensatableDanglingPageRank");
		
		// --------------- the inputs ---------------------

		// page rank input
		JobInputVertex pageWithRankInput = JobGraphUtils.createInput(new ImprovedDanglingPageRankInputFormat(),
			pageWithRankInputPath, "DanglingPageWithRankInput", jobGraph, degreeOfParallelism);
		TaskConfig pageWithRankInputConfig = new TaskConfig(pageWithRankInput.getConfiguration());
		pageWithRankInputConfig.addOutputShipStrategy(ShipStrategyType.PARTITION_HASH);
		pageWithRankInputConfig.setOutputComparator(fieldZeroComparator, 0);
		pageWithRankInputConfig.setOutputSerializer(recSerializer);
		pageWithRankInputConfig.setStubParameter("pageRank.numVertices", String.valueOf(numVertices));

		// edges as adjacency list
		JobInputVertex adjacencyListInput = JobGraphUtils.createInput(new ImprovedAdjacencyListInputFormat(),
			adjacencyListInputPath, "AdjancencyListInput", jobGraph, degreeOfParallelism);
		TaskConfig adjacencyListInputConfig = new TaskConfig(adjacencyListInput.getConfiguration());
		adjacencyListInputConfig.addOutputShipStrategy(ShipStrategyType.PARTITION_HASH);
		adjacencyListInputConfig.setOutputSerializer(recSerializer);
		adjacencyListInputConfig.setOutputComparator(fieldZeroComparator, 0);

		// --------------- the head ---------------------
		JobTaskVertex head = JobGraphUtils.createTask(IterationHeadPactTask.class, "IterationHead", jobGraph,
			degreeOfParallelism);
		TaskConfig headConfig = new TaskConfig(head.getConfiguration());
		headConfig.setIterationId(ITERATION_ID);
		
		// initial input / partial solution
		headConfig.addInputToGroup(0);
		headConfig.setIterationHeadPartialSolutionOrWorksetInputIndex(0);
		headConfig.setInputSerializer(recSerializer, 0);
		headConfig.setInputComparator(fieldZeroComparator, 0);
		headConfig.setInputLocalStrategy(0, LocalStrategy.SORT);
		headConfig.setRelativeMemoryInput(0, (double)minorConsumer/totalMemoryConsumption);
		headConfig.setFilehandlesInput(0, NUM_FILE_HANDLES_PER_SORT);
		headConfig.setSpillingThresholdInput(0, SORT_SPILL_THRESHOLD);
		
		// back channel / iterations
		headConfig.setRelativeBackChannelMemory((double)minorConsumer/totalMemoryConsumption);
		
		// output into iteration
		headConfig.setOutputSerializer(recSerializer);
		headConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
		headConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
		
		// final output
		TaskConfig headFinalOutConfig = new TaskConfig(new Configuration());
		headFinalOutConfig.setOutputSerializer(recSerializer);
		headFinalOutConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
		headConfig.setIterationHeadFinalOutputConfig(headFinalOutConfig);
		
		// the sync
		headConfig.setIterationHeadIndexOfSyncOutput(3);
		headConfig.setNumberOfIterations(numIterations);
		
		// the driver 
		headConfig.setDriver(CollectorMapDriver.class);
		headConfig.setDriverStrategy(DriverStrategy.COLLECTOR_MAP);
		headConfig.setStubWrapper(new UserCodeClassWrapper<CompensatingMap>(CompensatingMap.class));
		headConfig.setStubParameter("pageRank.numVertices", String.valueOf(numVertices));
		headConfig.setStubParameter("compensation.failingWorker", failingWorkers);
		headConfig.setStubParameter("compensation.failingIteration", String.valueOf(failingIteration));
		headConfig.setStubParameter("compensation.messageLoss", String.valueOf(messageLoss));
		headConfig.addIterationAggregator(CompensatableDotProductCoGroup.AGGREGATOR_NAME, new PageRankStatsAggregator());

		// --------------- the join ---------------------
		
		JobTaskVertex intermediate = JobGraphUtils.createTask(IterationIntermediatePactTask.class,
			"IterationIntermediate", jobGraph, degreeOfParallelism);
		TaskConfig intermediateConfig = new TaskConfig(intermediate.getConfiguration());
		intermediateConfig.setIterationId(ITERATION_ID);
//		intermediateConfig.setDriver(RepeatableHashjoinMatchDriverWithCachedBuildside.class);
		intermediateConfig.setDriver(BuildSecondCachedMatchDriver.class);
		intermediateConfig.setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
		intermediateConfig.setRelativeMemoryDriver((double)matchMemory/totalMemoryConsumption);
		intermediateConfig.addInputToGroup(0);
		intermediateConfig.addInputToGroup(1);
		intermediateConfig.setInputSerializer(recSerializer, 0);
		intermediateConfig.setInputSerializer(recSerializer, 1);
		intermediateConfig.setDriverComparator(fieldZeroComparator, 0);
		intermediateConfig.setDriverComparator(fieldZeroComparator, 1);
		intermediateConfig.setDriverPairComparator(pairComparatorFactory);
		
		intermediateConfig.setOutputSerializer(recSerializer);
		intermediateConfig.addOutputShipStrategy(ShipStrategyType.PARTITION_HASH);
		intermediateConfig.setOutputComparator(fieldZeroComparator, 0);
		
		intermediateConfig.setStubWrapper(new UserCodeClassWrapper<CompensatableDotProductMatch>(CompensatableDotProductMatch.class));
		intermediateConfig.setStubParameter("pageRank.numVertices", String.valueOf(numVertices));
		intermediateConfig.setStubParameter("compensation.failingWorker", failingWorkers);
		intermediateConfig.setStubParameter("compensation.failingIteration", String.valueOf(failingIteration));
		intermediateConfig.setStubParameter("compensation.messageLoss", String.valueOf(messageLoss));

		// ---------------- the tail (co group) --------------------
		
		JobTaskVertex tail = JobGraphUtils.createTask(IterationTailPactTask.class, "IterationTail", jobGraph,
			degreeOfParallelism);
		TaskConfig tailConfig = new TaskConfig(tail.getConfiguration());
		tailConfig.setIterationId(ITERATION_ID);
        tailConfig.setIsWorksetUpdate();
		// TODO we need to combine!
		
		// inputs and driver
		tailConfig.setDriver(CoGroupDriver.class);
		tailConfig.setDriverStrategy(DriverStrategy.CO_GROUP);
		tailConfig.addInputToGroup(0);
		tailConfig.addInputToGroup(1);
		tailConfig.setInputSerializer(recSerializer, 0);
		tailConfig.setInputSerializer(recSerializer, 1);
		tailConfig.setDriverComparator(fieldZeroComparator, 0);
		tailConfig.setDriverComparator(fieldZeroComparator, 1);
		tailConfig.setDriverPairComparator(pairComparatorFactory);
		tailConfig.setInputAsynchronouslyMaterialized(0, true);
		tailConfig.setRelativeInputMaterializationMemory(0, (double)minorConsumer/totalMemoryConsumption);
		tailConfig.setInputLocalStrategy(1, LocalStrategy.SORT);
		tailConfig.setInputComparator(fieldZeroComparator, 1);
		tailConfig.setRelativeMemoryInput(1, (double)coGroupSortMemory/totalMemoryConsumption);
		tailConfig.setFilehandlesInput(1, NUM_FILE_HANDLES_PER_SORT);
		tailConfig.setSpillingThresholdInput(1, SORT_SPILL_THRESHOLD);
		
		// output
		tailConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
		tailConfig.setOutputSerializer(recSerializer);
		
		// the stub
		tailConfig.setStubWrapper(new UserCodeClassWrapper<CompensatableDotProductCoGroup>(CompensatableDotProductCoGroup.class));
		tailConfig.setStubParameter("pageRank.numVertices", String.valueOf(numVertices));
		tailConfig.setStubParameter("pageRank.numDanglingVertices", String.valueOf(numDanglingVertices));
		tailConfig.setStubParameter("compensation.failingWorker", failingWorkers);
		tailConfig.setStubParameter("compensation.failingIteration", String.valueOf(failingIteration));
		tailConfig.setStubParameter("compensation.messageLoss", String.valueOf(messageLoss));
		
		// --------------- the output ---------------------

		JobOutputVertex output = JobGraphUtils.createFileOutput(jobGraph, "FinalOutput", degreeOfParallelism);
		TaskConfig outputConfig = new TaskConfig(output.getConfiguration());
		outputConfig.addInputToGroup(0);
		outputConfig.setInputSerializer(recSerializer, 0);
		outputConfig.setStubWrapper(new UserCodeClassWrapper<PageWithRankOutFormat>(PageWithRankOutFormat.class));
		outputConfig.setStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, outputPath);
		
		// --------------- the auxiliaries ---------------------
		
		JobOutputVertex fakeTailOutput = JobGraphUtils.createFakeOutput(jobGraph, "FakeTailOutput",
			degreeOfParallelism);

		JobOutputVertex sync = JobGraphUtils.createSync(jobGraph, degreeOfParallelism);
		TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
		syncConfig.setNumberOfIterations(numIterations);
		syncConfig.addIterationAggregator(CompensatableDotProductCoGroup.AGGREGATOR_NAME, new PageRankStatsAggregator());
		syncConfig.setConvergenceCriterion(CompensatableDotProductCoGroup.AGGREGATOR_NAME, new DiffL1NormConvergenceCriterion());
		syncConfig.setIterationId(ITERATION_ID);
		
		// --------------- the wiring ---------------------

		JobGraphUtils.connect(pageWithRankInput, head, ChannelType.NETWORK, DistributionPattern.BIPARTITE);

		JobGraphUtils.connect(head, intermediate, ChannelType.IN_MEMORY, DistributionPattern.POINTWISE);
		intermediateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);
		
		JobGraphUtils.connect(adjacencyListInput, intermediate, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		
		JobGraphUtils.connect(head, tail, ChannelType.NETWORK, DistributionPattern.POINTWISE);
		JobGraphUtils.connect(intermediate, tail, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		tailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);
		tailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(1, degreeOfParallelism);

		JobGraphUtils.connect(head, output, ChannelType.IN_MEMORY, DistributionPattern.POINTWISE);
		JobGraphUtils.connect(tail, fakeTailOutput, ChannelType.IN_MEMORY, DistributionPattern.POINTWISE);

		JobGraphUtils.connect(head, sync, ChannelType.NETWORK, DistributionPattern.POINTWISE);
		
		fakeTailOutput.setVertexToShareInstancesWith(tail);
		tail.setVertexToShareInstancesWith(head);
		pageWithRankInput.setVertexToShareInstancesWith(head);
		adjacencyListInput.setVertexToShareInstancesWith(head);
		intermediate.setVertexToShareInstancesWith(head);
		output.setVertexToShareInstancesWith(head);
		sync.setVertexToShareInstancesWith(head);

		return jobGraph;
	}
}
