/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.api;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.runtime.io.api.ChannelSelector;
import eu.stratosphere.runtime.io.channels.ChannelType;
import eu.stratosphere.streaming.api.invokable.StreamComponentInvokable;
import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamcomponent.StreamIterationSink;
import eu.stratosphere.streaming.api.streamcomponent.StreamIterationSource;
import eu.stratosphere.streaming.api.streamcomponent.StreamSink;
import eu.stratosphere.streaming.api.streamcomponent.StreamSource;
import eu.stratosphere.streaming.api.streamcomponent.StreamTask;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.faulttolerance.FaultToleranceType;
import eu.stratosphere.streaming.partitioner.BroadcastPartitioner;
import eu.stratosphere.streaming.partitioner.FieldsPartitioner;
import eu.stratosphere.streaming.partitioner.GlobalPartitioner;
import eu.stratosphere.streaming.partitioner.ShufflePartitioner;

/**
 * Object for building Flink stream processing job graphs
 */
public class JobGraphBuilder {

	private static final Log log = LogFactory.getLog(JobGraphBuilder.class);
	private final JobGraph jobGraph;
	protected Map<String, AbstractJobVertex> components;
	protected Map<String, Integer> numberOfInstances;
	protected Map<String, List<String>> edgeList;
	protected Map<String, List<Class<? extends ChannelSelector<StreamRecord>>>> connectionTypes;
	protected boolean iterationStart;
	protected Stack<String> iterationStartPoints;
	protected String maxParallelismVertexName;
	protected int maxParallelism;
	protected FaultToleranceType faultToleranceType;
	private int batchSize = 1;
	private long batchTimeout = 1000;

	/**
	 * Creates a new JobGraph with the given name
	 * 
	 * @param jobGraphName
	 *            Name of the JobGraph
	 * @param faultToleranceType
	 *            Fault tolerance type
	 */
	public JobGraphBuilder(String jobGraphName,
			FaultToleranceType faultToleranceType) {
		jobGraph = new JobGraph(jobGraphName);
		components = new HashMap<String, AbstractJobVertex>();
		numberOfInstances = new HashMap<String, Integer>();
		edgeList = new HashMap<String, List<String>>();
		connectionTypes = new HashMap<String, List<Class<? extends ChannelSelector<StreamRecord>>>>();
		iterationStartPoints = new Stack<String>();
		maxParallelismVertexName = "";
		maxParallelism = 0;
		if (log.isDebugEnabled()) {
			log.debug("JobGraph created");
		}
		this.faultToleranceType = faultToleranceType;
	}

	public void setDefaultBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public void setBatchTimeout(int timeout) {
		this.batchTimeout = timeout;
	}

	/**
	 * Adds source to the JobGraph with the given parameters
	 * 
	 * @param sourceName
	 *            Name of the component
	 * @param InvokableObject
	 *            User defined operator
	 * @param operatorName
	 *            Operator type
	 * @param serializedFunction
	 *            Serialized udf
	 * @param parallelism
	 *            Number of parallel instances created
	 */
	public void setSource(String sourceName,
			UserSourceInvokable<? extends Tuple> InvokableObject,
			String operatorName, byte[] serializedFunction, int parallelism) {

		final JobInputVertex source = new JobInputVertex(sourceName, jobGraph);

		source.setInvokableClass(StreamSource.class);

		setComponent(sourceName, source, InvokableObject, operatorName,
				serializedFunction, parallelism);

		if (log.isDebugEnabled()) {
			log.debug("SOURCE: " + sourceName);
		}
	}
	
	public void setIterationSource(String sourceName,
			StreamComponentInvokable InvokableObject,
			String operatorName, byte[] serializedFunction, int parallelism) {

		final JobInputVertex source = new JobInputVertex(sourceName, jobGraph);

		source.setInvokableClass(StreamIterationSource.class);

		setComponent(sourceName, source, InvokableObject, operatorName,
				serializedFunction, parallelism);

		if (log.isDebugEnabled()) {
			log.debug("SOURCE: " + sourceName);
		}
	}

	/**
	 * Adds task to the JobGraph with the given parameters
	 * 
	 * @param taskName
	 *            Name of the component
	 * @param TaskInvokableObject
	 *            User defined operator
	 * @param operatorName
	 *            Operator type
	 * @param serializedFunction
	 *            Serialized udf
	 * @param parallelism
	 *            Number of parallel instances created
	 */
	public void setTask(
			String taskName,
			UserTaskInvokable<? extends Tuple, ? extends Tuple> TaskInvokableObject,
			String operatorName, byte[] serializedFunction, int parallelism) {

		final JobTaskVertex task = new JobTaskVertex(taskName, jobGraph);
		task.setInvokableClass(StreamTask.class);
		setComponent(taskName, task, TaskInvokableObject, operatorName,
				serializedFunction, parallelism);

		if (log.isDebugEnabled()) {
			log.debug("TASK: " + taskName);
		}
	}

	/**
	 * Adds sink to the JobGraph with the given parameters
	 * 
	 * @param sinkName
	 *            Name of the component
	 * @param InvokableObject
	 *            User defined operator
	 * @param operatorName
	 *            Operator type
	 * @param serializedFunction
	 *            Serialized udf
	 * @param parallelism
	 *            Number of parallel instances created
	 */
	public void setSink(String sinkName,
			UserSinkInvokable<? extends Tuple> InvokableObject,
			String operatorName, byte[] serializedFunction, int parallelism) {

		final JobOutputVertex sink = new JobOutputVertex(sinkName, jobGraph);
		sink.setInvokableClass(StreamSink.class);
		setComponent(sinkName, sink, InvokableObject, operatorName,
				serializedFunction, parallelism);

		if (log.isDebugEnabled()) {
			log.debug("SINK: " + sinkName);
		}

	}
	
	public void setIterationSink(String sinkName,
			StreamComponentInvokable InvokableObject,
			String operatorName, byte[] serializedFunction, int parallelism) {

		final JobOutputVertex sink = new JobOutputVertex(sinkName, jobGraph);
		sink.setInvokableClass(StreamIterationSink.class);
		setComponent(sinkName, sink, InvokableObject, operatorName,
				serializedFunction, parallelism);

		if (log.isDebugEnabled()) {
			log.debug("SINK: " + sinkName);
		}

	}

	/**
	 * Sets component parameters in the JobGraph
	 * 
	 * @param componentName
	 *            Name of the component
	 * @param component
	 *            The component vertex
	 * @param InvokableObject
	 *            The user defined invokable object
	 * @param operatorName
	 *            Type of the user defined operator
	 * @param serializedFunction
	 *            Serialized operator
	 * @param parallelism
	 *            Number of parallel instances created
	 * @param subtasksPerInstance
	 *            Number of parallel instances on one task manager
	 */
	private void setComponent(String componentName,
			AbstractJobVertex component, Serializable InvokableObject,
			String operatorName, byte[] serializedFunction, int parallelism) {

		component.setNumberOfSubtasks(parallelism);

		if (iterationStart) {
			iterationStartPoints.push(componentName);
			iterationStart = false;
		}

		if (parallelism > maxParallelism) {
			maxParallelism = parallelism;
			maxParallelismVertexName = componentName;
		}

		Configuration config = new TaskConfig(component.getConfiguration())
				.getConfiguration();
		config.setClass("userfunction", InvokableObject.getClass());
		config.setString("componentName", componentName);
		config.setInteger("batchSize", batchSize);
		config.setLong("batchTimeout", batchTimeout);
		config.setInteger("faultToleranceType", faultToleranceType.id);
		config.setBytes("operator", serializedFunction);
		config.setString("operatorName", operatorName);
		addSerializedObject(InvokableObject, config);

		components.put(componentName, component);
		numberOfInstances.put(componentName, parallelism);
	}

	/**
	 * Adds serialized invokable object to the JobVertex configuration
	 * 
	 * @param InvokableObject
	 *            Invokable object to serialize
	 * @param config
	 *            JobVertex configuration to which the serialized invokable will
	 *            be added
	 */
	private void addSerializedObject(Serializable InvokableObject,
			Configuration config) {

		ByteArrayOutputStream baos = null;
		ObjectOutputStream oos = null;
		try {
			baos = new ByteArrayOutputStream();

			oos = new ObjectOutputStream(baos);

			oos.writeObject(InvokableObject);

			config.setBytes("serializedudf", baos.toByteArray());
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Serialization error "
					+ InvokableObject.getClass());
		}

	}

	/**
	 * Sets the number of tuples batched together for higher throughput
	 * 
	 * @param componentName
	 *            Name of the component
	 * @param batchSize
	 *            Number of tuples batched together
	 */
	public void setBatchSize(String componentName, int batchSize) {
		Configuration config = components.get(componentName).getConfiguration();
		config.setInteger("batchSize_"
				+ (components.get(componentName)
						.getNumberOfForwardConnections() - 1), batchSize);
	}

	/**
	 * Sets the number of parallel instances created for the given component.
	 * 
	 * @param componentName
	 *            Name of the component
	 * @param parallelism
	 *            Number of parallel instances created
	 */
	public void setParallelism(String componentName, int parallelism) {
		components.get(componentName).setNumberOfSubtasks(parallelism);
		numberOfInstances.put(componentName, parallelism);

		if (parallelism > maxParallelism) {
			maxParallelism = parallelism;
			maxParallelismVertexName = componentName;
		}
	}

	/**
	 * Connects two components with the given names by broadcast partitioning.
	 * <p>
	 * Broadcast partitioning: All the emitted tuples are replicated to all of
	 * the output instances
	 * 
	 * @param upStreamComponentName
	 *            Name of the upstream component, that will emit the records
	 * @param downStreamComponentName
	 *            Name of the downstream component, that will receive the
	 *            records
	 */
	public void broadcastConnect(String upStreamComponentName,
			String downStreamComponentName) {
		connect(upStreamComponentName, downStreamComponentName,
				BroadcastPartitioner.class);
		log.info("Broadcastconnected: " + upStreamComponentName + " to "
				+ downStreamComponentName);
	}

	/**
	 * Connects two components with the given names by fields partitioning on
	 * the given field.
	 * <p>
	 * Fields partitioning: Tuples are hashed by the given key, and grouped to
	 * outputs accordingly
	 * 
	 * @param upStreamComponentName
	 *            Name of the upstream component, that will emit the records
	 * @param downStreamComponentName
	 *            Name of the downstream component, that will receive the
	 *            records
	 * @param keyPosition
	 *            Position of key in the tuple
	 */
	public void fieldsConnect(String upStreamComponentName,
			String downStreamComponentName, int keyPosition) {

		AbstractJobVertex upStreamComponent = components
				.get(upStreamComponentName);
		AbstractJobVertex downStreamComponent = components
				.get(downStreamComponentName);

		addToEdges(upStreamComponentName, downStreamComponentName,
				FieldsPartitioner.class);

		try {
			upStreamComponent.connectTo(downStreamComponent,
					ChannelType.NETWORK);

			Configuration config = new TaskConfig(
					upStreamComponent.getConfiguration()).getConfiguration();

			config.setClass(
					"partitionerClass_"
							+ (upStreamComponent
									.getNumberOfForwardConnections() - 1),
					FieldsPartitioner.class);

			config.setInteger(
					"partitionerIntParam_"
							+ (upStreamComponent
									.getNumberOfForwardConnections() - 1),
					keyPosition);

			if (log.isDebugEnabled()) {
				log.debug("CONNECTED: FIELD PARTITIONING - "
						+ upStreamComponentName + " -> "
						+ downStreamComponentName + ", KEY: " + keyPosition);
			}
		} catch (JobGraphDefinitionException e) {
			if (log.isErrorEnabled()) {
				log.error("Cannot connect components by field: "
						+ upStreamComponentName + " to "
						+ downStreamComponentName, e);
			}
		}
		log.info("Fieldsconnected " + upStreamComponentName + " to "
				+ downStreamComponentName + " on " + keyPosition);

	}

	private void addToEdges(String upStreamComponentName,
			String downStreamComponentName, Class<?> ctype) {
		if (edgeList.containsKey(upStreamComponentName)) {
			connectionTypes.get(upStreamComponentName).add(
					FieldsPartitioner.class);
			edgeList.get(upStreamComponentName).add(downStreamComponentName);
		} else {
			connectionTypes
					.put(upStreamComponentName,
							new ArrayList<Class<? extends ChannelSelector<StreamRecord>>>());
			connectionTypes.get(upStreamComponentName).add(
					FieldsPartitioner.class);

			edgeList.put(upStreamComponentName, new ArrayList<String>());
			edgeList.get(upStreamComponentName).add(downStreamComponentName);
		}
	}

	/**
	 * Connects two components with the given names by global partitioning.
	 * <p>
	 * Global partitioning: sends all emitted tuples to one output instance
	 * (i.e. the first one)
	 * 
	 * @param upStreamComponentName
	 *            Name of the upstream component, that will emit the tuples
	 * @param downStreamComponentName
	 *            Name of the downstream component, that will receive the tuples
	 */
	public void globalConnect(String upStreamComponentName,
			String downStreamComponentName) {
		connect(upStreamComponentName, downStreamComponentName,
				GlobalPartitioner.class);
		log.info("Globalconnected: " + upStreamComponentName + " to "
				+ downStreamComponentName);

	}

	/**
	 * Connects two components with the given names by shuffle partitioning.
	 * <p>
	 * Shuffle partitioning: sends the output tuples to a randomly selected
	 * channel
	 * 
	 * @param upStreamComponentName
	 *            Name of the upstream component, that will emit the tuples
	 * @param downStreamComponentName
	 *            Name of the downstream component, that will receive the tuples
	 */
	public void shuffleConnect(String upStreamComponentName,
			String downStreamComponentName) {
		connect(upStreamComponentName, downStreamComponentName,
				ShufflePartitioner.class);
		log.info("Shuffleconnected: " + upStreamComponentName + " to "
				+ downStreamComponentName);
	}

	/**
	 * Connects to JobGraph components with the given names, partitioning and
	 * channel type
	 * 
	 * @param upStreamComponentName
	 *            Name of the upstream component, that will emit the tuples
	 * @param downStreamComponentName
	 *            Name of the downstream component, that will receive the tuples
	 * @param PartitionerClass
	 *            Class of the partitioner
	 */
	private void connect(String upStreamComponentName,
			String downStreamComponentName,
			Class<? extends ChannelSelector<StreamRecord>> PartitionerClass) {

		AbstractJobVertex upStreamComponent = components
				.get(upStreamComponentName);
		AbstractJobVertex downStreamComponent = components
				.get(downStreamComponentName);

		addToEdges(upStreamComponentName, downStreamComponentName,
				PartitionerClass);

		try {
			upStreamComponent.connectTo(downStreamComponent,
					ChannelType.NETWORK);
			Configuration config = new TaskConfig(
					upStreamComponent.getConfiguration()).getConfiguration();
			config.setClass(
					"partitionerClass_"
							+ (upStreamComponent
									.getNumberOfForwardConnections() - 1),
					PartitionerClass);
			if (log.isDebugEnabled()) {
				log.debug("CONNECTED: " + PartitionerClass.getSimpleName()
						+ " - " + upStreamComponentName + " -> "
						+ downStreamComponentName);
			}
		} catch (JobGraphDefinitionException e) {
			if (log.isErrorEnabled()) {
				log.error(
						"Cannot connect components with "
								+ PartitionerClass.getSimpleName() + " : "
								+ upStreamComponentName + " -> "
								+ downStreamComponentName, e);
			}
		}
	}

	/**
	 * Sets udf operator from one component to another, used with some sinks.
	 * 
	 * @param from
	 *            from
	 * @param to
	 *            to
	 */
	public void setBytesFrom(String from, String to) {
		Configuration fromConfig = components.get(from).getConfiguration();
		Configuration toConfig = components.get(to).getConfiguration();

		toConfig.setString("operatorName",
				fromConfig.getString("operatorName", null));
		toConfig.setBytes("operator", fromConfig.getBytes("operator", null));

	}

	/**
	 * Sets instance sharing between the given components
	 * 
	 * @param component1
	 *            Share will be called on this component
	 * @param component2
	 *            Share will be called to this component
	 */
	public void setInstanceSharing(String component1, String component2) {
		AbstractJobVertex c1 = components.get(component1);
		AbstractJobVertex c2 = components.get(component2);

		c1.setVertexToShareInstancesWith(c2);
	}

	/**
	 * Sets all components to share with the one with highest parallelism
	 */
	private void setAutomaticInstanceSharing() {

		AbstractJobVertex maxParallelismVertex = components
				.get(maxParallelismVertexName);

		for (String componentName : components.keySet()) {
			if (componentName != maxParallelismVertexName) {
				components.get(componentName).setVertexToShareInstancesWith(
						maxParallelismVertex);
			}
		}

	}

	/**
	 * Writes number of inputs into each JobVertex's config
	 */
	private void setNumberOfJobInputs() {
		for (AbstractJobVertex component : components.values()) {
			component.getConfiguration().setInteger("numberOfInputs",
					component.getNumberOfBackwardConnections());
		}
	}

	/**
	 * Writes the number of outputs and output channels into each JobVertex's
	 * config
	 */
	private void setNumberOfJobOutputs() {
		for (AbstractJobVertex component : components.values()) {
			component.getConfiguration().setInteger("numberOfOutputs",
					component.getNumberOfForwardConnections());
		}
	}

	/**
	 * Sets partitioner parameters which can only be set when the full graph is
	 * built
	 */
	private void setPartitionerParameters() {
		for (String componentName : connectionTypes.keySet()) {
			int i = 0;
			for (Class<?> ctype : connectionTypes.get(componentName)) {
				if (ctype.equals(FieldsPartitioner.class)) {
					Configuration config = components.get(componentName)
							.getConfiguration();
					config.setInteger("numOfOutputs_" + i, numberOfInstances
							.get(edgeList.get(componentName).get(i)));
				}
				i++;
			}
		}
	}

	/**
	 * Returns the JobGraph
	 * 
	 * @return JobGraph object
	 */
	public JobGraph getJobGraph() {
		setAutomaticInstanceSharing();
		setNumberOfJobInputs();
		setNumberOfJobOutputs();
		setPartitionerParameters();
		return jobGraph;
	}

}
