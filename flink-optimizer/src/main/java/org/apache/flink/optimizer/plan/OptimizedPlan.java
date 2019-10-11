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

package org.apache.flink.optimizer.plan;

import java.util.Collection;

import org.apache.flink.api.common.Plan;
import org.apache.flink.util.Visitable;
import org.apache.flink.util.Visitor;

/**
 * The execution plan generated by the Optimizer. It contains {@link PlanNode}s
 * and {@link Channel}s that describe exactly how the program should be executed.
 * 
 * <p>The optimized plan defines all ship strategies (local pipe, shuffle, broadcast, rebalance),
 * all operator strategies (sorting-merge join, hash join, sorted grouping, ...),
 * and the data exchange modes (batched, pipelined).</p>
 */
public class OptimizedPlan implements Visitable<PlanNode>  {
	
	/** The data sources in the plan. */
	private final Collection<SourcePlanNode> dataSources;

	/** The data sinks in the plan. */
	private final Collection<SinkPlanNode> dataSinks;

	/** All nodes in the optimizer plan. */
	private final Collection<PlanNode> allNodes;
	
	/** The original program (as a dataflow plan). */
	private final Plan originalProgram;

	/** Name of the job */
	private final String jobName;

	/**
	 * Creates a new instance of this optimizer plan container. The plan is given and fully
	 * described by the data sources, sinks and the collection of all nodes.
	 * 
	 * @param sources The data sources.
	 * @param sinks The data sinks.
	 * @param allNodes A collection containing all nodes in the plan.
	 * @param jobName The name of the program
	 */
	public OptimizedPlan(Collection<SourcePlanNode> sources, Collection<SinkPlanNode> sinks,
			Collection<PlanNode> allNodes, String jobName, Plan programPlan)
	{
		this.dataSources = sources;
		this.dataSinks = sinks;
		this.allNodes = allNodes;
		this.jobName = jobName;
		this.originalProgram = programPlan;
	}

	/**
	 * Gets the data sources from this OptimizedPlan.
	 * 
	 * @return The data sources.
	 */
	public Collection<SourcePlanNode> getDataSources() {
		return dataSources;
	}

	/**
	 * Gets the data sinks from this OptimizedPlan.
	 * 
	 * @return The data sinks.
	 */
	public Collection<SinkPlanNode> getDataSinks() {
		return dataSinks;
	}

	/**
	 * Gets all the nodes from this OptimizedPlan.
	 * 
	 * @return All nodes.
	 */
	public Collection<PlanNode> getAllNodes() {
		return allNodes;
	}

	/**
	 * Returns the name of the program.
	 * 
	 * @return The name of the program.
	 */
	public String getJobName() {
		return this.jobName;
	}
	
	/**
	 * Gets the original program's dataflow plan from which this optimized plan was created.
	 * 
	 * @return The original program's dataflow plan.
	 */
	public Plan getOriginalPlan() {
		return this.originalProgram;
	}

	// ------------------------------------------------------------------------

	/**
	 * Applies the given visitor top down to all nodes, starting at the sinks.
	 * 
	 * @param visitor
	 *        The visitor to apply to the nodes in this plan.
	 * @see org.apache.flink.util.Visitable#accept(org.apache.flink.util.Visitor)
	 */
	@Override
	public void accept(Visitor<PlanNode> visitor) {
		for (SinkPlanNode node : this.dataSinks) {
			node.accept(visitor);
		}
	}
}
