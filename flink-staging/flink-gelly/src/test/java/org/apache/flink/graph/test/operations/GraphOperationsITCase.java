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

package org.apache.flink.graph.test.operations;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.test.TestGraphUtils;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class GraphOperationsITCase extends MultipleProgramsTestBase {

	public GraphOperationsITCase(TestExecutionMode mode){
		super(mode);
	}

    private String expectedResult;

	@Test
	public void testUndirected() throws Exception {
		/*
		 * Test getUndirected()
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);

        DataSet<Edge<Long,Long>> data = graph.getUndirected().getEdges();
        List<Edge<Long, Long>> result= data.collect();
        
		expectedResult = "1,2,12\n" + "2,1,12\n" +
					"1,3,13\n" + "3,1,13\n" +
					"2,3,23\n" + "3,2,23\n" +
					"3,4,34\n" + "4,3,34\n" +
					"3,5,35\n" + "5,3,35\n" +
					"4,5,45\n" + "5,4,45\n" +
					"5,1,51\n" + "1,5,51\n";
		
		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testReverse() throws Exception {
		/*
		 * Test reverse()
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);

        DataSet<Edge<Long,Long>> data = graph.reverse().getEdges();
        List<Edge<Long, Long>> result= data.collect();
        
		expectedResult = "2,1,12\n" +
					"3,1,13\n" +
					"3,2,23\n" +
					"4,3,34\n" +
					"5,3,35\n" +
					"5,4,45\n" +
					"1,5,51\n";
		
		compareResultAsTuples(result, expectedResult);
	}

	@SuppressWarnings("serial")
	@Test
	public void testSubGraph() throws Exception {
		/*
		 * Test subgraph:
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);
		
		DataSet<Edge<Long,Long>> data= graph.subgraph(new FilterFunction<Vertex<Long, Long>>() {
						   public boolean filter(Vertex<Long, Long> vertex) throws Exception {
							   return (vertex.getValue() > 2);
						   }
					   },
				new FilterFunction<Edge<Long, Long>>() {
					public boolean filter(Edge<Long, Long> edge) throws Exception {
						return (edge.getValue() > 34);
					}
				}).getEdges();

        List<Edge<Long, Long>> result= data.collect();
        
		expectedResult = "3,5,35\n" +
					"4,5,45\n";
		
		compareResultAsTuples(result, expectedResult);
	}

	@SuppressWarnings("serial")
	@Test
	public void testFilterVertices() throws Exception {
		/*
		 * Test filterOnVertices:
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);
		
		DataSet<Edge<Long,Long>> data = graph.filterOnVertices(new FilterFunction<Vertex<Long, Long>>() {
			public boolean filter(Vertex<Long, Long> vertex) throws Exception {
				return (vertex.getValue() > 2);
			}
		}).getEdges();

        List<Edge<Long, Long>> result= data.collect();
		
		expectedResult =  "3,4,34\n" +
				"3,5,35\n" +
				"4,5,45\n";
		
		compareResultAsTuples(result, expectedResult);
	}

	@SuppressWarnings("serial")
	@Test
	public void testFilterEdges() throws Exception {
		/*
		 * Test filterOnEdges:
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);
		
		DataSet<Edge<Long,Long>> data = graph.filterOnEdges(new FilterFunction<Edge<Long, Long>>() {
			public boolean filter(Edge<Long, Long> edge) throws Exception {
				return (edge.getValue() > 34);
			}
		}).getEdges();

        List<Edge<Long, Long>> result= data.collect();
        
		expectedResult = "3,5,35\n" +
					"4,5,45\n" +
					"5,1,51\n";
		
		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testNumberOfVertices() throws Exception {
		/*
		 * Test numberOfVertices()
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);
		DataSet<Long> data = env.fromElements(graph.numberOfVertices());

        List<Long> result= data.collect();
        
		expectedResult = "5";
		
		compareResultAsText(result, expectedResult);
	}

	@Test
	public void testNumberOfEdges() throws Exception {
		/*
		 * Test numberOfEdges()
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);
		DataSet<Long> data = env.fromElements(graph.numberOfEdges());

        List<Long> result= data.collect();
        
		expectedResult = "7";
		
		compareResultAsText(result, expectedResult);
	}

	@Test
	public void testVertexIds() throws Exception {
		/*
		 * Test getVertexIds()
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);
		
		DataSet<Long> data = graph.getVertexIds();
        List<Long> result= data.collect();
        
		expectedResult = "1\n2\n3\n4\n5\n";
		
		compareResultAsText(result, expectedResult);
	}

	@Test
	public void testEdgesIds() throws Exception {
		/*
		 * Test getEdgeIds()
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);
		
		DataSet<Tuple2<Long,Long>> data = graph.getEdgeIds();
        List<Tuple2<Long, Long>> result= data.collect();
        
		expectedResult = "1,2\n" + "1,3\n" +
				"2,3\n" + "3,4\n" +
				"3,5\n" + "4,5\n" +
				"5,1\n";
		
		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testUnion() throws Exception {
		/*
		 * Test union()
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);

		List<Vertex<Long, Long>> vertices = new ArrayList<Vertex<Long, Long>>();
		List<Edge<Long, Long>> edges = new ArrayList<Edge<Long, Long>>();

		vertices.add(new Vertex<Long, Long>(6L, 6L));
		edges.add(new Edge<Long, Long>(6L, 1L, 61L));

		graph = graph.union(Graph.fromCollection(vertices, edges, env));

        DataSet<Edge<Long,Long>> data = graph.getEdges();
        List<Edge<Long, Long>> result= data.collect();

		expectedResult = "1,2,12\n" +
					"1,3,13\n" +
					"2,3,23\n" +
					"3,4,34\n" +
					"3,5,35\n" +
					"4,5,45\n" +
					"5,1,51\n" +
					"6,1,61\n";
		
		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testTriplets() throws Exception {
		/*
		 * Test getTriplets()
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);

        DataSet<Triplet<Long,Long,Long>> data = graph.getTriplets();
        List<Triplet<Long,Long,Long>> result= data.collect();

		expectedResult = "1,2,1,2,12\n" + "1,3,1,3,13\n" +
				"2,3,2,3,23\n" + "3,4,3,4,34\n" +
				"3,5,3,5,35\n" + "4,5,4,5,45\n" +
				"5,1,5,1,51\n";
		
		compareResultAsTuples(result, expectedResult);
	}
}