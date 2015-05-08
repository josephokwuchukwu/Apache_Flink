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

package org.apache.flink.graph.library;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.validation.GraphValidator;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.util.Collector;

/**
 * Affinity Propagation Algorithm.
 *
 * Initially, the input graph will be reconstructed to a new graph, in which the vertex value is hash map containing
 * similarity, responsibility and availability to its neighbor vertices. In the original affinity propagation, 
 * the responsibility availabilities if each pair is update in one iteration, here, on of this iteration is finshed by two
 * super steps (vertex centric iteration).
 * In the odd super step, each vertex propagates the availabilities to all its neighborhoods, and 
 * each vertex update the responsibility according to the received availabilities afterwards.
 * In the even step, each vertex propagates the availabilities to all its neighborhoods, and 
 * each vertex update the availability according to the received responsibilities afterwards.
 * The algorithm converges when all the maximum number of iterations is reached.
 *
 * @see <a href="http://www.psi.toronto.edu/index.php?q=affinity%20propagation">for details about affinity propagation algorithm</a>
 */
@SuppressWarnings({ "serial"})
public class AffinityPropogation implements GraphAlgorithm<Long, Long, Double> {

	private int maxIterations;
	private Double lambda;
	public AffinityPropogation(int maxIterations, double lambda){
		this.maxIterations = maxIterations;
		this.lambda = lambda;
	}
	
	/**
	 * Transfer the value of each vertex from Double to HashMap<Long, Tuple3<Double, Double,Double>>;
	 * In the hash map, the key is the ID of the neighbor vertex.  
	 * The content Tuple3<Double, Double, Double> contains the similarity, responsibility and availability to corresponding 
	 * to the neighbor vertex.
	 */
	private Graph<Long, HashMap<Long, Tuple3<Double, Double,Double>>, Double> transferGraph(Graph<Long, Long, Double> originalGraph){
		/*Remove duplicated edges and reconstruct the edge set for a directed graph.*/
		DataSet<Edge<Long, Double>> clearedEdges = originalGraph.getEdges().map(new MapFunction<Edge<Long, Double>, Edge<Long, Double>>(){
			@Override
			public Edge<Long, Double> map(Edge<Long, Double> e)
					throws Exception {
				if (e.f0 > e.f1){
					return e.reverse();
				}
				return e;
			}
			
		}).groupBy(0,1).reduceGroup(new GroupReduceFunction<Edge<Long,Double>,Edge<Long,Double>>(){
			@Override
			public void reduce(Iterable<Edge<Long, Double>> values,
					Collector<Edge<Long, Double>> out) throws Exception {
				for (Edge<Long, Double> e: values){
					out.collect(e);
					if (e.f0 !=e.f1){
						out.collect(e.reverse());
					}
					break;
				}
			}
			
		});
		/*Reconstruct a graph*/
		DataSet<Vertex<Long, Long>> vertices = originalGraph.getVertices();
		/*Transfer the vertex value to a hash map, which stores the <Similarity, Responsibility, Availability> to the neigbor vertices */
		DataSet<Vertex<Long,HashMap<Long, Tuple3<Double, Double,Double>>>> newVertices = vertices.map(
				new MapFunction<Vertex<Long, Long>,Vertex<Long,HashMap<Long, Tuple3<Double, Double,Double>>>>(){
					@Override
					public Vertex<Long, HashMap<Long, Tuple3<Double, Double, Double>>> map(
							Vertex<Long, Long> v) throws Exception {
						// TODO Auto-generated method stub
						HashMap<Long, Tuple3<Double, Double, Double>> hashmap = 
								new HashMap<Long, Tuple3<Double, Double,Double>>();
						hashmap.put(v.getId(),new Tuple3<Double, Double, Double>(0.0, 0.0, 0.0));
						return new Vertex<Long, HashMap<Long, Tuple3<Double, Double, Double>>>(v.getId(),
								hashmap);
					}
				}); 
		
		/*Construct a new graph with HashMap<key, <Similarity, Responsibility, Availability>> value for each vertex*/
		Graph<Long, HashMap<Long, Tuple3<Double, Double, Double>>, Double> newGraph = 
				Graph.fromDataSet(newVertices, clearedEdges, ExecutionEnvironment.getExecutionEnvironment());

		
		/*run iteration once to get similarity */
		Graph<Long, HashMap<Long, Tuple3<Double, Double,Double>>, Double> hashMappedGraph = 
				newGraph.runVertexCentricIteration(new InitialVertexHashmap(), new InitialMessenger(), 1);
			
		return hashMappedGraph;
	}
	
	public static final class InitialVertexHashmap 
	extends VertexUpdateFunction<Long, HashMap<Long, Tuple3<Double, Double,Double>>, Tuple2<Long, Double>>{
		
		@Override
		public void updateVertex(Long vertexKey, HashMap<Long, Tuple3<Double, Double,Double>> vertexValue,
				MessageIterator<Tuple2<Long, Double>> inMessages)
				throws Exception {
			for (Tuple2<Long, Double> message: inMessages){
				/*Remove degeneracies*/
				Double newS = message.f1 + (1e-12) * 66.2 * Math.random();
				if (!vertexValue.containsValue(message.f0)){
					vertexValue.put(message.f0, new Tuple3<Double, Double,Double>(newS, 0.0, 0.0));					
				}else{
					vertexValue.get(message.f0).f0 = newS;
				}
			}
			setNewVertexValue(vertexValue);
		}
	}
	
	
	public static final class InitialMessenger
		extends MessagingFunction<Long, HashMap<Long, Tuple3<Double, Double,Double>>, Tuple2<Long, Double>, Double>{
		@Override
		public void sendMessages(Long vertexKey, HashMap<Long, Tuple3<Double, Double,Double>> vertexValue)
				throws Exception {
			for (Edge<Long, Double> e: getOutgoingEdges()){
				sendMessageTo(e.getTarget(), new Tuple2<Long, Double>(vertexKey, e.getValue()));
			}
		}
	}

	
	/**
	 * Validate there is one self-edge for  each vertex
	 */
	public static final class SelfEdgeValidator extends GraphValidator<Long, Long, Double>{
		@Override
		public boolean validate(Graph<Long, Long, Double> graph) throws Exception{
			/*Check the number of self-edges*/
			DataSet<Edge<Long, Double>> selfEdges = graph.getEdges().filter(new FilterFunction<Edge<Long, Double>>(){
				@Override
				public boolean filter(Edge<Long, Double> value){
					return value.f0 == value.f1;
				}
			});
			if (selfEdges.count() != graph.getVertices().count()){
				return false;
			}
			/*Check duplicated self-edges*/
			DataSet<Edge<Long, Double>> duplicatedEdges = selfEdges.groupBy(0).reduceGroup(new GroupReduceFunction<Edge<Long, Double>,
					Edge<Long, Double>>(){
						@Override
						public void reduce(Iterable<Edge<Long, Double>> values,
								Collector<Edge<Long, Double>> out)
								throws Exception {
							int cnt = 0;
							for (Edge<Long, Double> e:values){
								cnt++;
								if (cnt > 1){
									out.collect(e);
								}
							}
						}
			});
			if (duplicatedEdges.count() > 0){
				return false;
			}
			return true;
		}
	}
	
	@Override
	public Graph<Long, Long, Double> run(Graph<Long, Long, Double> input) throws Exception{
		
		//Check the validity of the input graph
		if (!input.validate(new SelfEdgeValidator())){
			throw new Exception("Self-edges checking error");
		}
			
		Graph<Long, HashMap<Long, Tuple3<Double, Double,Double>>, Double> graph = transferGraph(input);
		
		//Run the vertex centric iteration
		Graph<Long, HashMap<Long, Tuple3<Double, Double, Double>>, Double> stableGraph = graph.runVertexCentricIteration(
				new VertexUpdater(lambda), new InformationMessenger(), maxIterations*2);
		
		//Find the centers of each vertices
		DataSet<Vertex<Long, Long>> resultSet = stableGraph.groupReduceOnNeighbors(new ExamplarSelection(), EdgeDirection.OUT);
		
		//Reconstruct the graph to the original format
		Graph<Long, Long, Double> resultGraph = Graph.fromDataSet(resultSet, 
				input.getEdges(), ExecutionEnvironment.getExecutionEnvironment());
		return resultGraph;
	}
	
	public static final class ExamplarSelection implements 
	NeighborsFunctionWithVertexValue<Long, HashMap<Long, Tuple3<Double, Double, Double>>, 
	Double, Vertex<Long, Long>>{

		@Override
		public void iterateNeighbors(
				Vertex<Long, HashMap<Long, Tuple3<Double, Double, Double>>> vertex,
				Iterable<Tuple2<Edge<Long, Double>, Vertex<Long, HashMap<Long, Tuple3<Double, Double, Double>>>>> neighbors,
				Collector<Vertex<Long, Long>> out) throws Exception {
			/*Get Evidence*/
			HashMap<Long, Tuple3<Double, Double, Double>> hmap = vertex.getValue();
			double selfEvidence = hmap.get(vertex.getId()).f1 + hmap.get(vertex.getId()).f2;
			if (selfEvidence > 0){
				out.collect(new Vertex<Long, Long>(vertex.getId(), vertex.getId()));
				return;
			}else{
				Double maxSimilarity = Double.NEGATIVE_INFINITY;
				Long belongExemplar = vertex.getId();
				for (Tuple2<Edge<Long, Double>, Vertex<Long, HashMap<Long, Tuple3<Double, Double, Double>>>> neigbor: neighbors){
					Long neigborId = neigbor.f1.getId();
					HashMap<Long, Tuple3<Double, Double, Double>> neigborMap = neigbor.f1.getValue();
					double neigborEvidence = neigborMap.get(neigborId).f1 + neigborMap.get(neigborId).f2;
					//Only the neighbor vertex with positive evidence can be possible exemplar of current vertex
					if (neigborEvidence > 0 ){
						Double neigborSimilarity = neigbor.f0.getValue();
						if (neigborSimilarity > maxSimilarity){
							belongExemplar = neigborId;
							maxSimilarity = neigborSimilarity;
						}
					}
				}
				out.collect(new Vertex<Long, Long>(vertex.getId(), belongExemplar));
				return;
			}
		}

		
	}

	/*Update r(i,k) for each vertex*/
	public static final class VertexUpdater 
		extends VertexUpdateFunction<Long, HashMap<Long, Tuple3<Double, Double,Double>>, Tuple2<Long, Double>>{
		private Double lambda;
		
		public VertexUpdater(Double lambda){
			this.lambda = lambda;
		}
		
		@Override
		public void updateVertex(Long vertexKey,
				HashMap<Long, Tuple3<Double, Double, Double>> vertexValue,
				MessageIterator<Tuple2<Long, Double>> inMessages)
				throws Exception {
			int step = getSuperstepNumber();
			/*odd step: receive neighbor responsibility and update the responsibility*/
			if (step % 2 == 1){ 
				double selfSum = vertexValue.get(vertexKey).f0 + vertexValue.get(vertexKey).f2;
				/*The max a(v, k) + s(v, k)*/
				double maxSum = selfSum;
				long maxKey = vertexKey;
				/*The second max a(v, k) + s(v, k)*/
				double secondMaxSum = Double.NEGATIVE_INFINITY;
				for (Tuple2<Long, Double> msg: inMessages){
					Long adjacentVertex = msg.f0;
					Double sum = msg.f1 + vertexValue.get(adjacentVertex).f0;
					if (sum > maxSum ){
						secondMaxSum = maxSum;
						maxSum = sum;
						maxKey = adjacentVertex;
					}else if(sum > secondMaxSum){
						secondMaxSum = sum;
					}
				}
				if (maxKey != vertexKey && selfSum > secondMaxSum){
					secondMaxSum = selfSum;
				}
				/*Update responsibility*/
				for (Entry<Long, Tuple3<Double, Double, Double>> entry: vertexValue.entrySet()){
					Double newRespons = 0.0;
					if (entry.getKey() != maxKey){
						newRespons = entry.getValue().f0 - maxSum;
					}else{
						newRespons = entry.getValue().f0 - secondMaxSum;
					}
					entry.getValue().f1 = (1 - lambda) * newRespons + lambda * entry.getValue().f1;
				}
				/*reset the hashmap of the vertex*/
				setNewVertexValue(vertexValue);
				
			}else{
				/*Odd step: receive responsibility and update availability */
				double sum = 0.0;
				/*msg.f1 is the received responsibility*/
				ArrayList<Tuple2<Long, Double>> allMessages = new ArrayList<Tuple2<Long, Double>>();
				for (Tuple2<Long, Double> msg: inMessages){
					Double posValue = msg.f1 > 0 ? msg.f1:0;
					sum += posValue;
					allMessages.add(new Tuple2<Long, Double>(msg.f0, posValue));
				}
				
				Double selfRespons = vertexValue.get(vertexKey).f1;
		
				for (Tuple2<Long, Double> msg: allMessages){
					Double gathered = selfRespons + sum - msg.f1;
					Double newAvailability = gathered < 0 ? gathered: 0;
					vertexValue.get(msg.f0).f2 = (1 - lambda) * newAvailability + lambda * vertexValue.get(msg.f0).f2;
				}
				/*update self-availability*/
				vertexValue.get(vertexKey).f2 = (1 - lambda) * sum  + lambda *vertexValue.get(vertexKey).f2;
				/*reset the hash map*/
				setNewVertexValue(vertexValue);
			}
			
		}
	}
	
	/*Send a(k',i) to k'*/
	public static final class InformationMessenger
	extends MessagingFunction<Long, HashMap<Long, Tuple3<Double, Double,Double>>, Tuple2<Long, Double>, Double>{
		@Override
		public void sendMessages(Long vertexKey,
				HashMap<Long, Tuple3<Double, Double, Double>> vertexValue)
				throws Exception {
			if (getSuperstepNumber() % 2 == 1){
				/*Odd step: Propagate availability*/
				for (Edge<Long, Double> e: getOutgoingEdges()){
					Long dest = e.getTarget();
					if (dest != vertexKey){
						sendMessageTo(dest, new Tuple2<Long, Double>(vertexKey, vertexValue.get(dest).f2));
					}
				}
			}else{
				/*Even step: propagate responsibility*/
				for (Edge<Long, Double> e: getOutgoingEdges()){
					long dest = e.getTarget();
					if (dest != vertexKey){
						sendMessageTo(dest, new Tuple2<Long, Double>(vertexKey, vertexValue.get(dest).f1));
					}
				}
			}
				
		}
	}
	
}
