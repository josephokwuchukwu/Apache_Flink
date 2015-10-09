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

package org.apache.flink.test.iterative;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.test.util.JavaProgramTestBase;

@SuppressWarnings("serial")
public class IterationIncompleteDynamicPathConsumptionITCase extends JavaProgramTestBase {
	
	@Override
	protected void testProgram() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// the test data is constructed such that the merge join zig zag
		// has an early out, leaving elements on the dynamic path input unconsumed
		
		DataSet<Path> edges = env.fromElements(
				new Path(1, 2),
				new Path(1, 4),
				new Path(3, 6),
				new Path(3, 8),
				new Path(1, 10),
				new Path(1, 12),
				new Path(3, 14),
				new Path(3, 16),
				new Path(1, 18),
				new Path(1, 20) );
		
		IterativeDataSet<Path> currentPaths = edges.iterate(10);
		
		DataSet<Path> newPaths = currentPaths
				.join(edges, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where("to").equalTo("from")
					.with(new PathConnector())
				.union(currentPaths).distinct("from", "to");
		
		DataSet<Path> result = currentPaths.closeWith(newPaths);
		
		result.output(new DiscardingOutputFormat<Path>());
		
		env.execute();
	}
	
	private static class PathConnector implements JoinFunction<Path, Path, Path> {
		
		@Override
		public Path join(Path path, Path edge)  {
			return new Path(path.from, edge.to);
		}
	}

	// --------------------------------------------------------------------------------------------
	
	public static class Path {
		
		public long from;
		public long to;
		
		public Path() {}
		
		public Path(long from, long to) {
			this.from = from;
			this.to = to;
		}
		
		@Override
		public String toString() {
			return "(" + from + "," + to + ")";
		}
	}
}
