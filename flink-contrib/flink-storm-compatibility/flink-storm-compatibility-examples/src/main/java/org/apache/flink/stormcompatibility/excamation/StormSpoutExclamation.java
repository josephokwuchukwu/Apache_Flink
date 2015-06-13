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

package org.apache.flink.stormcompatibility.excamation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.examples.java.wordcount.util.WordCountData;
import org.apache.flink.stormcompatibility.util.StormFileSpout;
import org.apache.flink.stormcompatibility.util.StormInMemorySpout;
import org.apache.flink.stormcompatibility.wrappers.StormFiniteSpoutWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StormSpoutExclamation {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(final String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// get input data
		final DataStream<String> text = getTextDataStream(env);

		final DataStream<String> exclaimed = text
				.map(new ExclamationMap())
				.map(new ExclamationMap());

		// emit result
		if (fileOutput) {
			exclaimed.writeAsText(outputPath);
		} else {
			exclaimed.print();
		}

		// execute program
		env.execute("Streaming Exclamation with Storm spout source");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	private static class ExclamationMap implements MapFunction<String, String> {

		@Override
		public String map(String value) throws Exception {
			return value + "!!!";
		}
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String textPath;
	private static String outputPath;

	private static boolean parseParameters(final String[] args) {

		if (args.length > 0) {
			// parse input arguments
			fileOutput = true;
			if (args.length == 2) {
				textPath = args[0];
				outputPath = args[1];
			} else {
				System.err.println("Usage: StormSpoutExclamation <text path> <result path>");
				return false;
			}
		} else {
			System.out.println("Executing StormSpoutExclamation example with built-in default data");
			System.out.println("  Provide parameters to read input data from a file");
			System.out.println("  Usage: StormSpoutExclamation <text path> <result path>");
		}
		return true;
	}

	private static DataStream<String> getTextDataStream(final StreamExecutionEnvironment env) {
		if (fileOutput) {
			// read the text file from given input path
			final String[] tokens = textPath.split(":");
			final String localFile = tokens[tokens.length - 1];
			return env.addSource(
					new StormFiniteSpoutWrapper<String>(new StormFileSpout(localFile), true),
					TypeExtractor.getForClass(String.class)).setParallelism(1);
		}

		return env.addSource(new StormFiniteSpoutWrapper<String>(new StormInMemorySpout(WordCountData.WORDS), true),
				TypeExtractor.getForClass(String.class));

	}

}
