/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.windowing;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateIdentifier;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.examples.java.wordcount.util.WordCountData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;
import org.apache.flink.streaming.examples.wordcount.WordCount;

/**
 * Implements a windowed version of the streaming "WordCount" program.
 *
 * <p>
 * The input is a plain text file with lines separated by newline characters.
 * 
 * <p>
 * Usage: <code>WordCount &lt;text path&gt; &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link org.apache.flink.examples.java.wordcount.util.WordCountData}.
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink Streaming program,
 * <li>use tuple data types,
 * <li>use basic windowing abstractions.
 * </ul>
 *
 */
public class WindowWordCount {

	// window parameters with default values
	private static int windowSize = 250;
	private static int slideSize = 150;

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataStream<String> text1 = env.socketTextStream("localhost", 9999)
				.keyBy(new KeySelector<String, String>() {
					@Override
					public String getKey(String value) throws Exception {
						return value;
					}
				});
		DataStream<String> text2 = env.socketTextStream("localhost", 9998)
				.keyBy(new KeySelector<String, String>() {
					@Override
					public String getKey(String value) throws Exception {
						return value;
					}
				});

		text1.connect(text2)
				.map(new RichCoMapFunction<String, String, String>() {
					private static final long serialVersionUID = 1L;

					ValueStateIdentifier<Long> stateId = new ValueStateIdentifier<>("count", 0L, LongSerializer.INSTANCE);

					@Override
					public String map1(String value) throws Exception {
						ValueState<Long> count = getRuntimeContext().getPartitionedState(stateId);

						count.update(count.value() + 1);

						System.out.println("IN 1, COUNT IS: " + count.value());
						return value;
					}

					@Override
					public String map2(String value) throws Exception {
						ValueState<Long> count = getRuntimeContext().getPartitionedState(stateId);

						count.update(count.value() + 1);

						System.out.println("IN 2, COUNT IS: " + count.value());
						return value;
					}
				})
				.print();


		// execute program
		env.execute("WindowWordCount");
	}


	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String textPath;
	private static String outputPath;

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			// parse input arguments
			fileOutput = true;
			if (args.length >= 2 && args.length <= 4) {
				textPath = args[0];
				outputPath = args[1];
				if (args.length >= 3){
					windowSize = Integer.parseInt(args[2]);

					// if no slide size is specified use the
					slideSize = args.length == 3 ? windowSize : Integer.parseInt(args[2]);
				}
			} else {
				System.err.println("Usage: WindowWordCount <text path> <result path> [<window size>] [<slide size>]");
				return false;
			}
		} else {
			System.out.println("Executing WindowWordCount example with built-in default data.");
			System.out.println("  Provide parameters to read input data from a file.");
			System.out.println("  Usage: WindowWordCount <text path> <result path> [<window size>] [<slide size>]");
		}
		return true;
	}

	private static DataStream<String> getTextDataStream(StreamExecutionEnvironment env) {
		if (fileOutput) {
			// read the text file from given input path
			return env.readTextFile(textPath);
		} else {
			// get default test text data
			return env.fromElements(WordCountData.WORDS);
		}
	}
}
