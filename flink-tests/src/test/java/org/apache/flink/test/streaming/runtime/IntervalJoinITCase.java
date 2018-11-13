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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.UnsupportedTimeCharacteristicException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Integration tests for interval joins.
 */
public class IntervalJoinITCase {

	private static List<String> testResults;

	@Before
	public void setup() {
		testResults = new ArrayList<>();
	}

	@Test
	public void testLeftOuterJoin() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		KeyedStream<Tuple2<String, Integer>, String> leftStream = env.fromElements(
			Tuple2.of("key", 0),
			Tuple2.of("key", 1),
			Tuple2.of("key", 2),
			Tuple2.of("key", 3)
		)
			.assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor())
			.keyBy(new Tuple2KeyExtractor());

		KeyedStream<Tuple2<String, Integer>, String> rightStream = env.fromElements(
			Tuple2.of("key", 0),
			Tuple2.of("key", 1),
			Tuple2.of("key", 3)
		)
			.assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor())
			.keyBy(new Tuple2KeyExtractor());

		leftStream.intervalJoin(rightStream)
			.between(Time.milliseconds(0), Time.milliseconds(0))
			.leftOuter()
			.process(new CombineToStringJoinFunction())
			.addSink(new ResultSink());

		env.execute();

		expectInAnyOrder(
			"(key,0):(key,0)",
			"(key,1):(key,1)",
			"(key,2):null",
			"(key,3):(key,3)"
		);
	}

	@Test(expected = FlinkRuntimeException.class)
	public void testLeftOuterJoinCantUseTimestampStrategyRight() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		KeyedStream<Tuple2<String, Integer>, String> leftStream = env.fromElements(
			Tuple2.of("key", 0))
			.assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor())
			.keyBy(new Tuple2KeyExtractor());

		KeyedStream<Tuple2<String, Integer>, String> rightStream = env.fromElements(
			Tuple2.of("key", 0)
		)
			.assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor())
			.keyBy(new Tuple2KeyExtractor());

		leftStream.intervalJoin(rightStream)
			.between(Time.milliseconds(0), Time.milliseconds(0))
			.leftOuter()
			.assignRightTimestamp()
			.process(new CombineToStringJoinFunction()).addSink(new ResultSink());
	}

	@Test
	public void testRightOuterJoin() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		KeyedStream<Tuple2<String, Integer>, String> leftStream = env.fromElements(
			Tuple2.of("key", 0),
			Tuple2.of("key", 1),
			Tuple2.of("key", 3)
		)
			.assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor())
			.keyBy(new Tuple2KeyExtractor());

		KeyedStream<Tuple2<String, Integer>, String> rightStream = env.fromElements(
			Tuple2.of("key", 0),
			Tuple2.of("key", 1),
			Tuple2.of("key", 2),
			Tuple2.of("key", 3)
		)
			.assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor())
			.keyBy(new Tuple2KeyExtractor());

		leftStream.intervalJoin(rightStream)
			.between(Time.milliseconds(0), Time.milliseconds(0))
			.rightOuter()
			.process(new CombineToStringJoinFunction()).addSink(new ResultSink());

		env.execute();

		expectInAnyOrder(
			"(key,0):(key,0)",
			"(key,1):(key,1)",
			"null:(key,2)",
			"(key,3):(key,3)"
		);
	}

	@Test(expected = FlinkRuntimeException.class)
	public void testRightOuterJoinCantUseTimestampStrategyLeft() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		KeyedStream<Tuple2<String, Integer>, String> leftStream = env.fromElements(
			Tuple2.of("key", 0))
			.assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor())
			.keyBy(new Tuple2KeyExtractor());

		KeyedStream<Tuple2<String, Integer>, String> rightStream = env.fromElements(
			Tuple2.of("key", 0)
		)
			.assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor())
			.keyBy(new Tuple2KeyExtractor());

		leftStream.intervalJoin(rightStream)
			.between(Time.milliseconds(0), Time.milliseconds(0))
			.rightOuter()
			.assignLeftTimestamp()
			.process(new CombineToStringJoinFunction()).addSink(new ResultSink());
	}

	@Test
	public void testFullOuterJoin() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		KeyedStream<Tuple2<String, Integer>, String> leftStream = env.fromElements(
			Tuple2.of("key", 0),
			Tuple2.of("key", 1),
			Tuple2.of("key", 3)
		)
			.assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor())
			.keyBy(new Tuple2KeyExtractor());

		KeyedStream<Tuple2<String, Integer>, String> rightStream = env.fromElements(
			Tuple2.of("key", 0),
			Tuple2.of("key", 2),
			Tuple2.of("key", 3)
		)
			.assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor())
			.keyBy(new Tuple2KeyExtractor());

		leftStream.intervalJoin(rightStream)
			.between(Time.milliseconds(0), Time.milliseconds(0))
			.fullOuter()
			.process(new CombineToStringJoinFunction()).addSink(new ResultSink());

		env.execute();

		expectInAnyOrder(
			"(key,0):(key,0)",
			"(key,1):null",
			"null:(key,2)",
			"(key,3):(key,3)"
		);
	}

	@Test(expected = FlinkRuntimeException.class)
	public void testFullOuterJoinCantUseTimestampStrategyLeft() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		KeyedStream<Tuple2<String, Integer>, String> leftStream = env.fromElements(
			Tuple2.of("key", 0))
			.assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor())
			.keyBy(new Tuple2KeyExtractor());

		KeyedStream<Tuple2<String, Integer>, String> rightStream = env.fromElements(
			Tuple2.of("key", 0)
		)
			.assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor())
			.keyBy(new Tuple2KeyExtractor());

		leftStream.intervalJoin(rightStream)
			.between(Time.milliseconds(0), Time.milliseconds(0))
			.fullOuter()
			.assignLeftTimestamp()
			.process(new CombineToStringJoinFunction()).addSink(new ResultSink());
	}

	@Test(expected = FlinkRuntimeException.class)
	public void testFullOuterJoinCantUseTimestampStrategyRight() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		KeyedStream<Tuple2<String, Integer>, String> leftStream = env.fromElements(
			Tuple2.of("key", 0))
			.assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor())
			.keyBy(new Tuple2KeyExtractor());

		KeyedStream<Tuple2<String, Integer>, String> rightStream = env.fromElements(
			Tuple2.of("key", 0)
		)
			.assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor())
			.keyBy(new Tuple2KeyExtractor());

		leftStream.intervalJoin(rightStream)
			.between(Time.milliseconds(0), Time.milliseconds(0))
			.fullOuter()
			.assignRightTimestamp()
			.process(new CombineToStringJoinFunction()).addSink(new ResultSink());
	}

	@Test
	public void testCanJoinOverSameKey() throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		KeyedStream<Tuple2<String, Integer>, String> streamOne = env.fromElements(
			Tuple2.of("key", 0),
			Tuple2.of("key", 1),
			Tuple2.of("key", 2),
			Tuple2.of("key", 3),
			Tuple2.of("key", 4),
			Tuple2.of("key", 5)
		)
			.assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor())
			.keyBy(new Tuple2KeyExtractor());

		KeyedStream<Tuple2<String, Integer>, String> streamTwo = env.fromElements(
			Tuple2.of("key", 0),
			Tuple2.of("key", 1),
			Tuple2.of("key", 2),
			Tuple2.of("key", 3),
			Tuple2.of("key", 4),
			Tuple2.of("key", 5)
		)
			.assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor())
			.keyBy(new Tuple2KeyExtractor());

		streamOne
			.intervalJoin(streamTwo)
			.between(Time.milliseconds(0), Time.milliseconds(0))
			.process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {
				@Override
				public void processElement(Tuple2<String, Integer> left,
					Tuple2<String, Integer> right, Context ctx,
					Collector<String> out) throws Exception {
					out.collect(left + ":" + right);
				}
			}).addSink(new ResultSink());

		env.execute();

		expectInAnyOrder(
			"(key,0):(key,0)",
			"(key,1):(key,1)",
			"(key,2):(key,2)",
			"(key,3):(key,3)",
			"(key,4):(key,4)",
			"(key,5):(key,5)"
		);
	}

	@Test
	public void testJoinsCorrectlyWithMultipleKeys() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		KeyedStream<Tuple2<String, Integer>, String> streamOne = env.fromElements(
			Tuple2.of("key1", 0),
			Tuple2.of("key2", 1),
			Tuple2.of("key1", 2),
			Tuple2.of("key2", 3),
			Tuple2.of("key1", 4),
			Tuple2.of("key2", 5)
		)
			.assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor())
			.keyBy(new Tuple2KeyExtractor());

		KeyedStream<Tuple2<String, Integer>, String> streamTwo = env.fromElements(
			Tuple2.of("key1", 0),
			Tuple2.of("key2", 1),
			Tuple2.of("key1", 2),
			Tuple2.of("key2", 3),
			Tuple2.of("key1", 4),
			Tuple2.of("key2", 5)
		)
			.assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor())
			.keyBy(new Tuple2KeyExtractor());

		streamOne
			.intervalJoin(streamTwo)
			// if it were not keyed then the boundaries [0; 1] would lead to the pairs (1, 1),
			// (1, 2), (2, 2), (2, 3)..., so that this is not happening is what we are testing here
			.between(Time.milliseconds(0), Time.milliseconds(1))
			.process(new CombineToStringJoinFunction())
			.addSink(new ResultSink());

		env.execute();

		expectInAnyOrder(
			"(key1,0):(key1,0)",
			"(key2,1):(key2,1)",
			"(key1,2):(key1,2)",
			"(key2,3):(key2,3)",
			"(key1,4):(key1,4)",
			"(key2,5):(key2,5)"
		);
	}

	@Test
	public void testBoundedUnorderedStreamsStillJoinCorrectly() throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		DataStream<Tuple2<String, Integer>> streamOne = env.addSource(new SourceFunction<Tuple2<String, Integer>>() {
			@Override
			public void run(SourceContext<Tuple2<String, Integer>> ctx) {
				ctx.collectWithTimestamp(Tuple2.of("key", 5), 5L);
				ctx.collectWithTimestamp(Tuple2.of("key", 1), 1L);
				ctx.collectWithTimestamp(Tuple2.of("key", 4), 4L);
				ctx.collectWithTimestamp(Tuple2.of("key", 3), 3L);
				ctx.collectWithTimestamp(Tuple2.of("key", 2), 2L);
				ctx.emitWatermark(new Watermark(5));
				ctx.collectWithTimestamp(Tuple2.of("key", 9), 9L);
				ctx.collectWithTimestamp(Tuple2.of("key", 8), 8L);
				ctx.collectWithTimestamp(Tuple2.of("key", 7), 7L);
				ctx.collectWithTimestamp(Tuple2.of("key", 6), 6L);
			}

			@Override
			public void cancel() {
				// do nothing
			}
		});

		DataStream<Tuple2<String, Integer>> streamTwo = env.addSource(new SourceFunction<Tuple2<String, Integer>>() {
			@Override
			public void run(SourceContext<Tuple2<String, Integer>> ctx) {
				ctx.collectWithTimestamp(Tuple2.of("key", 2), 2L);
				ctx.collectWithTimestamp(Tuple2.of("key", 1), 1L);
				ctx.collectWithTimestamp(Tuple2.of("key", 3), 3L);
				ctx.collectWithTimestamp(Tuple2.of("key", 4), 4L);
				ctx.collectWithTimestamp(Tuple2.of("key", 5), 5L);
				ctx.emitWatermark(new Watermark(5));
				ctx.collectWithTimestamp(Tuple2.of("key", 8), 8L);
				ctx.collectWithTimestamp(Tuple2.of("key", 7), 7L);
				ctx.collectWithTimestamp(Tuple2.of("key", 9), 9L);
				ctx.collectWithTimestamp(Tuple2.of("key", 6), 6L);
			}

			@Override
			public void cancel() {
				// do nothing
			}
		});

		streamOne
			.keyBy(new Tuple2KeyExtractor())
			.intervalJoin(streamTwo.keyBy(new Tuple2KeyExtractor()))
			.between(Time.milliseconds(-1), Time.milliseconds(1))
			.process(new CombineToStringJoinFunction())
			.addSink(new ResultSink());

		env.execute();

		expectInAnyOrder(
			"(key,1):(key,1)",
			"(key,1):(key,2)",

			"(key,2):(key,1)",
			"(key,2):(key,2)",
			"(key,2):(key,3)",

			"(key,3):(key,2)",
			"(key,3):(key,3)",
			"(key,3):(key,4)",

			"(key,4):(key,3)",
			"(key,4):(key,4)",
			"(key,4):(key,5)",

			"(key,5):(key,4)",
			"(key,5):(key,5)",
			"(key,5):(key,6)",

			"(key,6):(key,5)",
			"(key,6):(key,6)",
			"(key,6):(key,7)",

			"(key,7):(key,6)",
			"(key,7):(key,7)",
			"(key,7):(key,8)",

			"(key,8):(key,7)",
			"(key,8):(key,8)",
			"(key,8):(key,9)",

			"(key,9):(key,8)",
			"(key,9):(key,9)"
		);
	}

	@Test(expected = NullPointerException.class)
	public void testFailsWithoutUpperBound() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		DataStream<Tuple2<String, Integer>> streamOne = env.fromElements(Tuple2.of("1", 1));
		DataStream<Tuple2<String, Integer>> streamTwo = env.fromElements(Tuple2.of("1", 1));

		streamOne
			.keyBy(new Tuple2KeyExtractor())
			.intervalJoin(streamTwo.keyBy(new Tuple2KeyExtractor()))
			.between(Time.milliseconds(0), null);
	}

	@Test(expected = NullPointerException.class)
	public void testFailsWithoutLowerBound() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		DataStream<Tuple2<String, Integer>> streamOne = env.fromElements(Tuple2.of("1", 1));
		DataStream<Tuple2<String, Integer>> streamTwo = env.fromElements(Tuple2.of("1", 1));

		streamOne
			.keyBy(new Tuple2KeyExtractor())
			.intervalJoin(streamTwo.keyBy(new Tuple2KeyExtractor()))
			.between(null, Time.milliseconds(1));
	}

	@Test
	public void testBoundsCanBeExclusive() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		DataStream<Tuple2<String, Integer>> streamOne = env.fromElements(
			Tuple2.of("key", 0),
			Tuple2.of("key", 1),
			Tuple2.of("key", 2)
		).assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor());

		DataStream<Tuple2<String, Integer>> streamTwo = env.fromElements(
			Tuple2.of("key", 0),
			Tuple2.of("key", 1),
			Tuple2.of("key", 2)
		).assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor());

		streamOne.keyBy(new Tuple2KeyExtractor())
			.intervalJoin(streamTwo.keyBy(new Tuple2KeyExtractor()))
			.between(Time.milliseconds(0), Time.milliseconds(2))
			.upperBoundExclusive()
			.lowerBoundExclusive()
			.process(new CombineToStringJoinFunction())
			.addSink(new ResultSink());

		env.execute();

		expectInAnyOrder(
			"(key,0):(key,1)",
			"(key,1):(key,2)"
		);
	}

	@Test
	public void testBoundsAreInclusiveByDefault() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		DataStream<Tuple2<String, Integer>> streamOne = env.fromElements(
			Tuple2.of("key", 0),
			Tuple2.of("key", 1),
			Tuple2.of("key", 2)
		).assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor());

		DataStream<Tuple2<String, Integer>> streamTwo = env.fromElements(
			Tuple2.of("key", 0),
			Tuple2.of("key", 1),
			Tuple2.of("key", 2)
		).assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor());

		streamOne.keyBy(new Tuple2KeyExtractor())
			.intervalJoin(streamTwo.keyBy(new Tuple2KeyExtractor()))
			.between(Time.milliseconds(0), Time.milliseconds(2))
			.process(new CombineToStringJoinFunction())
			.addSink(new ResultSink());

		env.execute();

		expectInAnyOrder(
			"(key,0):(key,0)",
			"(key,0):(key,1)",
			"(key,0):(key,2)",

			"(key,1):(key,1)",
			"(key,1):(key,2)",

			"(key,2):(key,2)"
		);
	}

	@Test
	public void testUseLeftTimestamp() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		DataStream<Tuple2<String, Integer>> streamOne = env.fromElements(
			Tuple2.of("key", 0),
			Tuple2.of("key", 1),
			Tuple2.of("key", 2)
		).assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor());

		DataStream<Tuple2<String, Integer>> streamTwo = env.fromElements(
			Tuple2.of("key", 0),
			Tuple2.of("key", 1),
			Tuple2.of("key", 2)
		).assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor());

		streamOne.keyBy(new Tuple2KeyExtractor())
			.intervalJoin(streamTwo.keyBy(new Tuple2KeyExtractor()))
			.between(Time.milliseconds(0), Time.milliseconds(2))
			.assignLeftTimestamp()
			.process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {
				@Override
				public void processElement(Tuple2<String, Integer> left, Tuple2<String, Integer> right, Context ctx, Collector<String> out) throws Exception {
					Assert.assertEquals(ctx.getTimestamp(), ctx.getLeftTimestamp());
				}
			})
			.addSink(new ResultSink());

		env.execute();
	}

	@Test
	public void testUseRightTimestamp() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		DataStream<Tuple2<String, Integer>> streamOne = env.fromElements(
			Tuple2.of("key", 1),
			Tuple2.of("key", 2),
			Tuple2.of("key", 3)
		).assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor());

		DataStream<Tuple2<String, Integer>> streamTwo = env.fromElements(
			Tuple2.of("key", 2),
			Tuple2.of("key", 3),
			Tuple2.of("key", 4)
		).assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor());

		streamOne.keyBy(new Tuple2KeyExtractor())
			.intervalJoin(streamTwo.keyBy(new Tuple2KeyExtractor()))
			.between(Time.milliseconds(-2), Time.milliseconds(0))
			.assignRightTimestamp()
			.process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {
				@Override
				public void processElement(Tuple2<String, Integer> left, Tuple2<String, Integer> right, Context ctx, Collector<String> out) throws Exception {
					Assert.assertEquals(ctx.getTimestamp(), ctx.getRightTimestamp());
				}
			})
			.addSink(new ResultSink());

		env.execute();
	}

	@Test
	public void testUseMaxTimestamp() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		DataStream<Tuple2<String, Integer>> streamOne = env.fromElements(
			Tuple2.of("key", 0),
			Tuple2.of("key", 1),
			Tuple2.of("key", 2)
		).assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor());

		DataStream<Tuple2<String, Integer>> streamTwo = env.fromElements(
			Tuple2.of("key", 0),
			Tuple2.of("key", 1),
			Tuple2.of("key", 2)
		).assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor());

		streamOne.keyBy(new Tuple2KeyExtractor())
			.intervalJoin(streamTwo.keyBy(new Tuple2KeyExtractor()))
			.between(Time.milliseconds(0), Time.milliseconds(2))
			.assignMaxTimestamp()
			.process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {
				@Override
				public void processElement(Tuple2<String, Integer> left, Tuple2<String, Integer> right, Context ctx, Collector<String> out) throws Exception {
					Long expected = Math.max(ctx.getRightTimestamp(), ctx.getLeftTimestamp());
					Assert.assertEquals(ctx.getTimestamp(), expected);
				}
			})
			.addSink(new ResultSink());

		env.execute();
	}

	@Test
	public void testUseMinTimestamp() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		DataStream<Tuple2<String, Integer>> streamOne = env.fromElements(
			Tuple2.of("key", 0),
			Tuple2.of("key", 1),
			Tuple2.of("key", 2)
		).assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor());

		DataStream<Tuple2<String, Integer>> streamTwo = env.fromElements(
			Tuple2.of("key", 0),
			Tuple2.of("key", 1),
			Tuple2.of("key", 2)
		).assignTimestampsAndWatermarks(new AscendingTuple2TimestampExtractor());

		streamOne.keyBy(new Tuple2KeyExtractor())
			.intervalJoin(streamTwo.keyBy(new Tuple2KeyExtractor()))
			.between(Time.milliseconds(0), Time.milliseconds(2))
			.assignMinTimestamp()
			.process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {
				@Override
				public void processElement(Tuple2<String, Integer> left, Tuple2<String, Integer> right, Context ctx, Collector<String> out) throws Exception {
					Long expected = Math.min(ctx.getRightTimestamp(), ctx.getLeftTimestamp());
					Assert.assertEquals(ctx.getTimestamp(), expected);
				}
			})
			.addSink(new ResultSink());

		env.execute();
	}

	@Test(expected = UnsupportedTimeCharacteristicException.class)
	public void testExecutionFailsInProcessingTime() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		env.setParallelism(1);

		DataStream<Tuple2<String, Integer>> streamOne = env.fromElements(Tuple2.of("1", 1));
		DataStream<Tuple2<String, Integer>> streamTwo = env.fromElements(Tuple2.of("1", 1));

		streamOne.keyBy(new Tuple2KeyExtractor())
			.intervalJoin(streamTwo.keyBy(new Tuple2KeyExtractor()))
			.between(Time.milliseconds(0), Time.milliseconds(0))
			.process(new CombineToStringJoinFunction());
	}

	private static void expectInAnyOrder(String... expected) {
		List<String> listExpected = Lists.newArrayList(expected);
		Collections.sort(listExpected);
		Collections.sort(testResults);
		Assert.assertEquals(listExpected, testResults);
	}

	private static class AscendingTuple2TimestampExtractor extends AscendingTimestampExtractor<Tuple2<String, Integer>> {
		@Override
		public long extractAscendingTimestamp(Tuple2<String, Integer> element) {
			return element.f1;
		}
	}

	private static class ResultSink implements SinkFunction<String> {
		@Override
		public void invoke(String value, Context context) throws Exception {
			testResults.add(value);
		}
	}

	private static class CombineToStringJoinFunction extends ProcessJoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String> {
		@Override
		public void processElement(
			Tuple2<String, Integer> left,
			Tuple2<String, Integer> right, Context ctx,
			Collector<String> out
		) throws Exception {
			if (left == null) {
				out.collect("null:" + right);
			} else if (right == null) {
				out.collect(left + ":null");
			} else {
				out.collect(left + ":" + right);
			}
		}
	}

	private static class Tuple2KeyExtractor implements KeySelector<Tuple2<String, Integer>, String> {

		@Override
		public String getKey(Tuple2<String, Integer> value) throws Exception {
			return value.f0;
		}
	}
}
