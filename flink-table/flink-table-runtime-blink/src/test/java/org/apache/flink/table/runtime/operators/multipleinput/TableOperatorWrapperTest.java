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

package org.apache.flink.table.runtime.operators.multipleinput;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.MockOutput;
import org.apache.flink.streaming.util.MockStreamConfig;
import org.apache.flink.streaming.util.MockStreamTaskBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.multipleinput.TableOperatorWrapper.Edge;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link TableOperatorWrapper}.
 */
public class TableOperatorWrapperTest extends TableOperatorWrapperTestBase {

	@Test
	public void testBasicInfo() {
		TestOneInputStreamOperator inOperator1 = new TestOneInputStreamOperator();
		TestOneInputStreamOperator inOperator2 = new TestOneInputStreamOperator();
		TestTwoInputStreamOperator outOperator = new TestTwoInputStreamOperator();
		TableOperatorWrapper<TestOneInputStreamOperator> wrapper1 =
				createOneInputOperatorWrapper(inOperator1, "test1");

		TableOperatorWrapper<TestOneInputStreamOperator> wrapper2 =
				createOneInputOperatorWrapper(inOperator2, "test2");

		TableOperatorWrapper<TestTwoInputStreamOperator> wrapper3 =
				createTwoInputOperatorWrapper(outOperator, "test3");
		wrapper3.addInput(wrapper1, 1);
		wrapper3.addInput(wrapper2, 2);

		assertTrue(wrapper1.getInputEdges().isEmpty());
		assertTrue(wrapper1.getInputWrappers().isEmpty());
		assertWrapperEquals(Collections.singletonList(wrapper3), wrapper1.getOutputWrappers());
		assertEdgeEquals(
				Collections.singletonList(new Edge(wrapper1, wrapper3, 1)),
				wrapper1.getOutputEdges());

		assertTrue(wrapper2.getInputEdges().isEmpty());
		assertTrue(wrapper2.getInputWrappers().isEmpty());
		assertWrapperEquals(Collections.singletonList(wrapper3), wrapper2.getOutputWrappers());
		assertEdgeEquals(Collections.singletonList(
				new Edge(wrapper2, wrapper3, 2)),
				wrapper2.getOutputEdges());

		assertTrue(wrapper3.getOutputEdges().isEmpty());
		assertTrue(wrapper3.getOutputWrappers().isEmpty());
		assertWrapperEquals(Arrays.asList(wrapper1, wrapper2), wrapper3.getInputWrappers());
		assertEdgeEquals(Arrays.asList(
				new Edge(wrapper1, wrapper3, 1),
				new Edge(wrapper2, wrapper3, 2)),
				wrapper3.getInputEdges());
	}

	@Test
	public void testCreateOperator() throws Exception {
		TestOneInputStreamOperator operator = new TestOneInputStreamOperator();
		TableOperatorWrapper<TestOneInputStreamOperator> wrapper =
				createOneInputOperatorWrapper(operator, "test");
		StreamOperatorParameters<RowData> parameters = createStreamOperatorParameters();
		wrapper.createOperator(parameters);

		assertEquals(operator, wrapper.getStreamOperator());

		// create operator again, will throw exception
		try {
			wrapper.createOperator(parameters);
			fail("This should not happen");
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("This operator has been initialized"));
		}
	}

	@Test
	public void testEndInput() throws Exception {
		StreamOperatorParameters<RowData> parameters = createStreamOperatorParameters();
		TestOneInputStreamOperator inOperator1 = new TestOneInputStreamOperator();
		TestOneInputStreamOperator inOperator2 = new TestOneInputStreamOperator();
		TestTwoInputStreamOperator outOperator = new TestTwoInputStreamOperator();
		TableOperatorWrapper<TestOneInputStreamOperator> wrapper1 =
				createOneInputOperatorWrapper(inOperator1, "test1");
		wrapper1.createOperator(parameters);

		TableOperatorWrapper<TestOneInputStreamOperator> wrapper2 =
				createOneInputOperatorWrapper(inOperator2, "test2");
		wrapper2.createOperator(parameters);

		TableOperatorWrapper<TestTwoInputStreamOperator> wrapper3 =
				createTwoInputOperatorWrapper(outOperator, "test3");
		wrapper3.addInput(wrapper1, 1);
		wrapper3.addInput(wrapper2, 2);
		wrapper3.createOperator(parameters);

		// initialized status
		assertFalse(inOperator1.isEnd());
		assertFalse(inOperator2.isEnd());
		assertTrue(outOperator.getEndInputs().isEmpty());

		// end first input
		wrapper1.endOperatorInput(1);
		assertTrue(inOperator1.isEnd());
		assertEquals(1, wrapper1.getEndedInputCount());
		assertFalse(inOperator2.isEnd());
		assertEquals(0, wrapper2.getEndedInputCount());
		assertEquals(Collections.singletonList(1), outOperator.getEndInputs());
		assertEquals(1, wrapper3.getEndedInputCount());

		// end second input
		wrapper2.endOperatorInput(1);
		assertTrue(inOperator1.isEnd());
		assertEquals(1, wrapper1.getEndedInputCount());
		assertTrue(inOperator2.isEnd());
		assertEquals(1, wrapper2.getEndedInputCount());
		assertEquals(Arrays.asList(1, 2), outOperator.getEndInputs());
		assertEquals(2, wrapper3.getEndedInputCount());
	}

	@Test
	public void testClose() throws Exception {
		TestOneInputStreamOperator operator = new TestOneInputStreamOperator();
		TableOperatorWrapper<TestOneInputStreamOperator> wrapper =
				createOneInputOperatorWrapper(operator, "test");
		StreamOperatorParameters<RowData> parameters = createStreamOperatorParameters();
		wrapper.createOperator(parameters);
		assertEquals(operator, wrapper.getStreamOperator());

		assertFalse(operator.isClosed());
		assertFalse(wrapper.isClosed());
		wrapper.close();
		assertTrue(wrapper.isClosed());
		assertTrue(operator.isClosed());

		// close again
		wrapper.close();
		assertTrue(wrapper.isClosed());
		assertTrue(operator.isClosed());
	}

	private StreamOperatorParameters<RowData> createStreamOperatorParameters() throws Exception {
		Environment env = new MockEnvironmentBuilder().build();
		StreamTask task = new MockStreamTaskBuilder(env).build();
		return new StreamOperatorParameters<>(
				task,
				new MockStreamConfig(new Configuration(), 1),
				new MockOutput<>(new ArrayList<>()),
				TestProcessingTimeService::new,
				null
		);
	}

	private TableOperatorWrapper<TestOneInputStreamOperator> createOneInputOperatorWrapper(
			TestOneInputStreamOperator operator, String name) {
		return new TableOperatorWrapper<>(
				SimpleOperatorFactory.of(operator),
				name,
				Collections.singletonList(new RowTypeInfo(Types.STRING)),
				new RowTypeInfo(Types.STRING)
		);
	}

	private TableOperatorWrapper<TestTwoInputStreamOperator> createTwoInputOperatorWrapper(
			TestTwoInputStreamOperator operator, String name) {
		return new TableOperatorWrapper<>(
				SimpleOperatorFactory.of(operator),
				name,
				Arrays.asList(new RowTypeInfo(Types.STRING), new RowTypeInfo(Types.STRING)),
				new RowTypeInfo(Types.STRING, Types.STRING)
		);
	}

}
