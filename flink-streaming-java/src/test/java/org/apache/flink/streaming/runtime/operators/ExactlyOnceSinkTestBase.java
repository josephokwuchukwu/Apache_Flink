/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.junit.Test;

import java.util.ArrayList;

public abstract class ExactlyOnceSinkTestBase<IN extends Tuple, S extends GenericExactlyOnceSink<IN>> {

	protected class OperatorExposingTask<IN> extends OneInputStreamTask<IN, IN> {
		public OneInputStreamOperator<IN, IN> getOperator() {
			return this.headOperator;
		}
	}

	protected OperatorExposingTask<IN> createTask() {
		return new OperatorExposingTask<>();
	}

	protected abstract S createSink();

	protected abstract TupleTypeInfo<IN> createTypeInfo();

	protected abstract IN generateValue(int counter, int checkpointID);

	protected abstract void verifyResultsIdealCircumstances(
		OneInputStreamTaskTestHarness<IN, IN> harness, OneInputStreamTask<IN, IN> task, S sink) throws Exception;

	protected abstract void verifyResultsDataPersistenceUponMissedNotify(
		OneInputStreamTaskTestHarness<IN, IN> harness, OneInputStreamTask<IN, IN> task, S sink) throws Exception;

	protected abstract void verifyResultsDataDiscardingUponRestore(
		OneInputStreamTaskTestHarness<IN, IN> harness, OneInputStreamTask<IN, IN> task, S sink) throws Exception;

	@Test
	public void testIdealCircumstances() throws Exception {
		S sink = createSink();
		OperatorExposingTask<IN> task = createTask();
		TupleTypeInfo<IN> info = createTypeInfo();
		OneInputStreamTaskTestHarness<IN, IN> testHarness = new OneInputStreamTaskTestHarness<>(task, 1, 1, info, info);
		StreamConfig streamConfig = testHarness.getStreamConfig();
		streamConfig.setStreamOperator(sink);

		int elementCounter = 1;

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		ArrayList<StreamTaskState> states = new ArrayList<>();

		for (int x = 0; x < 200; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 0)));
			elementCounter++;
		}
		testHarness.waitForInputProcessing();

		states.add(task.getOperator().snapshotOperatorState(states.size(), 0));
		task.notifyCheckpointComplete(states.size() - 1);

		for (int x = 0; x < 200; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 1)));
			elementCounter++;
		}
		testHarness.waitForInputProcessing();

		states.add(task.getOperator().snapshotOperatorState(states.size(), 0));
		task.notifyCheckpointComplete(states.size() - 1);

		for (int x = 0; x < 200; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 2)));
			elementCounter++;
		}
		testHarness.waitForInputProcessing();

		states.add(task.getOperator().snapshotOperatorState(states.size(), 0));
		task.notifyCheckpointComplete(states.size() - 1);

		testHarness.endInput();
		testHarness.waitForTaskCompletion();

		verifyResultsIdealCircumstances(testHarness, task, (S) task.getOperator());
	}

	@Test
	public void testDataPersistenceUponMissedNotify() throws Exception {
		S sink = createSink();
		OperatorExposingTask<IN> task = createTask();
		TupleTypeInfo<IN> info = createTypeInfo();
		OneInputStreamTaskTestHarness<IN, IN> testHarness = new OneInputStreamTaskTestHarness<>(task, 1, 1, info, info);
		StreamConfig streamConfig = testHarness.getStreamConfig();
		streamConfig.setStreamOperator(sink);

		int elementCounter = 1;

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		ArrayList<StreamTaskState> states = new ArrayList<>();

		for (int x = 0; x < 200; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 0)));
			elementCounter++;
		}
		testHarness.waitForInputProcessing();

		states.add(task.getOperator().snapshotOperatorState(states.size(), 0));
		task.notifyCheckpointComplete(states.size() - 1);

		for (int x = 0; x < 200; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 1)));
			elementCounter++;
		}
		testHarness.waitForInputProcessing();

		states.add(task.getOperator().snapshotOperatorState(states.size(), 0));

		for (int x = 0; x < 200; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 2)));
			elementCounter++;
		}
		testHarness.waitForInputProcessing();

		states.add(task.getOperator().snapshotOperatorState(states.size(), 0));
		task.notifyCheckpointComplete(states.size() - 1);

		testHarness.endInput();
		testHarness.waitForTaskCompletion();

		verifyResultsDataPersistenceUponMissedNotify(testHarness, task, (S) task.getOperator());
	}

	@Test
	public void testDataDiscardingUponRestore() throws Exception {
		S sink = createSink();
		OperatorExposingTask<IN> task = createTask();
		TupleTypeInfo<IN> info = createTypeInfo();
		OneInputStreamTaskTestHarness<IN, IN> testHarness = new OneInputStreamTaskTestHarness<>(task, 1, 1, info, info);
		StreamConfig streamConfig = testHarness.getStreamConfig();
		streamConfig.setStreamOperator(sink);

		int elementCounter = 1;

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		ArrayList<StreamTaskState> states = new ArrayList<>();

		for (int x = 0; x < 200; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 0)));
			elementCounter++;
		}
		testHarness.waitForInputProcessing();

		states.add(task.getOperator().snapshotOperatorState(states.size(), 0));
		task.notifyCheckpointComplete(states.size() - 1);

		for (int x = 0; x < 200; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 1)));
			elementCounter++;
		}
		testHarness.waitForInputProcessing();

		task.getOperator().restoreState(states.get(states.size() - 1), 0);

		for (int x = 0; x < 200; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 2)));
			elementCounter++;
		}
		testHarness.waitForInputProcessing();

		states.add(task.getOperator().snapshotOperatorState(states.size(), 0));
		task.notifyCheckpointComplete(states.size() - 1);

		testHarness.endInput();
		testHarness.waitForTaskCompletion();

		verifyResultsDataDiscardingUponRestore(testHarness, task, (S) task.getOperator());
	}
}
