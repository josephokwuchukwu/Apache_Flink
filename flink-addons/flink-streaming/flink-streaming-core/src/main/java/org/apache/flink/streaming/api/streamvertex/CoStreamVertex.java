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

package org.apache.flink.streaming.api.streamvertex;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.io.network.api.reader.BufferReader;
import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.runtime.io.network.api.reader.UnionBufferReader;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.streaming.api.invokable.operator.co.CoInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.io.CoReaderIterator;
import org.apache.flink.streaming.io.CoRecordReader;
import org.apache.flink.util.MutableObjectIterator;

import java.util.ArrayList;

public class CoStreamVertex<IN1, IN2, OUT> extends
		StreamVertex<IN1,OUT> {

	private OutputHandler<OUT> outputHandler;

	protected StreamRecordSerializer<IN1> inputDeserializer1 = null;
	protected StreamRecordSerializer<IN2> inputDeserializer2 = null;

	MutableObjectIterator<StreamRecord<IN1>> inputIter1;
	MutableObjectIterator<StreamRecord<IN2>> inputIter2;

	CoRecordReader<DeserializationDelegate<StreamRecord<IN1>>, DeserializationDelegate<StreamRecord<IN2>>> coReader;
	CoReaderIterator<StreamRecord<IN1>, StreamRecord<IN2>> coIter;

	private CoInvokable<IN1, IN2, OUT> userInvokable;
	private static int numTasks;

	public CoStreamVertex() {
		userInvokable = null;
		numTasks = newVertex();
		instanceID = numTasks;
	}

	private void setDeserializers() {
		TypeInformation<IN1> inputTypeInfo1 = configuration.getTypeInfoIn1();
		inputDeserializer1 = new StreamRecordSerializer<IN1>(inputTypeInfo1);

		TypeInformation<IN2> inputTypeInfo2 = configuration.getTypeInfoIn2();
		inputDeserializer2 = new StreamRecordSerializer<IN2>(inputTypeInfo2);
	}

	@Override
	public void setInputsOutputs() {
		outputHandler = new OutputHandler<OUT>(this);

		setConfigInputs();

		coIter = new CoReaderIterator<StreamRecord<IN1>, StreamRecord<IN2>>(coReader,
				inputDeserializer1, inputDeserializer2);
	}

	@Override
	protected void setInvokable() {
		userInvokable = configuration.getUserInvokable();
		userInvokable.initialize(outputHandler.getCollector(), coIter, inputDeserializer1,
				inputDeserializer2, isMutable);
	}

	protected void setConfigInputs() throws StreamVertexException {
		setDeserializers();

		int numberOfInputs = configuration.getNumberOfInputs();

		ArrayList<BufferReader> inputList1 = new ArrayList<BufferReader>();
		ArrayList<BufferReader> inputList2 = new ArrayList<BufferReader>();

		for (int i = 0; i < numberOfInputs; i++) {
			int inputType = configuration.getInputType(i);
			BufferReader reader = getEnvironment().getReader(i);
			switch (inputType) {
			case 1:
				inputList1.add(reader);
				break;
			case 2:
				inputList2.add(reader);
				break;
			default:
				throw new RuntimeException("Invalid input type number: " + inputType);
			}
		}

		MutableRecordReader<DeserializationDelegate<StreamRecord<IN1>>> reader1;
		if (inputList1.size() == 1) {
			reader1 = new MutableRecordReader<DeserializationDelegate<StreamRecord<IN1>>>(inputList1.get(0));
		}
		else if (inputList1.size() > 1) {
			reader1 = new MutableRecordReader<DeserializationDelegate<StreamRecord<IN1>>>(new UnionBufferReader(inputList1.toArray(new BufferReader[inputList1.size()])));
		}
		else {
			throw new IllegalStateException("Illegal input size for first input.");
		}

		MutableRecordReader<DeserializationDelegate<StreamRecord<IN2>>> reader2;
		if (inputList2.size() == 1) {
			reader2 = new MutableRecordReader<DeserializationDelegate<StreamRecord<IN2>>>(inputList2.get(0));
		}
		else if (inputList2.size() > 1) {
			reader2 = new MutableRecordReader<DeserializationDelegate<StreamRecord<IN2>>>(new UnionBufferReader(inputList2.toArray(new BufferReader[inputList2.size()])));
		}
		else {
			throw new IllegalStateException("Illegal input size for first input.");
		}

		coReader = new CoRecordReader<DeserializationDelegate<StreamRecord<IN1>>, DeserializationDelegate<StreamRecord<IN2>>>(
				reader1, reader2);
	}

	@Override
	public void invoke() throws Exception {
		outputHandler.invokeUserFunction("CO-TASK", userInvokable);
	}

}
