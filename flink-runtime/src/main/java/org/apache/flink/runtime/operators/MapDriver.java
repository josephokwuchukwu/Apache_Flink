/**
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


package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.functions.MapFunctional;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

/**
 * Map task which is executed by a Nephele task manager. The task has a single
 * input and one or multiple outputs. It is provided with a MapFunction
 * implementation.
 * <p>
 * The MapTask creates an iterator over all key-value pairs of its input and hands that to the <code>map()</code> method
 * of the MapFunction.
 * 
 * @see org.apache.flink.api.common.functions.MapFunctional
 * 
 * @param <IT> The mapper's input data type.
 * @param <OT> The mapper's output data type.
 */
public class MapDriver<IT, OT> implements PactDriver<MapFunctional<IT, OT>, OT> {
	
	private PactTaskContext<MapFunctional<IT, OT>, OT> taskContext;
	
	private volatile boolean running;
	
	
	@Override
	public void setup(PactTaskContext<MapFunctional<IT, OT>, OT> context) {
		this.taskContext = context;
		this.running = true;
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public Class<MapFunctional<IT, OT>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<MapFunctional<IT, OT>> clazz = (Class<MapFunctional<IT, OT>>) (Class<?>) MapFunctional.class;
		return clazz;
	}

	@Override
	public boolean requiresComparatorOnInput() {
		return false;
	}

	@Override
	public void prepare() {
		// nothing, since a mapper does not need any preparation
	}

	@Override
	public void run() throws Exception {
		// cache references on the stack
		final MutableObjectIterator<IT> input = this.taskContext.getInput(0);
		final MapFunctional<IT, OT> function = this.taskContext.getStub();
		final Collector<OT> output = this.taskContext.getOutputCollector();

		IT record = this.taskContext.<IT>getInputSerializer(0).getSerializer().createInstance();

		while (this.running && ((record = input.next(record)) != null)) {
			output.collect(function.map(record));
		}
	}

	@Override
	public void cleanup() {
		// mappers need no cleanup, since no strategies are used.
	}

	@Override
	public void cancel() {
		this.running = false;
	}
}
