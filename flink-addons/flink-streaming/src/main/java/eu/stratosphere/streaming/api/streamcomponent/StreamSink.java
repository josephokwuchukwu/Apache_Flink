/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.api.streamcomponent;

import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.streaming.api.AckEvent;
import eu.stratosphere.streaming.api.FailEvent;
import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class StreamSink extends AbstractOutputTask {

	private List<RecordReader<StreamRecord>> inputs;
	private UserSinkInvokable userFunction;
	private StreamComponentHelper<StreamSink> streamSinkHelper;

	public StreamSink() {
		// TODO: Make configuration file visible and call setClassInputs() here
		inputs = new LinkedList<RecordReader<StreamRecord>>();
		userFunction = null;
		streamSinkHelper = new StreamComponentHelper<StreamSink>();
	}

	@Override
	public void registerInputOutput() {
		Configuration taskConfiguration = getTaskConfiguration();
		
		try {
			streamSinkHelper.setConfigInputs(this, taskConfiguration, inputs);
		} catch (Exception e) {
			e.printStackTrace();
		}
		userFunction = streamSinkHelper.getUserFunction(taskConfiguration);
	}

	@Override
	public void invoke() throws Exception {
		boolean hasInput = true;
		while (hasInput) {
			hasInput = false;
			for (RecordReader<StreamRecord> input : inputs) {
				if (input.hasNext()) {
					hasInput = true;
					StreamRecord rec = input.next();
					String id = rec.getId();
					try {
						userFunction.invoke(rec);
						streamSinkHelper.threadSafePublish(new AckEvent(id), input);
					} catch (Exception e) {
						streamSinkHelper.threadSafePublish(new FailEvent(id), input);

					}
				}

			}
		}
	}
}
