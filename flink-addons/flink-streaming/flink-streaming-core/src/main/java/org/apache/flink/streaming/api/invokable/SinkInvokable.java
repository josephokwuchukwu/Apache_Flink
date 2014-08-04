/**
 *
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
 *
 */

package org.apache.flink.streaming.api.invokable;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.function.sink.SinkFunction;

public class SinkInvokable<IN> extends StreamRecordInvokable<IN, IN> {
	private static final long serialVersionUID = 1L;

	private SinkFunction<IN> sinkFunction;

	public SinkInvokable(SinkFunction<IN> sinkFunction) {
		this.sinkFunction = sinkFunction;
	}

	@Override
	protected void immutableInvoke() throws Exception {
		while ((reuse = recordIterator.next(reuse)) != null) {
			sinkFunction.invoke((IN) reuse.getObject());
			resetReuse();
		}
	}

	@Override
	protected void mutableInvoke() throws Exception {
		while ((reuse = recordIterator.next(reuse)) != null) {
			sinkFunction.invoke((IN) reuse.getObject());
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		if (sinkFunction instanceof RichFunction) {
			((RichFunction) sinkFunction).open(parameters);
		}
	}

	@Override
	public void close() throws Exception {
		if (sinkFunction instanceof RichFunction) {
			((RichFunction) sinkFunction).close();
		}
	}
}
