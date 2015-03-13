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

package org.apache.flink.streaming.connectors.kafka.api.simple;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.OperatorState;
import org.apache.flink.streaming.api.streamvertex.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.util.DeserializationSchema;
import org.apache.flink.util.Collector;

/**
 * Kafka source persisting its offset through the {@link OperatorState} interface.
 * This allows the offset to be restored to the latest one that has been acknowledged
 * by the whole execution graph.
 *
 * @param <OUT>
 *            Type of the messages on the topic.
 */
public class PersistentKafkaSource<OUT> extends SimpleKafkaSource<OUT> {

	private static final long serialVersionUID = 1L;

	private long initialOffset;

	private transient OperatorState<Long> kafkaOffSet;

	public PersistentKafkaSource(String topicId, String host, int port, int partition, long initialOffset,
			DeserializationSchema<OUT> deserializationSchema) {
		super(topicId, host, port, partition, deserializationSchema);
		this.initialOffset = initialOffset;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void open(Configuration parameters) throws InterruptedException {
		StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
		
		if (context.containsState("kafka")) {
			kafkaOffSet = (OperatorState<Long>) context.getState("kafka");
		} else {
			kafkaOffSet = new OperatorState<Long>(initialOffset);
			context.registerState("kafka", kafkaOffSet);
		}

		super.open(parameters);
	}

	@Override
	protected void setInitialOffset(Configuration config) throws InterruptedException{
		iterator.initializeFromOffset(kafkaOffSet.getState());
	}


	@Override
	public void run(Collector<OUT> collector) throws Exception {
		MessageWithOffset msg;
		while (iterator.hasNext()) {
			msg = iterator.nextWithOffset();
			OUT out = schema.deserialize(msg.getMessage());
			collector.collect(out);
			kafkaOffSet.update(msg.getOffset());
		}
	}
}
