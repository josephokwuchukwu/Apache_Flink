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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.Optional;
import java.util.Properties;

/**
 * Kafka 0.11 table sink for writing data into Kafka.
 */
@Internal
public class Kafka011DynamicSink extends KafkaDynamicSinkBase {

	public Kafka011DynamicSink(
			DataType consumedDataType,
			String topic,
			Properties properties,
			Optional<FlinkKafkaPartitioner<RowData>> partitioner,
			EncodingFormat<SerializationSchema<RowData>> encodingFormat,
			String semantic) {
		super(
				consumedDataType,
				topic,
				properties,
				partitioner,
				encodingFormat,
				semantic);
	}

	@Override
	protected SinkFunction<RowData> createKafkaProducer(
			String topic,
			Properties properties,
			SerializationSchema<RowData> serializationSchema,
			Optional<FlinkKafkaPartitioner<RowData>> partitioner,
			String semantic) {
		return new FlinkKafkaProducer011<>(
				topic,
				new KeyedSerializationSchemaWrapper<>(serializationSchema),
				properties,
				partitioner,
				getSemantic(semantic),
				FlinkKafkaProducer011.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE);
	}

	@Override
	public DynamicTableSink copy() {
		return new Kafka011DynamicSink(
				this.consumedDataType,
				this.topic,
				this.properties,
				this.partitioner,
				this.encodingFormat,
				this.semantic);
	}

	@Override
	public String asSummaryString() {
		return "Kafka 0.11 table sink";
	}

	private FlinkKafkaProducer011.Semantic getSemantic(String semantic){
		switch (semantic){
			case "exactly-once":
				return FlinkKafkaProducer011.Semantic.EXACTLY_ONCE;
			case "at-least-once":
				return FlinkKafkaProducer011.Semantic.AT_LEAST_ONCE;
			case "none":
				return FlinkKafkaProducer011.Semantic.NONE;
			default:
				throw new TableException("Validator should have validted.");
		}
	}
}
