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

package org.apache.flink.table.sources.datagen;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.StatefulSequenceSource;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * A {@link StreamTableSource} that emits each number from a given interval exactly once,
 * possibly in parallel. See {@link StatefulSequenceSource}.
 */
public class DataGenTableSource implements StreamTableSource<Row> {

	private final DataGenerator[] fieldGenerators;
	private final TableSchema schema;
	private final long rowsPerSecond;

	public DataGenTableSource(DataGenerator[] fieldGenerators, TableSchema schema, long rowsPerSecond) {
		this.fieldGenerators = fieldGenerators;
		this.schema = schema;
		this.rowsPerSecond = rowsPerSecond;
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		return execEnv.addSource(createSource());
	}

	@VisibleForTesting
	public DataGeneratorSource<Row> createSource() {
		return new DataGeneratorSource<>(new RowGenerator(), rowsPerSecond);
	}

	@Override
	public TableSchema getTableSchema() {
		return schema;
	}

	@Override
	public DataType getProducedDataType() {
		return schema.toRowDataType();
	}

	private class RowGenerator implements DataGenerator<Row> {

		@Override
		public void open(
				String name,
				FunctionInitializationContext context,
				RuntimeContext runtimeContext) throws Exception {
			for (int i = 0; i < fieldGenerators.length; i++) {
				fieldGenerators[i].open(schema.getFieldName(i).get(), context, runtimeContext);
			}
		}

		@Override
		public boolean hasNext() {
			for (DataGenerator generator : fieldGenerators) {
				if (!generator.hasNext()) {
					return false;
				}
			}
			return true;
		}

		@Override
		public Row next() {
			Row row = new Row(schema.getFieldCount());
			for (int i = 0; i < fieldGenerators.length; i++) {
				row.setField(i, fieldGenerators[i].next());
			}
			return row;
		}
	}
}
