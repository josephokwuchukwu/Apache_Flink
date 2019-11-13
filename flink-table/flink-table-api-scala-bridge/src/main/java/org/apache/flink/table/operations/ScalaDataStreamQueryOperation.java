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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ObjectIdentifier;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Describes a relational operation that reads from a {@link DataStream}.
 *
 * <p>This operation may expose only part, or change the order of the fields available in a
 * {@link org.apache.flink.api.common.typeutils.CompositeType} of the underlying {@link DataStream}.
 * The {@link ScalaDataStreamQueryOperation#getFieldIndices()} describes the mapping between fields of the
 * {@link TableSchema} to the {@link org.apache.flink.api.common.typeutils.CompositeType}.
 */
@Internal
public class ScalaDataStreamQueryOperation<E> implements QueryOperation {

	// ScalaDataStreamQueryOperation represent a registered DataStream from `registerDataStream` method
	// or an unregistered DataStream from `fromDataStream`. However the name under which the DataStream
	// is registered is missing when converting a ScalaDataStreamQueryOperation to a RelNode.
	// So using `identifier` to keep the original name.
	private ObjectIdentifier identifier;
	private final DataStream<E> dataStream;
	private final int[] fieldIndices;
	private final TableSchema tableSchema;

	public ScalaDataStreamQueryOperation(
			DataStream<E> dataStream,
			int[] fieldIndices,
			TableSchema tableSchema) {
		this.dataStream = dataStream;
		this.tableSchema = tableSchema;
		this.fieldIndices = fieldIndices;
	}

	public DataStream<E> getDataStream() {
		return dataStream;
	}

	public int[] getFieldIndices() {
		return fieldIndices;
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	public ObjectIdentifier getIdentifier() {
		return identifier;
	}

	public void setIdentifier(ObjectIdentifier identifier) {
		this.identifier = identifier;
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> args = new LinkedHashMap<>();
		if (identifier != null) {
			args.put("id", identifier.asSummaryString());
		} else {
			args.put("id", dataStream.getId());
		}
		args.put("fields", tableSchema.getFieldNames());

		return OperationUtils.formatWithChildren(
			"DataStream",
			args,
			getChildren(),
			Operation::asSummaryString);
	}

	@Override
	public List<QueryOperation> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public <T> T accept(QueryOperationVisitor<T> visitor) {
		return visitor.visit(this);
	}
}
