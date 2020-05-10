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

package org.apache.flink.table.planner.sinks;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;

/**
 * An utility class that changes to default conversion class
 * and converts time attributes (proc time / event time) to regular timestamps.
 */
class SelectTableSinkSchemaConverter {

	/**
	 * Change to default conversion class and
	 * convert time attributes (proc time / event time) to regular timestamps,
	 * return a new {@link TableSchema}.
	 */
	static TableSchema convert(TableSchema tableSchema) {
		DataType[] oldTypes = tableSchema.getFieldDataTypes();
		String[] oldNames = tableSchema.getFieldNames();

		TableSchema.Builder builder = TableSchema.builder();
		for (int i = 0; i < tableSchema.getFieldCount(); i++) {
			// change to default conversion class
			DataType fieldType = LogicalTypeDataTypeConverter.fromLogicalTypeToDataType(
					LogicalTypeDataTypeConverter.fromDataTypeToLogicalType(oldTypes[i]));
			String fieldName = oldNames[i];
			if (fieldType.getLogicalType() instanceof TimestampType) {
				TimestampType timestampType = (TimestampType) fieldType.getLogicalType();
				if (!timestampType.getKind().equals(TimestampKind.REGULAR)) {
					// converts `TIME ATTRIBUTE(ROWTIME)`/`TIME ATTRIBUTE(PROCTIME)` to `TIMESTAMP(3)`
					builder.field(fieldName, DataTypes.TIMESTAMP(3));
					continue;
				}
			}
			builder.field(fieldName, fieldType);
		}
		return builder.build();
	}
}
