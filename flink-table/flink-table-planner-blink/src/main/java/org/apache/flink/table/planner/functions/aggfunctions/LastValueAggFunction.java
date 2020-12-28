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

package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import static org.apache.flink.table.types.utils.DataTypeUtils.toInternalDataType;

/**
 * Built-in LAST_VALUE aggregate function.
 */
@Internal
public final class LastValueAggFunction<T> extends InternalAggregateFunction<T, RowData> {

	private transient DataType valueDataType;

	public LastValueAggFunction(LogicalType valueType) {
		this.valueDataType = toInternalDataType(valueType);
	}

	// --------------------------------------------------------------------------------------------
	// Planning
	// --------------------------------------------------------------------------------------------

	public void merge(GenericRowData acc, Iterable<GenericRowData> its) {
		Long thisOrder = null;
		if (acc != null) {
			thisOrder = (Long) acc.getField(1);
		}
		for (GenericRowData other : its) {
			if (other == null) {
				continue;
			}
			Long otherOrder = (Long) other.getField(1);
			if (acc == null) {
				acc = other;
				thisOrder = otherOrder;
			} else if (otherOrder > thisOrder) {
				acc.setField(0, other.getField(0));
				acc.setField(1, otherOrder);
				thisOrder = otherOrder;
			}
		}
	}

	@Override
	public DataType[] getInputDataTypes() {
		return new DataType[]{valueDataType};
	}

	@Override
	public DataType getAccumulatorDataType() {
		return DataTypes.ROW(
			DataTypes.FIELD("lastValue", valueDataType.nullable()),
			DataTypes.FIELD("lastOrder", DataTypes.BIGINT())).bridgedTo(RowData.class);
	}

	@Override
	public DataType getOutputDataType() {
		return valueDataType;
	}

	@Override
	public boolean isDeterministic() {
		return false;
	}

	// --------------------------------------------------------------------------------------------
	// Runtime
	// --------------------------------------------------------------------------------------------

	@Override
	public RowData createAccumulator() {
		GenericRowData acc = new GenericRowData(2);
		acc.setField(0, null);
		acc.setField(1, Long.MIN_VALUE);
		return acc;
	}

	public void accumulate(RowData rowData, Object value) {
		GenericRowData acc = (GenericRowData) rowData;
		if (value != null) {
			acc.setField(0, value);
		}
	}

	public void accumulate(RowData rowData, Object value, Long order) {
		GenericRowData acc = (GenericRowData) rowData;
		if (value != null && acc.getLong(1) < order) {
			acc.setField(0, value);
			acc.setField(1, order);
		}
	}

	public void accumulate(GenericRowData acc, StringData value) {
		if (value != null) {
			accumulate(acc, (Object) ((BinaryStringData) value).copy());
		}
	}

	public void accumulate(GenericRowData acc, StringData value, Long order) {
		if (value != null) {
			accumulate(acc, (Object) ((BinaryStringData) value).copy(), order);
		}
	}

	public void resetAccumulator(RowData rowData) {
		GenericRowData acc = (GenericRowData) rowData;
		acc.setField(0, null);
		acc.setField(1, Long.MIN_VALUE);
	}

	@SuppressWarnings("unchecked")
	@Override
	public T getValue(RowData rowData) {
		GenericRowData acc = (GenericRowData) rowData;
		return (T) acc.getField(0);
	}
}
