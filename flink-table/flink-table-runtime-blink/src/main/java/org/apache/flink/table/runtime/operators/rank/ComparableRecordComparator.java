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

package org.apache.flink.table.runtime.operators.rank;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Because it's impossible to restore a {@link RecordComparator} instance generated by
 * {@link GeneratedRecordComparator} from checkpoint snapshot. Hence, we introduce
 * {@link ComparableRecordComparator} class to wrap the {@link GeneratedRecordComparator}.
 * A {@link ComparableRecordComparator} instance is serializable and can restore the {@link RecordComparator}
 * from the serialized {@link ComparableRecordComparator}. Besides, the {@link ComparableRecordComparator#equals(Object)}
 * doesn't take {@link GeneratedRecordComparator} into account, because the code is not deterministic
 * across different client. Therefore, {@link ComparableRecordComparator#equals(Object)} only compares the
 * meta information used for generating code of {@link RecordComparator}.
 *
 * <p>Note: currently, this class is only used for {@link RetractableTopNFunction}.
 *
 * @see RetractableTopNFunction
 */
public final class ComparableRecordComparator implements RecordComparator {
	private static final long serialVersionUID = 4386377835781068140L;

	private transient Comparator<RowData> comparator;
	private final GeneratedRecordComparator generatedRecordComparator;

	// used for compare equals for instances of RowDataComparator
	private final int[] compareKeyPositions;
	private final LogicalType[] compareKeyTypes;
	private final boolean[] compareOrders;
	private final boolean[] nullsIsLast;

	public ComparableRecordComparator(
			GeneratedRecordComparator generatedRecordComparator,
			int[] compareKeyPositions,
			LogicalType[] compareKeyTypes,
			boolean[] compareOrders,
			boolean[] nullsIsLast) {
		this.generatedRecordComparator = generatedRecordComparator;
		this.compareKeyPositions = compareKeyPositions;
		this.compareKeyTypes = compareKeyTypes;
		this.compareOrders = compareOrders;
		this.nullsIsLast = nullsIsLast;
	}

	public GeneratedRecordComparator getGeneratedRecordComparator() {
		return generatedRecordComparator;
	}

	@Override
	public int compare(RowData o1, RowData o2) {
		if (comparator == null) {
			comparator = generatedRecordComparator.newInstance(Thread.currentThread().getContextClassLoader());
		}
		return comparator.compare(o1, o2);
	}

	@Override
	public int hashCode() {
		int result = Arrays.hashCode(compareKeyPositions);
		result = 31 * result + Arrays.hashCode(compareKeyTypes);
		result = 31 * result + Arrays.hashCode(compareOrders);
		result = 31 * result + Arrays.hashCode(nullsIsLast);
		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ComparableRecordComparator that = (ComparableRecordComparator) o;
		return Arrays.equals(compareKeyPositions, that.compareKeyPositions) &&
			Arrays.equals(compareKeyTypes, that.compareKeyTypes) &&
			Arrays.equals(compareOrders, that.compareOrders) &&
			Arrays.equals(nullsIsLast, that.nullsIsLast);
	}
}
