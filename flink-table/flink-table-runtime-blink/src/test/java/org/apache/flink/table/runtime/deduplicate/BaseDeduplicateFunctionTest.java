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

package org.apache.flink.table.runtime.deduplicate;

import org.apache.flink.table.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.generated.RecordEqualiser;
import org.apache.flink.table.runtime.util.BaseRowHarnessAssertor;
import org.apache.flink.table.runtime.util.BaseRowRecordEqualiser;
import org.apache.flink.table.runtime.util.BinaryRowKeySelector;
import org.apache.flink.table.runtime.util.GenericRowRecordSortComparator;
import org.apache.flink.table.type.InternalTypes;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

/**
 * Base Tests for {@link DeduplicateFunction} and {@link MiniBatchDeduplicateFunction}.
 */
public abstract class BaseDeduplicateFunctionTest {

	protected BaseRowTypeInfo inputRowType = new BaseRowTypeInfo(
			InternalTypes.STRING,
			InternalTypes.LONG,
			InternalTypes.INT);

	protected GeneratedRecordEqualiser generatedEqualiser = new GeneratedRecordEqualiser("", "", new Object[0]) {

		private static final long serialVersionUID = -5080236034372380295L;

		@Override
		public RecordEqualiser newInstance(ClassLoader classLoader) {
			return new BaseRowRecordEqualiser();
		}
	};

	private int rowKeyIdx = 1;
	protected BinaryRowKeySelector rowKeySelector = new BinaryRowKeySelector(new int[] { rowKeyIdx },
			inputRowType.getInternalTypes());


	protected BaseRowHarnessAssertor assertor = new BaseRowHarnessAssertor(
			inputRowType.getFieldTypes(),
			new GenericRowRecordSortComparator(rowKeyIdx, inputRowType.getInternalTypes()[rowKeyIdx]));

}
