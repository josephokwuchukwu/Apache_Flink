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

package org.apache.flink.table.runtime.operators.deduplicate;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.runtime.util.BaseRowHarnessAssertor;
import org.apache.flink.table.runtime.util.BinaryRowKeySelector;
import org.apache.flink.table.runtime.util.GenericRowRecordSortComparator;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.record;

/**
 * Base class of tests for all kinds of DeduplicateFunction.
 */
abstract class DeduplicateFunctionTestBase {

	Time minTime = Time.milliseconds(10);
	long incCleanupKeys = 3;
	BaseRowTypeInfo inputRowType = new BaseRowTypeInfo(new VarCharType(VarCharType.MAX_LENGTH), new BigIntType(),
			new IntType());

	int rowKeyIdx = 1;
	BinaryRowKeySelector rowKeySelector = new BinaryRowKeySelector(new int[] { rowKeyIdx },
			inputRowType.getLogicalTypes());

	BaseRowHarnessAssertor assertor = new BaseRowHarnessAssertor(
			inputRowType.getFieldTypes(),
			new GenericRowRecordSortComparator(rowKeyIdx, inputRowType.getLogicalTypes()[rowKeyIdx]));

	public void triggerMoreIncrementalCleanupByOtherOps(
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness) throws Exception {
		for (long i = incCleanupKeys; i < incCleanupKeys * 10; i++) {
			testHarness.processElement(record("book", i, 20));
		}
	}

	public void addRecordToExpectedOutput(List<Object> expectedOutput) {
		for (long i = incCleanupKeys; i < incCleanupKeys * 10; i++) {
			expectedOutput.add(record("book", i, 20));
		}
	}

}
