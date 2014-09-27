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


package org.apache.flink.test.recordJobs.util;

import org.apache.flink.api.java.record.io.DelimitedInputFormat;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;

public class IntTupleDataInFormat extends DelimitedInputFormat {
	private static final long serialVersionUID = 1L;

	public static final int MAX_COLUMNS = 20;

	public static final int DELIMITER = '|';
	
	private final IntValue key = new IntValue();
	private final int[] offsets = new int[MAX_COLUMNS];

	@Override
	public Record readRecord(Record target, byte[] line, int offset, int numBytes) {
		final int limit = offset + numBytes;
		int readPos = offset;

		// allocate the offsets array
		final int[] offsets = this.offsets;
		offsets[0] = offset;

		int col = 1; // the column we are in

		while (readPos < limit) {
			if (line[readPos++] == DELIMITER) {
				offsets[col++] = readPos;
			}
		}

		final Tuple value = new Tuple(line, offsets, col - 1);
		this.key.setValue((int) value.getLongValueAt(0));
		
		target.setField(0, this.key);
		target.setField(1, value);
		return target;
	}
}
