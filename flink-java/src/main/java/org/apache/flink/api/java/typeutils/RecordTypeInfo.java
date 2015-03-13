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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.record.RecordSerializer;
import org.apache.flink.types.Record;

/**
 * Type information for the {@link Record} data type.
 */
public class RecordTypeInfo extends TypeInformation<Record> {

	private static final long serialVersionUID = 1L;
	
	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return 1;
	}
	
	@Override
	public int getTotalFields() {
		return 1;
	}

	@Override
	public Class<Record> getTypeClass() {
		return Record.class;
	}
	
	@Override
	public boolean isKeyType() {
		return false;
	}

	@Override
	public TypeSerializer<Record> createSerializer(ExecutionConfig config) {
		return RecordSerializer.get();
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public int hashCode() {
		return Record.class.hashCode() ^ 0x165667b1;
	}
	
	@Override
	public boolean equals(Object obj) {
		return obj.getClass() == RecordTypeInfo.class;
	}
	
	@Override
	public String toString() {
		return "RecordType";
	}
}
