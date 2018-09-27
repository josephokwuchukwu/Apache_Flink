/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;

import org.rocksdb.ColumnFamilyHandle;

import javax.annotation.Nonnull;

import java.io.Closeable;

/**
 * This class combines a {@link ColumnFamilyHandle} and {@link RegisteredStateMetaInfoBase} to represent a state that is
 * backed by a RocksDB column family.
 */
public class StateColumnFamilyHandle implements Closeable {

	@Nonnull
	private final ColumnFamilyHandle columnFamilyHandle;

	@Nonnull
	private RegisteredStateMetaInfoBase stateMetaInfo;

	public StateColumnFamilyHandle(
		@Nonnull ColumnFamilyHandle columnFamilyHandle,
		@Nonnull RegisteredStateMetaInfoBase stateMetaInfo) {

		this.columnFamilyHandle = columnFamilyHandle;
		this.stateMetaInfo = stateMetaInfo;
	}

	@Nonnull
	public ColumnFamilyHandle getColumnFamilyHandle() {
		return columnFamilyHandle;
	}

	@Nonnull
	public RegisteredStateMetaInfoBase getStateMetaInfo() {
		return stateMetaInfo;
	}

	public void setStateMetaInfo(@Nonnull RegisteredStateMetaInfoBase stateMetaInfo) {
		this.stateMetaInfo = stateMetaInfo;
	}

	@Override
	public void close() {
		columnFamilyHandle.close();
	}
}
