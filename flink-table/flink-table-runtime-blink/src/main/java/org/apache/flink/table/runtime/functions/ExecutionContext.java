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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.dataformat.BaseRow;

/**
 * A ExecutionContext contains information about the context in which functions are executed and
 * the APIs to create state.
 */
public interface ExecutionContext {

	// TODO add create state method.

	/**
	 * @return the key serializer of state key
	 */
	<K> TypeSerializer<K> getKeySerializer();

	/**
	 * @return key of the current processed element.
	 */
	BaseRow currentKey();

	/**
	 * Sets current key.
	 */
	void setCurrentKey(BaseRow key);

	RuntimeContext getRuntimeContext();
}
