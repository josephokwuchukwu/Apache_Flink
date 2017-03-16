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

package org.apache.flink.runtime.checkpoint.savepoint;

import org.apache.flink.runtime.checkpoint.TaskState;

import java.util.Collection;

/**
 * Savepoint version 1.
 *
 * <p>This format was introduced with Flink 1.1.0.
 *
 * @deprecated Deprecated in favour of {@link SavepointV2}.
 */
@Deprecated
public class SavepointV1 extends AbstractSavepoint {

	/** The savepoint version. */
	public static final int VERSION = 1;

	public SavepointV1(long checkpointId, Collection<TaskState> taskStates) {
		super(checkpointId, taskStates);
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

}
