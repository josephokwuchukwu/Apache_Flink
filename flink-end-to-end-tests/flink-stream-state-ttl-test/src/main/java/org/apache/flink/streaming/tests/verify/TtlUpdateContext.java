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

package org.apache.flink.streaming.tests.verify;

import javax.annotation.Nonnull;

import java.io.Serializable;

/** Contains context relevant for state update with TTL. */
public class TtlUpdateContext<UV, GV> implements Serializable {
	private final long timestampBeforeUpdate;
	private final GV valueBeforeUpdate;
	private final UV update;
	private final GV updatedValue;
	private final long timestampAfterUpdate;

	public TtlUpdateContext(
		long timestampBeforeUpdate,
		GV valueBeforeUpdate, UV update, GV updatedValue,
		long timestampAfterUpdate) {
		this.valueBeforeUpdate = valueBeforeUpdate;
		this.update = update;
		this.updatedValue = updatedValue;
		this.timestampBeforeUpdate = timestampBeforeUpdate;
		this.timestampAfterUpdate = timestampAfterUpdate;
	}

	long getTimestampBeforeUpdate() {
		return timestampBeforeUpdate;
	}

	GV getValueBeforeUpdate() {
		return valueBeforeUpdate;
	}

	@Nonnull
	public ValueWithTs<UV> getUpdateWithTs() {
		return new ValueWithTs<>(update, timestampBeforeUpdate, timestampAfterUpdate);
	}

	GV getUpdatedValue() {
		return updatedValue;
	}

	@Override
	public String toString() {
		return "TtlUpdateContext{" +
			"timestampBeforeUpdate=" + timestampBeforeUpdate +
			", valueBeforeUpdate=" + valueBeforeUpdate +
			", update=" + update +
			", updatedValue=" + updatedValue +
			", timestampAfterUpdate=" + timestampAfterUpdate +
			'}';
	}
}
