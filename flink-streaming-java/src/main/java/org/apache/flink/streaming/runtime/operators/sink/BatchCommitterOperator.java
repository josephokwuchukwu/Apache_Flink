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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Runtime {@link org.apache.flink.streaming.api.operators.StreamOperator} for executing
 * {@link Committer} in the batch execution mode.
 *
 * @param <CommT> The committable type of the {@link Committer}.
 */
final class BatchCommitterOperator<CommT> extends AbstractStreamOperator<byte[]>
		implements OneInputStreamOperator<byte[], byte[]>, BoundedOneInput {

	/** Responsible for committing the committable to the external system. */
	private final Committer<CommT> committer;

	/** Record all the committables until the end of the input. */
	private final List<CommT> allCommittables;

	/** The serializer for the committable. */
	private final SimpleVersionedSerializer<CommT> committableSerializer;

	/** Whether sending committables to the downstream operator or not. */
	private final boolean sendCommittables;

	public BatchCommitterOperator(
			Committer<CommT> committer,
			SimpleVersionedSerializer<CommT> committableSerializer, boolean sendCommittables) {
		this.committer = checkNotNull(committer);
		this.committableSerializer = committableSerializer;
		this.sendCommittables = sendCommittables;
		this.allCommittables = new ArrayList<>();
	}

	@Override
	public void processElement(StreamRecord<byte[]> element) throws IOException {
		allCommittables.add(committableSerializer.deserialize(
				committableSerializer.getVersion(),
				element.getValue()));
	}

	@Override
	public void endInput() throws Exception {
		if (!allCommittables.isEmpty()) {
			final List<CommT> neededRetryCommittables = committer.commit(allCommittables);
			if (!neededRetryCommittables.isEmpty()) {
				throw new UnsupportedOperationException("Currently does not support the re-commit!");
			}
			if (sendCommittables) {
				for (CommT committable : allCommittables) {
					output.collect(new StreamRecord<>(committableSerializer.serialize(committable)));
				}
			}
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		committer.close();
	}
}
