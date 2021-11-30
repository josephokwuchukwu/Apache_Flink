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

package org.apache.flink.streaming.api.operators.sorted.state;

import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests that verify an exception is thrown in methods that are not supported in the BATCH runtime
 * mode.
 */
public class BatchExecutionStateBackendVerificationTest extends TestLogger {

    private static final LongSerializer LONG_SERIALIZER = new LongSerializer();

    @Rule public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void verifySnapshotNotSupported() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Snapshotting is not supported in BATCH runtime mode.");

        BatchExecutionKeyedStateBackend<Long> stateBackend =
                new BatchExecutionKeyedStateBackend<>(LONG_SERIALIZER, new KeyGroupRange(0, 9));

        long checkpointId = 0L;
        CheckpointStreamFactory streamFactory = new MemCheckpointStreamFactory(10);
        stateBackend.snapshot(checkpointId, 0L, streamFactory, CheckpointType.CHECKPOINT);
    }
}
