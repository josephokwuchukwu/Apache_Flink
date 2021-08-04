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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.MockResultPartitionWriter;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkArgument;

/** {@link ResultPartitionWriter} that collects output on the List. */
@ThreadSafe
public abstract class AbstractCollectingResultPartitionWriter extends MockResultPartitionWriter {

    @Override
    public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {
        checkArgument(targetSubpartition < getNumberOfSubpartitions());
        deserializeRecord(record);
    }

    @Override
    public void broadcastRecord(ByteBuffer record) throws IOException {
        deserializeRecord(record);
    }

    private void deserializeRecord(ByteBuffer serializedRecord) throws IOException {
        checkArgument(serializedRecord.hasArray());

        MemorySegment segment = MemorySegmentFactory.wrap(serializedRecord.array());
        NetworkBuffer buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
        buffer.setSize(serializedRecord.remaining());
        deserializeBuffer(buffer);
    }

    protected abstract void deserializeBuffer(Buffer buffer) throws IOException;
}
