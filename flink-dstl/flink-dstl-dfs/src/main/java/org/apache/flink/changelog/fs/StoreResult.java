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

package org.apache.flink.changelog.fs;

import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.changelog.SequenceNumber;

final class StoreResult {
    public final StreamStateHandle streamStateHandle;
    public final long offset;
    public final SequenceNumber sequenceNumber;

    public StoreResult(
            StreamStateHandle streamStateHandle, long offset, SequenceNumber sequenceNumber) {
        this.streamStateHandle = streamStateHandle;
        this.offset = offset;
        this.sequenceNumber = sequenceNumber;
    }

    public StreamStateHandle getStreamStateHandle() {
        return streamStateHandle;
    }

    public long getOffset() {
        return offset;
    }

    public SequenceNumber getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public String toString() {
        return "streamStateHandle="
                + streamStateHandle
                + ", offset="
                + offset
                + ", sequenceNumber="
                + sequenceNumber;
    }
}
