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

package org.apache.flink.connector.pulsar.source.reader.emitter;

import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SourceOutputWrapper;
import org.apache.flink.connector.pulsar.source.reader.message.PulsarMessage;
import org.apache.flink.connector.pulsar.source.reader.source.PulsarOrderedSourceReader;
import org.apache.flink.connector.pulsar.source.reader.source.PulsarUnorderedSourceReader;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplitState;

/**
 * The {@link RecordEmitter} implementation for both {@link PulsarOrderedSourceReader} and {@link
 * PulsarUnorderedSourceReader}. We would always update the last consumed message id in this
 * emitter.
 */
public class PulsarRecordEmitter<T>
        implements RecordEmitter<PulsarMessage<T>, T, PulsarPartitionSplitState> {

    @Override
    public void emitRecord(
            PulsarMessage<T> element,
            SourceOutputWrapper<T> output,
            PulsarPartitionSplitState splitState)
            throws Exception {
        // Sink the record to source output.
        output.collect(element.getValue(), element.getEventTime());
        // Update the split state.
        splitState.setLatestConsumedId(element.getId());
    }
}
