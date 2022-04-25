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

package org.apache.flink.api.connector.source.lib;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceEnumerator;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceReader;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.NumberSequenceIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A data source that produces a sequence of numbers (longs). This source is useful for testing and
 * for cases that just need a stream of N events of any kind.
 *
 * <p>The source splits the sequence into as many parallel sub-sequences as there are parallel
 * source readers. Each sub-sequence will be produced in order. Consequently, if the parallelism is
 * limited to one, this will produce one sequence in order.
 *
 * <p>This source is always bounded. For very long sequences (for example over the entire domain of
 * long integer values), user may want to consider executing the application in a streaming manner,
 * because, despite the fact that the produced stream is bounded, the end bound is pretty far away.
 */
@Public
public class NumberSequenceSource
        implements Source<Long, NumberSequenceSplit<Long>, Collection<NumberSequenceSplit<Long>>>,
                ResultTypeQueryable<Long> {

    private static final long serialVersionUID = 1L;

    /** The starting number in the sequence, inclusive. */
    private final long from;

    /** The end number in the sequence, inclusive. */
    private final long to;

    /**
     * Creates a new {@code NumberSequenceSource} that produces parallel sequences covering the
     * range {@code from} to {@code to} (both boundaries are inclusive).
     */
    public NumberSequenceSource(long from, long to) {
        checkArgument(from <= to, "'from' must be <= 'to'");
        this.from = from;
        this.to = to;
    }

    public long getFrom() {
        return from;
    }

    public long getTo() {
        return to;
    }

    // ------------------------------------------------------------------------
    //  source methods
    // ------------------------------------------------------------------------

    @Override
    public TypeInformation<Long> getProducedType() {
        return Types.LONG;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<Long, NumberSequenceSplit<Long>> createReader(
            SourceReaderContext readerContext) {
        return new IteratorSourceReader<>(readerContext);
    }

    @Override
    public SplitEnumerator<NumberSequenceSplit<Long>, Collection<NumberSequenceSplit<Long>>>
            createEnumerator(final SplitEnumeratorContext<NumberSequenceSplit<Long>> enumContext) {

        final List<NumberSequenceSplit<Long>> splits =
                splitNumberRange(from, to, enumContext.currentParallelism());
        return new IteratorSourceEnumerator<>(enumContext, splits);
    }

    @Override
    public SplitEnumerator<NumberSequenceSplit<Long>, Collection<NumberSequenceSplit<Long>>>
            restoreEnumerator(
                    final SplitEnumeratorContext<NumberSequenceSplit<Long>> enumContext,
                    Collection<NumberSequenceSplit<Long>> checkpoint) {
        return new IteratorSourceEnumerator<>(enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<NumberSequenceSplit<Long>> getSplitSerializer() {
        return new SplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<NumberSequenceSplit<Long>>>
            getEnumeratorCheckpointSerializer() {
        return new CheckpointSerializer();
    }

    protected List<NumberSequenceSplit<Long>> splitNumberRange(long from, long to, int numSplits) {
        final NumberSequenceIterator[] subSequences =
                new NumberSequenceIterator(from, to).split(numSplits);
        final ArrayList<NumberSequenceSplit<Long>> splits = new ArrayList<>(subSequences.length);

        int splitId = 1;
        for (NumberSequenceIterator seq : subSequences) {
            if (seq.hasNext()) {
                splits.add(
                        new NumberSequenceSplit<>(
                                String.valueOf(splitId++),
                                seq.getCurrent(),
                                seq.getTo(),
                                MapFunction.identity()));
            }
        }

        return splits;
    }

    // ------------------------------------------------------------------------
    //  splits & checkpoint
    // ------------------------------------------------------------------------

    private static final class SplitSerializer
            implements SimpleVersionedSerializer<NumberSequenceSplit<Long>> {

        private static final int CURRENT_VERSION = 1;

        @Override
        public int getVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public byte[] serialize(NumberSequenceSplit<Long> split) throws IOException {
            checkArgument(
                    split.getClass() == NumberSequenceSplit.class, "cannot serialize subclasses");

            // We will serialize 2 longs (16 bytes) plus the UFT representation of the string (2 +
            // length)
            final DataOutputSerializer out =
                    new DataOutputSerializer(split.splitId().length() + 18);
            serializeV1(out, split);
            return out.getCopyOfBuffer();
        }

        @Override
        public NumberSequenceSplit<Long> deserialize(int version, byte[] serialized)
                throws IOException {
            if (version != CURRENT_VERSION) {
                throw new IOException("Unrecognized version: " + version);
            }
            final DataInputDeserializer in = new DataInputDeserializer(serialized);
            return deserializeV1(in);
        }

        static void serializeV1(DataOutputView out, NumberSequenceSplit<Long> split)
                throws IOException {
            out.writeUTF(split.splitId());
            out.writeLong(split.from());
            out.writeLong(split.to());
        }

        static NumberSequenceSplit<Long> deserializeV1(DataInputView in) throws IOException {
            return new NumberSequenceSplit<>(
                    in.readUTF(), in.readLong(), in.readLong(), MapFunction.identity());
        }
    }

    private static final class CheckpointSerializer
            implements SimpleVersionedSerializer<Collection<NumberSequenceSplit<Long>>> {

        private static final int CURRENT_VERSION = 1;

        @Override
        public int getVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public byte[] serialize(Collection<NumberSequenceSplit<Long>> checkpoint)
                throws IOException {
            // Each split needs 2 longs (16 bytes) plus the UFT representation of the string (2 +
            // length)
            // Assuming at most 4 digit split IDs, 22 bytes per split avoids any intermediate array
            // resizing.
            // plus four bytes for the length field
            final DataOutputSerializer out = new DataOutputSerializer(checkpoint.size() * 22 + 4);
            out.writeInt(checkpoint.size());
            for (NumberSequenceSplit<Long> split : checkpoint) {
                SplitSerializer.serializeV1(out, split);
            }
            return out.getCopyOfBuffer();
        }

        @Override
        public Collection<NumberSequenceSplit<Long>> deserialize(int version, byte[] serialized)
                throws IOException {
            if (version != CURRENT_VERSION) {
                throw new IOException("Unrecognized version: " + version);
            }
            final DataInputDeserializer in = new DataInputDeserializer(serialized);
            final int num = in.readInt();
            final ArrayList<NumberSequenceSplit<Long>> result = new ArrayList<>(num);
            for (int remaining = num; remaining > 0; remaining--) {
                result.add(SplitSerializer.deserializeV1(in));
            }
            return result;
        }
    }
}
