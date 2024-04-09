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

package org.apache.flink.datastream.impl.operators;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.datastream.api.context.ProcessingTimeManager;
import org.apache.flink.datastream.api.function.TwoInputBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.impl.common.KeyCheckedOutputCollector;
import org.apache.flink.datastream.impl.common.OutputCollector;
import org.apache.flink.datastream.impl.common.TimestampCollector;
import org.apache.flink.datastream.impl.context.DefaultProcessingTimeManager;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;

import javax.annotation.Nullable;

/** Operator for {@link TwoInputBroadcastStreamProcessFunction} in {@link KeyedPartitionStream}. */
public class KeyedTwoInputBroadcastProcessOperator<KEY, IN1, IN2, OUT>
        extends TwoInputBroadcastProcessOperator<IN1, IN2, OUT>
        implements Triggerable<KEY, VoidNamespace> {
    private transient InternalTimerService<VoidNamespace> timerService;

    @Nullable private final KeySelector<OUT, KEY> outKeySelector;

    public KeyedTwoInputBroadcastProcessOperator(
            TwoInputBroadcastStreamProcessFunction<IN1, IN2, OUT> userFunction) {
        this(userFunction, null);
    }

    public KeyedTwoInputBroadcastProcessOperator(
            TwoInputBroadcastStreamProcessFunction<IN1, IN2, OUT> userFunction,
            @Nullable KeySelector<OUT, KEY> outKeySelector) {
        super(userFunction);
        this.outKeySelector = outKeySelector;
    }

    @Override
    public void open() throws Exception {
        this.timerService =
                getInternalTimerService("processing timer", VoidNamespaceSerializer.INSTANCE, this);
        super.open();
    }

    @Override
    protected TimestampCollector<OUT> getOutputCollector() {
        return outKeySelector == null
                ? new OutputCollector<>(output)
                : new KeyCheckedOutputCollector<>(
                        new OutputCollector<>(output), outKeySelector, () -> (KEY) getCurrentKey());
    }

    @Override
    protected Object currentKey() {
        return getCurrentKey();
    }

    protected ProcessingTimeManager getProcessingTimeManager() {
        return new DefaultProcessingTimeManager(timerService);
    }

    @Override
    public void onEventTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
        // do nothing at the moment.
    }

    @Override
    public void onProcessingTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
        // align the key context with the registered timer.
        partitionedContext
                .getStateManager()
                .executeInKeyContext(
                        () ->
                                userFunction.onProcessingTimer(
                                        timer.getTimestamp(),
                                        getOutputCollector(),
                                        partitionedContext),
                        timer.getKey());
    }
}
