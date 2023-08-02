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

package org.apache.flink.test.flush.util;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * An aggregation operator with user-defined flush function.
 *
 * <p>The operator will buffer inputs in memory until a flush operation is triggered.
 */
public class FlushAggregator extends AbstractStreamOperator<Tuple2<Integer, Long>>
        implements OneInputStreamOperator<Integer, Tuple2<Integer, Long>> {
    private Map<Integer, Long> bundle;
    private final KeySelector<Integer, Integer> keySelector;
    private int numOfElements;
    private ValueState<Long> store;
    private long visits;

    public FlushAggregator(KeySelector<Integer, Integer> keySelector) {
        super();
        this.keySelector = keySelector;
    }

    private Integer getKey(Integer input) throws Exception {
        return keySelector.getKey(input);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        store =
                context.getKeyedStateStore()
                        .getState(new ValueStateDescriptor<>("store", Long.class));
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.bundle = new HashMap<>();
        visits = 0;
        numOfElements = 0;

        // counter metric to get the size of bundle
        getRuntimeContext()
                .getMetricGroup()
                .gauge("bundleSize", (Gauge<Integer>) () -> numOfElements);
    }

    @Override
    public void processElement(StreamRecord<Integer> element) throws Exception {
        Integer input = element.getValue();
        if (getExecutionConfig().getAllowedLatency() > 0) {
            Long bundleValue = bundle.get(input);
            // get a new value after adding this element to bundle
            Long newBundleValue = bundleValue == null ? 1 : bundleValue + 1;
            bundle.put(input, newBundleValue);
        } else {
            visits += 1;
            Long storeValue = store.value();
            Long newStoreValue = storeValue == null ? 1 : storeValue + 1;
            store.update(newStoreValue);
            output.collect(new StreamRecord<>(new Tuple2<>(input, newStoreValue)));
        }
    }

    @Override
    public void finish() throws Exception {
        finishBundle();
        System.out.println("RocksDB visits: " + visits);
    }

    public void finishBundle() {
        if (bundle != null && !bundle.isEmpty()) {
            finishBundle(bundle, output);
            bundle.clear();
        }
    }

    public void finishBundle(
            Map<Integer, Long> bundle, Output<StreamRecord<Tuple2<Integer, Long>>> output) {
        bundle.forEach(
                (k, v) -> {
                    try {
                        visits += 1;
                        setKeyContextElement1(new StreamRecord<>(k));
                        Long storeValue = store.value();
                        Long newStoreValue = storeValue == null ? v : storeValue + v;
                        store.update(newStoreValue);
                        output.collect(new StreamRecord<>(new Tuple2<>(k, newStoreValue)));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Override
    public void flush() {
        finishBundle();
    }
}
