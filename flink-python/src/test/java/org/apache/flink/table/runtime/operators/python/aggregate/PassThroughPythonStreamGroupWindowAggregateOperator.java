/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.python.aggregate;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerServiceImpl;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.UpdatableRowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.functions.python.PythonAggregateFunctionInfo;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.typeutils.DataViewUtils;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner;
import org.apache.flink.table.runtime.utils.PassThroughStreamGroupWindowAggregatePythonFunctionRunner;
import org.apache.flink.table.runtime.utils.PythonTestUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.stream.Collectors;

/** PassThroughPythonStreamGroupWindowAggregateOperator. */
public class PassThroughPythonStreamGroupWindowAggregateOperator<K>
        extends PythonStreamGroupWindowAggregateOperator<K, TimeWindow> {

    private final MockPythonWindowOperator<K> mockPythonWindowOperator;
    private final int[] grouping;
    private final PythonAggregateFunctionInfo aggregateFunction;
    private final int[] namedProperties;
    private InternalTimerServiceImpl<K, TimeWindow> mockPythonInternalService;
    private Map<String, Map<TimeWindow, List<RowData>>> windowAccumulateData;
    private Map<String, Map<TimeWindow, List<RowData>>> windowRetractData;
    private transient UpdatableRowData reusePythonRowData;

    private transient UpdatableRowData reusePythonTimerRowData;
    private transient UpdatableRowData reusePythonTimerData;
    private transient LinkedBlockingQueue<byte[]> resultBuffer;
    private Projection<RowData, BinaryRowData> groupKeyProjection;
    private Function<RowData, RowData> aggExtracter;
    private Function<TimeWindow, RowData> windowExtractor;
    private JoinedRowData reuseJoinedRow;
    private JoinedRowData windowAggResult;

    public PassThroughPythonStreamGroupWindowAggregateOperator(
            Configuration config,
            RowType inputType,
            RowType outputType,
            PythonAggregateFunctionInfo[] aggregateFunctions,
            int[] grouping,
            int indexOfCountStar,
            boolean generateUpdateBefore,
            boolean countStarInserted,
            int inputTimeFieldIndex,
            WindowAssigner<TimeWindow> windowAssigner,
            LogicalWindow window,
            int[] namedProperties) {
        super(
                config,
                inputType,
                outputType,
                aggregateFunctions,
                new DataViewUtils.DataViewSpec[0][0],
                grouping,
                indexOfCountStar,
                generateUpdateBefore,
                countStarInserted,
                inputTimeFieldIndex,
                windowAssigner,
                window,
                namedProperties);
        this.mockPythonWindowOperator = new MockPythonWindowOperator<>();
        this.aggregateFunction = aggregateFunctions[0];
        this.namedProperties = namedProperties;
        this.grouping = grouping;
    }

    @Override
    public void open() throws Exception {
        super.open();
        reusePythonRowData = new UpdatableRowData(GenericRowData.of(NORMAL_RECORD, null, null), 3);
        reusePythonTimerRowData =
                new UpdatableRowData(GenericRowData.of(TRIGGER_TIMER, null, null), 3);
        reusePythonTimerData =
                new UpdatableRowData(GenericRowData.of(0, null, null, null, null), 5);
        reuseJoinedRow = new JoinedRowData();
        windowAggResult = new JoinedRowData();
        reusePythonTimerRowData.setField(2, reusePythonTimerData);
        windowAccumulateData = new HashMap<>();
        windowRetractData = new HashMap<>();
        mockPythonInternalService =
                (InternalTimerServiceImpl<K, TimeWindow>)
                        getInternalTimerService(
                                "python-window-timers",
                                windowSerializer,
                                this.mockPythonWindowOperator);
        this.groupKeyProjection = createProjection("GroupKey", grouping);
        int inputFieldIndex = (int) aggregateFunction.getInputs()[0];
        this.aggExtracter =
                input -> {
                    GenericRowData aggResult = new GenericRowData(1);
                    aggResult.setField(0, input.getLong(inputFieldIndex));
                    return aggResult;
                };
        this.windowExtractor =
                window -> {
                    GenericRowData windowProperty = new GenericRowData(namedProperties.length);
                    for (int i = 0; i < namedProperties.length; i++) {
                        switch (namedProperties[i]) {
                            case 0:
                                windowProperty.setField(
                                        i, TimestampData.fromEpochMillis(window.getStart()));
                                break;
                            case 1:
                                windowProperty.setField(
                                        i, TimestampData.fromEpochMillis(window.getEnd()));
                                break;
                            case 2:
                                windowProperty.setField(
                                        i, TimestampData.fromEpochMillis(window.getEnd() - 1));
                                break;
                            case 3:
                                windowProperty.setField(i, TimestampData.fromEpochMillis(-1));
                        }
                    }
                    return windowProperty;
                };
    }

    @Override
    public PythonFunctionRunner createPythonFunctionRunner() throws Exception {
        return new PassThroughStreamGroupWindowAggregatePythonFunctionRunner(
                getRuntimeContext().getTaskName(),
                PythonTestUtils.createTestEnvironmentManager(),
                userDefinedFunctionInputType,
                userDefinedFunctionOutputType,
                STREAM_GROUP_WINDOW_AGGREGATE_URN,
                getUserDefinedFunctionsProto(),
                FLINK_AGGREGATE_FUNCTION_SCHEMA_CODER_URN,
                new HashMap<>(),
                PythonTestUtils.createMockFlinkMetricContainer(),
                getKeyedStateBackend(),
                getKeySerializer(),
                this);
    }

    public void processPythonElement(byte[] inputBytes) {
        try {
            RowData input =
                    udfInputTypeSerializer.deserialize(new DataInputDeserializer(inputBytes));
            if (input.getByte(0) == NORMAL_RECORD) {
                // normal data
                RowData inputRow = input.getRow(1, inputType.getFieldCount());
                BinaryRowData key = groupKeyProjection.apply(inputRow).copy();
                Map<TimeWindow, List<RowData>> curKeyWindowAccumulateData =
                        windowAccumulateData.computeIfAbsent(
                                key.getString(0).toString(), k -> new HashMap<>());
                Map<TimeWindow, List<RowData>> curKeyWindowRetractData =
                        windowRetractData.computeIfAbsent(
                                key.getString(0).toString(), k -> new HashMap<>());

                long watermark = input.getLong(2);
                // advance watermark
                mockPythonInternalService.advanceWatermark(watermark);

                // get timestamp
                long timestamp = inputRow.getLong(inputTimeFieldIndex);
                Collection<TimeWindow> elementWindows =
                        windowAssigner.assignWindows(inputRow, timestamp);
                for (TimeWindow window : elementWindows) {
                    if (RowDataUtil.isAccumulateMsg(inputRow)) {
                        List<RowData> currentWindowDatas =
                                curKeyWindowAccumulateData.computeIfAbsent(
                                        window, k -> new LinkedList<>());
                        currentWindowDatas.add(inputRow);
                    } else {
                        List<RowData> currentWindowDatas =
                                curKeyWindowRetractData.computeIfAbsent(
                                        window, k -> new LinkedList<>());
                        currentWindowDatas.add(inputRow);
                    }
                }
                List<TimeWindow> actualWindows = new ArrayList<>(elementWindows.size());
                for (TimeWindow window : elementWindows) {
                    if (!isWindowLate(window)) {
                        actualWindows.add(window);
                    }
                }
                for (TimeWindow window : actualWindows) {
                    boolean triggerResult = onElement(key, window);
                    if (triggerResult) {
                        triggerWindowProcess(key, window);
                    }
                    // register a clean up timer for the window
                    registerCleanupTimer(key, window);
                }
            } else {
                RowData timerData = input.getRow(3, 4);
                long timestamp = input.getLong(2);
                RowData key = timerData.getRow(1, getKeyType().getFieldCount());
                long start = timerData.getLong(2);
                long end = timerData.getLong(3);
                TimeWindow window = TimeWindow.of(start, end);
                if (timestamp == window.maxTimestamp()) {
                    triggerWindowProcess(key, window);
                }
                cleanWindowIfNeeded(key, window, timestamp);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setResultBuffer(LinkedBlockingQueue<byte[]> resultBuffer) {
        this.resultBuffer = resultBuffer;
    }

    private boolean isWindowLate(TimeWindow window) {
        return windowAssigner.isEventTime()
                && (cleanupTime(window) <= mockPythonInternalService.currentWatermark());
    }

    private long cleanupTime(TimeWindow window) {
        if (windowAssigner.isEventTime()) {
            long cleanupTime = window.maxTimestamp();
            return cleanupTime >= window.maxTimestamp() ? cleanupTime : Long.MAX_VALUE;
        } else {
            return window.maxTimestamp();
        }
    }

    private boolean onElement(BinaryRowData key, TimeWindow window) throws IOException {
        if (window.maxTimestamp() <= mockPythonInternalService.currentWatermark()) {
            return true;
        } else {
            if (windowAssigner.isEventTime()) {
                registerEventTimeTimer(key, window);
            } else {
                registerProcessingTimeTimer(key, window);
            }
            return false;
        }
    }

    private void triggerWindowProcess(RowData key, TimeWindow window) throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(1);
        Iterable<RowData> currentWindowAccumulateData =
                windowAccumulateData.get(key.getString(0).toString()).get(window);
        Iterable<RowData> currentWindowRetractData =
                windowRetractData.get(key.getString(0).toString()).get(window);
        if (currentWindowAccumulateData != null) {
            for (RowData accumulateData : currentWindowAccumulateData) {
                if (!hasRetractData(accumulateData, currentWindowRetractData)) {
                    // only output first value in group window.
                    RowData aggResult = aggExtracter.apply(accumulateData);
                    RowData windowProperty = windowExtractor.apply(window);
                    windowAggResult.replace(key, aggResult);
                    reuseJoinedRow.replace(windowAggResult, windowProperty);
                    reusePythonRowData.setField(1, reuseJoinedRow);
                    udfOutputTypeSerializer.serialize(reusePythonRowData, output);
                    resultBuffer.add(output.getCopyOfBuffer());
                    break;
                }
            }
        }
    }

    private boolean hasRetractData(
            RowData accumulateData, Iterable<RowData> currentWindowRetractData) {
        if (currentWindowRetractData != null) {
            for (RowData retractData : currentWindowRetractData) {
                if (retractData.getRowKind() == RowKind.UPDATE_BEFORE) {
                    retractData.setRowKind(RowKind.UPDATE_AFTER);
                } else {
                    retractData.setRowKind(RowKind.INSERT);
                }
                if (accumulateData.equals(retractData)) {
                    return true;
                }
            }
        }
        return false;
    }

    private Projection<RowData, BinaryRowData> createProjection(String name, int[] fields) {
        final RowType forwardedFieldType =
                new RowType(
                        Arrays.stream(fields)
                                .mapToObj(i -> inputType.getFields().get(i))
                                .collect(Collectors.toList()));
        final GeneratedProjection generatedProjection =
                ProjectionCodeGenerator.generateProjection(
                        CodeGeneratorContext.apply(new TableConfig()),
                        name,
                        inputType,
                        forwardedFieldType,
                        fields);
        // noinspection unchecked
        return generatedProjection.newInstance(Thread.currentThread().getContextClassLoader());
    }

    private void registerCleanupTimer(RowData key, TimeWindow window) throws IOException {
        long cleanupTime = cleanupTime(window);
        if (cleanupTime == Long.MAX_VALUE) {
            // don't set a GC timer for "end of time"
            return;
        }
        if (windowAssigner.isEventTime()) {
            registerEventTimeTimer(key, window);
        } else {
            registerProcessingTimeTimer(key, window);
        }
    }

    private void registerEventTimeTimer(RowData key, TimeWindow window) throws IOException {
        reusePythonTimerData.setByte(
                0, PythonStreamGroupWindowAggregateOperator.REGISTER_EVENT_TIMER);
        reusePythonTimerData.setField(1, key);
        reusePythonTimerData.setLong(2, window.maxTimestamp());
        reusePythonTimerData.setLong(3, window.getStart());
        reusePythonTimerData.setLong(4, window.getEnd());
        DataOutputSerializer output = new DataOutputSerializer(1);
        udfOutputTypeSerializer.serialize(reusePythonTimerRowData, output);
        resultBuffer.add(output.getCopyOfBuffer());
    }

    private void deleteEventTimeTimer(RowData key, TimeWindow window) throws IOException {
        reusePythonTimerData.setByte(
                0, PythonStreamGroupWindowAggregateOperator.DELETE_EVENT_TIMER);
        reusePythonTimerData.setField(1, key);
        reusePythonTimerData.setLong(2, window.maxTimestamp());
        reusePythonTimerData.setLong(3, window.getStart());
        reusePythonTimerData.setLong(4, window.getEnd());
        DataOutputSerializer output = new DataOutputSerializer(1);
        udfOutputTypeSerializer.serialize(reusePythonTimerRowData, output);
        resultBuffer.add(output.getCopyOfBuffer());
    }

    private void registerProcessingTimeTimer(RowData key, TimeWindow window) throws IOException {
        reusePythonTimerData.setByte(
                0, PythonStreamGroupWindowAggregateOperator.REGISTER_PROCESSING_TIMER);
        reusePythonTimerData.setField(1, key);
        reusePythonTimerData.setLong(2, window.maxTimestamp());
        reusePythonTimerData.setLong(3, window.getStart());
        reusePythonTimerData.setLong(4, window.getEnd());
        DataOutputSerializer output = new DataOutputSerializer(1);
        udfOutputTypeSerializer.serialize(reusePythonTimerRowData, output);
        resultBuffer.add(output.getCopyOfBuffer());
    }

    private void deleteProcessingTimeTimer(RowData key, TimeWindow window) throws IOException {
        reusePythonTimerData.setByte(
                0, PythonStreamGroupWindowAggregateOperator.DELETE_PROCESSING_TIMER);
        reusePythonTimerData.setField(1, key);
        reusePythonTimerData.setLong(2, window.maxTimestamp());
        reusePythonTimerData.setLong(3, window.getStart());
        reusePythonTimerData.setLong(4, window.getEnd());
        DataOutputSerializer output = new DataOutputSerializer(1);
        udfOutputTypeSerializer.serialize(reusePythonTimerRowData, output);
        resultBuffer.add(output.getCopyOfBuffer());
    }

    private void cleanWindowIfNeeded(RowData key, TimeWindow window, long currentTime)
            throws IOException {
        if (currentTime == cleanupTime(window)) {
            // 1. delete state
            // only output first value in group window.
            DataOutputSerializer output = new DataOutputSerializer(1);
            RowData windowProperty = windowExtractor.apply(window);
            windowAggResult.replace(
                    GenericRowData.of(
                            StringData.fromString(
                                    "state_cleanup_triggered: "
                                            + key.getString(0).toString()
                                            + " : "
                                            + window)),
                    GenericRowData.of(0L));
            reuseJoinedRow.replace(windowAggResult, windowProperty);
            reusePythonRowData.setField(1, reuseJoinedRow);
            udfOutputTypeSerializer.serialize(reusePythonRowData, output);
            resultBuffer.add(output.getCopyOfBuffer());
            // 2. delete window timer
            if (windowAssigner.isEventTime()) {
                deleteEventTimeTimer(key, window);
            } else {
                deleteProcessingTimeTimer(key, window);
            }
        }
    }

    private static class MockPythonWindowOperator<K> implements Triggerable<K, TimeWindow> {

        MockPythonWindowOperator() {}

        @Override
        public void onEventTime(InternalTimer<K, TimeWindow> timer) throws Exception {}

        @Override
        public void onProcessingTime(InternalTimer<K, TimeWindow> timer) throws Exception {}
    }
}
