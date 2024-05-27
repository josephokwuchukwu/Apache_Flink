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

package org.apache.flink.datastream.impl.context;

import org.apache.flink.api.common.WatermarkManager;
import org.apache.flink.api.common.eventtime.GenericWatermark;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.WatermarkEvent;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class DefaultWatermarkManager implements WatermarkManager {

    private final Output<?> streamRecordOutput;
    public DefaultWatermarkManager(Output<?> streamRecordOutput) {
        this.streamRecordOutput = streamRecordOutput;
    }

    @Override
    public void emitWatermark(GenericWatermark watermark) {
        this.streamRecordOutput.emitWatermark(new WatermarkEvent(watermark));
    }
}
