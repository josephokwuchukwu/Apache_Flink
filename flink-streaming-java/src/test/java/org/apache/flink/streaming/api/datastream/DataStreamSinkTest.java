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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.runtime.operators.sink.TestSinkV2;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit test for {@link DataStreamSink}. */
class DataStreamSinkTest {

    @Test
    void testGettingTransformationWithNewSinkAPI() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final Transformation<?> transformation =
                env.fromData(1, 2)
                        .sinkTo(TestSinkV2.<Integer>newBuilder().build())
                        .getTransformation();
        assertThat(transformation).isInstanceOf(SinkTransformation.class);
    }

    @Test
    void throwExceptionWhenSetUidWithNewSinkAPI() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        assertThatThrownBy(
                        () ->
                                env.fromData(1, 2)
                                        .sinkTo(TestSinkV2.<Integer>newBuilder().build())
                                        .setUidHash("Test"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
