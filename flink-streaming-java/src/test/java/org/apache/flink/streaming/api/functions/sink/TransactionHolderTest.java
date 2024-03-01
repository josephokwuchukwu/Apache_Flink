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

package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction.TransactionHolder;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests {@link TransactionHolder}. */
class TransactionHolderTest {

    @Test
    void testElapsedTime() {
        final long elapsedTime =
                new TransactionHolder<>(new Object(), 0)
                        .elapsedTime(Clock.fixed(Instant.ofEpochMilli(1000), ZoneOffset.UTC));
        assertThat(elapsedTime).isEqualTo(1000L);
    }
}
