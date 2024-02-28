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

package org.apache.flink.api.java.tuple;

import org.apache.flink.types.NullFieldException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

/** Tests for {@link Tuple2}. */
class Tuple2Test {

    @Test
    void testSwapValues() {
        Tuple2<String, Integer> toSwap = new Tuple2<>("Test case", 25);
        Tuple2<Integer, String> swapped = toSwap.swap();

        assertThat(toSwap.f1).isEqualTo(swapped.f0);

        assertThat(toSwap.f0).isEqualTo(swapped.f1);
    }

    @Test
    void testGetFieldNotNull() {
        assertThatExceptionOfType(NullFieldException.class)
                .isThrownBy(
                        () -> {
                            Tuple2<String, Integer> tuple = new Tuple2<>("Test case", null);

                            assertThat(tuple.getFieldNotNull(0)).isEqualTo("Test case");
                            tuple.getFieldNotNull(1);
                        });
    }
}
