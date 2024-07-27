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

package org.apache.flink.runtime.state.v2;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MapStateDescriptor}. */
public class MapStateDescriptorTest {

    @Test
    void testHashCodeAndEquals() throws Exception {
        final String name = "testName";

        MapStateDescriptor<String, Integer> original =
                new MapStateDescriptor<>(
                        name, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
        MapStateDescriptor<String, Integer> same =
                new MapStateDescriptor<>(
                        name, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
        MapStateDescriptor<String, Integer> sameBySerializer =
                new MapStateDescriptor<>(
                        name, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

        // test that hashCode() works on state descriptors with initialized and uninitialized
        // serializers
        assertThat(same).hasSameHashCodeAs(original);
        assertThat(sameBySerializer).hasSameHashCodeAs(original);

        assertThat(same).isEqualTo(original);
        assertThat(sameBySerializer).isEqualTo(original);

        // equality with a clone
        MapStateDescriptor<String, Integer> clone =
                CommonTestUtils.createCopySerializable(original);
        assertThat(clone).isEqualTo(original);
    }
}
