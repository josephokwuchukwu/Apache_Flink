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

package org.apache.flink.runtime.scheduler.loading;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link DefaultLoadingWeight}. */
class DefaultLoadingWeightTest {

    @Test
    void testInvalidLoading() {
        assertThatThrownBy(() -> new DefaultLoadingWeight(-1f))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testMerge() {
        assertThat(new DefaultLoadingWeight(0).merge(null).getLoading()).isZero();
        assertThat(new DefaultLoadingWeight(0).merge(new DefaultLoadingWeight(1.2f)).getLoading())
                .isEqualTo(1.2f);
    }
}
