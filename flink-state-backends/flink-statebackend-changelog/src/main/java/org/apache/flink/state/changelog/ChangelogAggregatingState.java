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

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalKvState;

import java.util.Collection;

/**
 * Delegated partitioned {@link AggregatingState} that forwards changes to {@link StateChange} upon
 * {@link AggregatingState} is updated.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <IN> The type of the value added to the state.
 * @param <ACC> The type of the value stored in the state (the accumulator type).
 * @param <OUT> The type of the value returned from the state.
 */
class ChangelogAggregatingState<K, N, IN, ACC, OUT>
        extends AbstractChangelogState<K, N, ACC, InternalAggregatingState<K, N, IN, ACC, OUT>>
        implements InternalAggregatingState<K, N, IN, ACC, OUT> {

    ChangelogAggregatingState(InternalAggregatingState<K, N, IN, ACC, OUT> delegatedState) {
        super(delegatedState);
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        delegatedState.mergeNamespaces(target, sources);
    }

    @Override
    public ACC getInternal() throws Exception {
        return delegatedState.getInternal();
    }

    @Override
    public void updateInternal(ACC valueToStore) throws Exception {
        delegatedState.updateInternal(valueToStore);
    }

    @Override
    public OUT get() throws Exception {
        return delegatedState.get();
    }

    @Override
    public void add(IN value) throws Exception {
        delegatedState.add(value);
    }

    @Override
    public void clear() {
        delegatedState.clear();
    }

    @SuppressWarnings("unchecked")
    static <T, K, N, SV, S extends State, IS extends S> IS create(
            InternalKvState<K, N, SV> aggregatingState) {
        return (IS)
                new ChangelogAggregatingState<>(
                        (InternalAggregatingState<K, N, T, SV, ?>) aggregatingState);
    }
}
