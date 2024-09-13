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

package org.apache.flink.runtime.state.v2;

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;

import java.util.Collection;

/**
 * This class defines the internal interface for merging state.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <V> The type of values kept internally in state
 */
public abstract class InternalMergingState<K, N, V> extends InternalKeyedState<K, N, V> {

    public InternalMergingState(
            StateRequestHandler stateRequestHandler, StateDescriptor<V> stateDescriptor) {
        super(stateRequestHandler, stateDescriptor);
    }

    /**
     * Merges the state of the current key for the given source namespaces into the state of the
     * target namespace.
     *
     * @param target The target namespace where the merged state should be stored.
     * @param sources The source namespaces whose state should be merged.
     * @throws Exception The method may forward exception thrown internally (by I/O or functions).
     */
    public abstract StateFuture<Void> asyncMergeNamespaces(N target, Collection<N> sources)
            throws Exception;

    public abstract void mergeNamespaces(N target, Collection<N> sources);
}
