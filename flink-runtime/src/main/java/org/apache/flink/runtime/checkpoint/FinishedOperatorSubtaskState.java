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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.state.StateObject;

import java.util.function.Function;

/**
 * A specialized {@link OperatorSubtaskState} representing the subtask is finished. It is also
 * read-only and could not have actual states.
 */
public class FinishedOperatorSubtaskState extends OperatorSubtaskState {

    public static final FinishedOperatorSubtaskState INSTANCE = new FinishedOperatorSubtaskState();

    private static final long serialVersionUID = 1L;

    /** Disallow creating new instances. */
    private FinishedOperatorSubtaskState() {}

    @Override
    public boolean isFinished() {
        return true;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof FinishedOperatorSubtaskState;
    }

    @Override
    public String toString() {
        return "FinishedOperatorSubtaskState{}";
    }

    @Override
    public StateObject transform(Function<StateObject, StateObject> transformation) {
        return transformation.apply(this);
    }
}
