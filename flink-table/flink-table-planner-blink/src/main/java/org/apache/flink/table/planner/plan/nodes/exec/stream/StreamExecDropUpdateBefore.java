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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.StreamFilter;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.runtime.operators.misc.DropUpdateBeforeFunction;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;

/**
 * Stream {@link ExecNode} which will drop the UPDATE_BEFORE messages. This is usually used as an
 * optimization for the downstream operators that doesn't need the UPDATE_BEFORE messages, but the
 * upstream operator can't drop it by itself (e.g. the source).
 */
public class StreamExecDropUpdateBefore extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData> {

    public StreamExecDropUpdateBefore(ExecEdge inputEdge, RowType outputType, String description) {
        super(Collections.singletonList(inputEdge), outputType, description);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) getInputNodes().get(0).translateToPlan(planner);
        final StreamFilter<RowData> operator = new StreamFilter<>(new DropUpdateBeforeFunction());

        return new OneInputTransformation<>(
                inputTransform,
                getDesc(),
                operator,
                inputTransform.getOutputType(),
                inputTransform.getParallelism());
    }
}
