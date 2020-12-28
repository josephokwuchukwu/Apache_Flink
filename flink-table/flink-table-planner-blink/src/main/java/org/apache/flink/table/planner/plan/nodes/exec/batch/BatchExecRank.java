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
 *
 */

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.operators.sort.RankOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.stream.IntStream;

/**
 * Batch physical ExecNode for Rank.
 *
 * <p>This node supports two-stage(local and global) rank to reduce data-shuffling.
 */
public class BatchExecRank extends ExecNodeBase<RowData> implements BatchExecNode<RowData> {

    private final int[] partitionFields;
    private final int[] sortFields;
    private final long rankStart;
    private final long rankEnd;
    private final boolean outputRankNumber;

    public BatchExecRank(
            int[] partitionFields,
            int[] sortFields,
            long rankStart,
            long rankEnd,
            boolean outputRankNumber,
            ExecEdge inputEdge,
            LogicalType outputType,
            String description) {
        super(Collections.singletonList(inputEdge), outputType, description);
        this.partitionFields = partitionFields;
        this.sortFields = sortFields;
        this.rankStart = rankStart;
        this.rankEnd = rankEnd;
        this.outputRankNumber = outputRankNumber;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {

        Transformation<RowData> input =
                (Transformation<RowData>) getInputNodes().get(0).translateToPlan(planner);

        RowType outputType = (RowType) getOutputType();
        RowType inputType = (RowType) getInputNodes().get(0).getOutputType();

        LogicalType[] partitionTypes =
                IntStream.of(partitionFields)
                        .mapToObj(inputType::getTypeAt)
                        .toArray(LogicalType[]::new);
        LogicalType[] sortTypes =
                IntStream.of(sortFields).mapToObj(inputType::getTypeAt).toArray(LogicalType[]::new);

        // operator needn't cache data
        // The collation for the partition-by and order-by fields is inessential here,
        // we only use the comparator to distinguish fields change.
        RankOperator operator =
                new RankOperator(
                        ComparatorCodeGenerator.gen(
                                planner.getTableConfig(),
                                "PartitionByComparator",
                                partitionFields,
                                partitionTypes,
                                new boolean[partitionFields.length],
                                new boolean[partitionFields.length]),
                        ComparatorCodeGenerator.gen(
                                planner.getTableConfig(),
                                "OrderByComparator",
                                sortFields,
                                sortTypes,
                                new boolean[sortFields.length],
                                new boolean[sortFields.length]),
                        rankStart,
                        rankEnd,
                        outputRankNumber);

        OneInputTransformation<RowData, RowData> ret =
                ExecNodeUtil.createOneInputTransformation(
                        input,
                        getDesc(),
                        SimpleOperatorFactory.of(operator),
                        InternalTypeInfo.of(outputType),
                        input.getParallelism(),
                        0);
        if (inputsContainSingleton()) {
            ret.setParallelism(1);
            ret.setMaxParallelism(1);
        }
        return ret;
    }
}
