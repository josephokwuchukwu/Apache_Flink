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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilityContext;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.abilities.source.WatermarkPushDownSpec;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.time.Duration;

/**
 * Base rule for interface {@link SupportsWatermarkPushDown}. It offers a util to push the {@link
 * FlinkLogicalWatermarkAssigner} into the {@link FlinkLogicalTableSourceScan}.
 */
public abstract class PushWatermarkIntoTableSourceScanRuleBase extends RelOptRule {

    public PushWatermarkIntoTableSourceScanRuleBase(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    /**
     * It uses the input watermark expression to generate the {@link WatermarkGeneratorSupplier}.
     * After the {@link WatermarkStrategy} is pushed into the scan, it will build a new scan.
     * However, when {@link FlinkLogicalWatermarkAssigner} is the parent of the {@link
     * FlinkLogicalTableSourceScan} it should modify the rowtime type to keep the type of plan is
     * consistent. In other cases, it just keep the data type of the scan as same as before and
     * leave the work when rewriting the projection.
     *
     * <p>NOTES: the row type of the scan is not always as same as the watermark assigner. Because
     * the scan will not add the rowtime column into the row when pushing the watermark assigner
     * into the scan. In some cases, query may have computed columns defined on rowtime column. If
     * modifying the type of the rowtime(with time attribute), it will also influence the type of
     * the computed column. Therefore, if the watermark assigner is not the parent of the scan, set
     * the type of the scan as before and leave the work to projection.
     */
    protected FlinkLogicalTableSourceScan getNewScan(
            FlinkLogicalWatermarkAssigner watermarkAssigner,
            RexNode watermarkExpr,
            FlinkLogicalTableSourceScan scan,
            TableConfig tableConfig,
            boolean useWatermarkAssignerRowType) {
        String digest = String.format("watermark=[%s]", watermarkExpr);
        Duration idleTimeout =
                tableConfig
                        .getConfiguration()
                        .get(ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT);
        final long idleTimeoutMillis;
        if (!idleTimeout.isZero() && !idleTimeout.isNegative()) {
            idleTimeoutMillis = idleTimeout.toMillis();
            digest = String.format("%s, idletimeout=[%s]", digest, idleTimeoutMillis);
        } else {
            idleTimeoutMillis = -1L;
        }

        TableSourceTable tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
        DynamicTableSource newDynamicTableSource = tableSourceTable.tableSource().copy();

        final RelDataType newType;
        if (useWatermarkAssignerRowType) {
            // project is trivial and set rowtime type in scan
            newType = watermarkAssigner.getRowType();
        } else {
            // project add/delete columns and set the rowtime column type in project
            newType = scan.getRowType();
        }

        WatermarkPushDownSpec watermarkPushDownSpec =
                new WatermarkPushDownSpec(
                        watermarkExpr,
                        idleTimeoutMillis,
                        (RowType) FlinkTypeFactory.toLogicalType(newType));
        watermarkPushDownSpec.apply(newDynamicTableSource, SourceAbilityContext.from(scan));
        TableSourceTable newTableSourceTable =
                tableSourceTable.copy(
                        newDynamicTableSource,
                        newType,
                        new String[] {digest},
                        new SourceAbilitySpec[] {watermarkPushDownSpec});
        return FlinkLogicalTableSourceScan.create(scan.getCluster(), newTableSourceTable);
    }

    protected boolean supportsWatermarkPushDown(FlinkLogicalTableSourceScan scan) {
        TableSourceTable tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
        return tableSourceTable != null
                && tableSourceTable.tableSource() instanceof SupportsWatermarkPushDown;
    }
}
