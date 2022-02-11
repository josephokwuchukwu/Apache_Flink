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

package org.apache.flink.table.api.internal;

import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TablePipeline;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.operations.ModifyOperation;

import static java.util.Collections.singletonList;

/** Implementation of {@link TablePipeline}. */
class TablePipelineImpl implements TablePipeline {

    private final TableEnvironmentInternal tableEnvironment;
    private final ModifyOperation operation;

    TablePipelineImpl(TableEnvironmentInternal tableEnvironment, ModifyOperation operation) {
        this.tableEnvironment = tableEnvironment;
        this.operation = operation;
    }

    ModifyOperation getOperation() {
        return operation;
    }

    @Override
    public CompiledPlan compilePlan() throws TableException {
        return tableEnvironment.compilePlan(singletonList(operation));
    }

    @Override
    public TableResult execute() {
        return tableEnvironment.executeInternal(operation);
    }

    @Override
    public String explain(ExplainDetail... extraDetails) {
        return tableEnvironment.explainInternal(singletonList(operation), extraDetails);
    }
}
