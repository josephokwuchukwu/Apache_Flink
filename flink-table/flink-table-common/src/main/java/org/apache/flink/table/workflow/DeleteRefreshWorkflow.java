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

package org.apache.flink.table.workflow;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.refresh.RefreshHandler;

/**
 * {@link DeleteRefreshWorkflow} provides the related information to delete refresh workflow of
 * {@link CatalogMaterializedTable}.
 *
 * @param <T> The type of {@link RefreshHandler} used by specific {@link WorkflowScheduler} to
 *     locate the refresh workflow in scheduler service.
 */
@PublicEvolving
public class DeleteRefreshWorkflow<T extends RefreshHandler> implements RefreshWorkflow {

    private final T refreshHandler;

    public DeleteRefreshWorkflow(T refreshHandler) {
        this.refreshHandler = refreshHandler;
    }

    /**
     * Return {@link RefreshHandler} from corresponding {@link WorkflowScheduler} which provides
     * meta info to points to the refresh workflow in scheduler service.
     */
    public T getRefreshHandler() {
        return refreshHandler;
    }
}
