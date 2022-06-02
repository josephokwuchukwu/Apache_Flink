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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.operations.QueryOperation;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** A view created from a {@link QueryOperation} via operations on {@link Table}. */
@Internal
public final class QueryOperationCatalogView implements CatalogView {

    private final QueryOperation queryOperation;

    public QueryOperationCatalogView(QueryOperation queryOperation) {
        this.queryOperation = queryOperation;
    }

    public QueryOperation getQueryOperation() {
        return queryOperation;
    }

    @Override
    public Schema getUnresolvedSchema() {
        return Schema.newBuilder().fromResolvedSchema(queryOperation.getResolvedSchema()).build();
    }

    @Override
    public Map<String, String> getOptions() {
        throw new TableException("A view backed by a query operation has no options.");
    }

    @Override
    public String getComment() {
        return queryOperation.asSummaryString();
    }

    @Override
    public QueryOperationCatalogView copy() {
        return new QueryOperationCatalogView(queryOperation);
    }

    @Override
    public Optional<String> getDescription() {
        return Optional.of(getComment());
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return getDescription();
    }

    @Override
    public boolean isPartitioned() {
        throw new TableException("A view backed by a query operation has no partition attribute.");
    }

    @Override
    public List<String> getPartitionKeys() {
        throw new TableException("A view backed by a query operation has no partition attribute.");
    }

    @Override
    public String getOriginalQuery() {
        throw new TableException(
                "A view backed by a query operation has no serializable representation.");
    }

    @Override
    public String getExpandedQuery() {
        throw new TableException(
                "A view backed by a query operation has no serializable representation.");
    }
}
