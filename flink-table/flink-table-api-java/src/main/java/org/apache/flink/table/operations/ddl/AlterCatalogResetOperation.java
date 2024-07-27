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

package org.apache.flink.table.operations.ddl;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogChange;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Operation to describe an ALTER CATALOG RESET statement. */
@Internal
public class AlterCatalogResetOperation implements AlterOperation {

    private final String catalogName;
    private final Set<String> resetKeys;

    public AlterCatalogResetOperation(String catalogName, Set<String> resetKeys) {
        this.catalogName = checkNotNull(catalogName);
        this.resetKeys = Collections.unmodifiableSet(checkNotNull(resetKeys));
    }

    public String getCatalogName() {
        return catalogName;
    }

    public Set<String> getResetKeys() {
        return resetKeys;
    }

    @Override
    public String asSummaryString() {
        return String.format(
                "ALTER CATALOG %s\n%s",
                catalogName,
                resetKeys.stream()
                        .map(key -> String.format("  RESET '%s'", key))
                        .collect(Collectors.joining(",\n")));
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        try {
            ctx.getCatalogManager()
                    .alterCatalog(
                            catalogName,
                            new CatalogChange.CatalogConfigurationChange(
                                    conf -> resetKeys.forEach(conf::removeKey)));

            return TableResultImpl.TABLE_RESULT_OK;
        } catch (CatalogException e) {
            throw new ValidationException(
                    String.format("Could not execute %s", asSummaryString()), e);
        }
    }
}
