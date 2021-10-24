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

import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Operation for ALTER TABLE ADD single column clause. */
public class AlterTableAddColumnOperation extends AlterTableAddColumnsOperation {

    public AlterTableAddColumnOperation(
            ObjectIdentifier tableIdentifier,
            CatalogTable catalogTable,
            CatalogTable origCatalogTable) {
        super(tableIdentifier, catalogTable, origCatalogTable);
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("identifier", tableIdentifier);
        getAddedColumns()
                .ifPresent(
                        tableColumns -> params.put("addedColumn", getAddedColumns().get().get(0)));

        return OperationUtils.formatWithChildren(
                "ALTER TABLE ADD COLUMN",
                params,
                Collections.emptyList(),
                Operation::asSummaryString);
    }
}
