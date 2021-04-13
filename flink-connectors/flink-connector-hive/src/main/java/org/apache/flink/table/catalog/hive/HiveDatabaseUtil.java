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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveDatabase;
import org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveDatabaseOwner;
import org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveDatabase;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;

import java.util.Map;

import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveDatabase.ALTER_DATABASE_OP;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveDatabaseOwner.DATABASE_OWNER_NAME;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveDatabaseOwner.DATABASE_OWNER_TYPE;

/** Util methods for processing databases in HiveCatalog. */
public class HiveDatabaseUtil {

    private HiveDatabaseUtil() {}

    static Database instantiateHiveDatabase(String databaseName, CatalogDatabase database) {

        Map<String, String> properties = database.getProperties();

        String dbLocationUri = properties.remove(SqlCreateHiveDatabase.DATABASE_LOCATION_URI);

        return new Database(databaseName, database.getComment(), dbLocationUri, properties);
    }

    static Database alterDatabase(Database hiveDB, CatalogDatabase newDatabase) {
        Map<String, String> params = hiveDB.getParameters();
        Map<String, String> newParams = newDatabase.getProperties();
        String opStr = newParams.remove(ALTER_DATABASE_OP);
        if (opStr == null) {
            // by default is to alter db properties
            opStr = SqlAlterHiveDatabase.AlterHiveDatabaseOp.CHANGE_PROPS.name();
        }
        String newLocation = newParams.remove(SqlCreateHiveDatabase.DATABASE_LOCATION_URI);
        SqlAlterHiveDatabase.AlterHiveDatabaseOp op =
                SqlAlterHiveDatabase.AlterHiveDatabaseOp.valueOf(opStr);
        switch (op) {
            case CHANGE_PROPS:
                if (params == null) {
                    hiveDB.setParameters(newParams);
                } else {
                    params.putAll(newParams);
                }
                break;
            case CHANGE_LOCATION:
                hiveDB.setLocationUri(newLocation);
                break;
            case CHANGE_OWNER:
                String ownerName = newParams.remove(DATABASE_OWNER_NAME);
                String ownerType = newParams.remove(DATABASE_OWNER_TYPE);
                hiveDB.setOwnerName(ownerName);
                switch (ownerType) {
                    case SqlAlterHiveDatabaseOwner.ROLE_OWNER:
                        hiveDB.setOwnerType(PrincipalType.ROLE);
                        break;
                    case SqlAlterHiveDatabaseOwner.USER_OWNER:
                        hiveDB.setOwnerType(PrincipalType.USER);
                        break;
                    default:
                        throw new CatalogException("Unsupported database owner type: " + ownerType);
                }
                break;
            default:
                throw new CatalogException("Unsupported alter database op:" + opStr);
        }
        // is_generic is deprecated, remove it
        if (hiveDB.getParameters() != null) {
            hiveDB.getParameters().remove(CatalogPropertiesUtil.IS_GENERIC);
        }
        return hiveDB;
    }
}
