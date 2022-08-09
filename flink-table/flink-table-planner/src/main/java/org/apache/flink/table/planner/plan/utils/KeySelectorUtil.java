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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.keyselector.BinaryRowDataKeySelector;
import org.apache.flink.table.runtime.keyselector.EmptyRowDataKeySelector;
import org.apache.flink.table.runtime.keyselector.GenericRowDataKeySelector;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/** Utility for KeySelector. */
public class KeySelectorUtil {

    public static RowDataKeySelector getRowDataSelector(
            ClassLoader classLoader, int[] keyFields, InternalTypeInfo<RowData> rowType) {
        return getRowDataSelector(classLoader, keyFields, rowType, BinaryRowData.class);
    }

    /**
     * Create a RowDataKeySelector to extract keys from DataStream which type is {@link
     * InternalTypeInfo} of {@link RowData}.
     *
     * @param classLoader user ClassLoader
     * @param keyFields key fields
     * @param rowType type of DataStream to extract keys
     * @return the RowDataKeySelector to extract keys from DataStream which type is {@link
     *     InternalTypeInfo} of {@link RowData}.
     */
    public static RowDataKeySelector getRowDataSelector(
            ClassLoader classLoader,
            int[] keyFields,
            InternalTypeInfo<RowData> rowType,
            Class<? extends RowData> outClass) {
        if (keyFields.length > 0) {
            LogicalType[] inputFieldTypes = rowType.toRowFieldTypes();
            LogicalType[] keyFieldTypes = new LogicalType[keyFields.length];
            for (int i = 0; i < keyFields.length; ++i) {
                keyFieldTypes[i] = inputFieldTypes[keyFields[i]];
            }
            // do not provide field names for the result key type,
            // because we may have duplicate key fields and the field names may conflict
            RowType returnType = RowType.of(keyFieldTypes);
            RowType inputType = rowType.toRowType();
            GeneratedProjection generatedProjection =
                    ProjectionCodeGenerator.generateProjection(
                            new CodeGeneratorContext(new Configuration(), classLoader),
                            "KeyProjection",
                            inputType,
                            returnType,
                            keyFields,
                            outClass);
            InternalTypeInfo<RowData> keyRowType = InternalTypeInfo.of(returnType);
            if (outClass == BinaryRowData.class) {
                return new BinaryRowDataKeySelector(keyRowType, generatedProjection);
            } else if (outClass == GenericRowData.class) {
                RowDataSerializer keySerializer = InternalSerializers.create(returnType);
                return new GenericRowDataKeySelector(
                        keyRowType, keySerializer, generatedProjection);
            } else {
                throw new UnsupportedOperationException(
                        "Currently only GenericRowData and BinaryRowData supported as outClass of KeySelector.");
            }
        } else {
            return EmptyRowDataKeySelector.INSTANCE;
        }
    }
}
