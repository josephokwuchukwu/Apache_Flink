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

package org.apache.flink.table.gateway.rest.serde;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.results.ResultSetImpl;
import org.apache.flink.table.gateway.rest.util.RowFormat;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.utils.print.RowDataToStringConverter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.gateway.service.result.NotReadyResult.NOT_READY_RESULT;

/**
 * A {@code ResultInfo} contains information of a {@link ResultSet}. It is designed for transferring
 * the information of ResultSet via REST. For its serialization and deserialization, See:
 *
 * <p>{@link ResultInfoJsonSerializer} and {@link ResultInfoJsonDeserializer}
 */
@Internal
public class ResultInfo {

    // Columns
    public static final String FIELD_NAME_COLUMN_INFOS = "columns";

    // RowData
    public static final String FIELD_NAME_DATA = "data";
    public static final String FIELD_NAME_KIND = "kind";
    public static final String FIELD_NAME_FIELDS = "fields";

    // RowFormat
    public static final String FIELD_NAME_ROW_FORMAT = "rowFormat";

    private final List<ColumnInfo> columnInfos;
    private final List<RowData> data;
    private final RowFormat rowFormat;

    public ResultInfo(List<ColumnInfo> columnInfos, List<RowData> data, RowFormat rowFormat) {
        this.columnInfos = columnInfos;
        this.data = data;
        this.rowFormat = rowFormat;
    }

    public static ResultInfo createResultInfo(ResultSet resultSet, RowFormat rowFormat) {
        if (resultSet == NOT_READY_RESULT) {
            return new ResultInfo(Collections.emptyList(), Collections.emptyList(), rowFormat);
        }

        List<RowData> data = resultSet.getData();

        switch (rowFormat) {
            case JSON:
                break;
            case PLAIN_TEXT:
                RowDataToStringConverter converter = ((ResultSetImpl) resultSet).getConverter();
                data =
                        data.stream()
                                .map(rowData -> convertToPlainText(rowData, converter))
                                .collect(Collectors.toList());

                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported row format: %s.", rowFormat));
        }

        return new ResultInfo(
                resultSet.getResultSchema().getColumns().stream()
                        .map(ColumnInfo::toColumnInfo)
                        .collect(Collectors.toList()),
                data,
                rowFormat);
    }

    public List<ColumnInfo> getColumnInfos() {
        return Collections.unmodifiableList(columnInfos);
    }

    public List<RowData> getData() {
        return data;
    }

    public RowFormat getRowFormat() {
        return rowFormat;
    }

    public List<RowData.FieldGetter> getFieldGetters() {
        List<LogicalType> columnTypes =
                columnInfos.stream().map(ColumnInfo::getLogicalType).collect(Collectors.toList());
        return IntStream.range(0, columnTypes.size())
                .mapToObj(i -> RowData.createFieldGetter(columnTypes.get(i), i))
                .collect(Collectors.toList());
    }

    public ResolvedSchema getResultSchema() {
        return ResolvedSchema.of(
                columnInfos.stream().map(ColumnInfo::toColumn).collect(Collectors.toList()));
    }

    private static RowData convertToPlainText(RowData rowData, RowDataToStringConverter converter) {
        String[] plainTexts = converter.convert(rowData);
        // The RowDataToStringConverter will convert null to a specific string. Here reassign it to
        // null and let the caller determine how to use it.
        IntStream.range(0, rowData.getArity())
                .filter(rowData::isNullAt)
                .forEach(i -> plainTexts[i] = null);

        return GenericRowData.ofKind(
                rowData.getRowKind(),
                Arrays.stream(plainTexts).map(StringData::fromString).toArray());
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnInfos, data);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ResultInfo)) {
            return false;
        }
        ResultInfo that = (ResultInfo) o;
        return Objects.equals(columnInfos, that.columnInfos) && Objects.equals(data, that.data);
    }

    @Override
    public String toString() {
        return String.format(
                "ResultInfo{\n  columnInfos=[%s],\n  rows=[%s]\n}",
                columnInfos.stream().map(Object::toString).collect(Collectors.joining(",")),
                data.stream().map(Object::toString).collect(Collectors.joining(",")));
    }
}
