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

package org.apache.flink.table.planner.connectors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.Column.MetadataColumn;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.operations.CollectModifyOperation;
import org.apache.flink.table.operations.ExternalModifyOperation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.plan.abilities.sink.OverwriteSpec;
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec;
import org.apache.flink.table.planner.plan.abilities.sink.WritingMetadataSpec;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalSink;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeTransformations;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapContext;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;
import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsAvoidingCast;
import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsExplicitCast;
import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsImplicitCast;

/** Utilities for dealing with {@link DynamicTableSink}. */
@Internal
public final class DynamicSinkUtils {

    /** Converts an {@link TableResult#collect()} sink to a {@link RelNode}. */
    public static RelNode convertCollectToRel(
            FlinkRelBuilder relBuilder,
            RelNode input,
            CollectModifyOperation collectModifyOperation,
            ReadableConfig configuration,
            ClassLoader classLoader) {
        final DataTypeFactory dataTypeFactory =
                unwrapContext(relBuilder).getCatalogManager().getDataTypeFactory();
        final ResolvedSchema childSchema = collectModifyOperation.getChild().getResolvedSchema();
        final ResolvedSchema schema =
                ResolvedSchema.physical(
                        childSchema.getColumnNames(), childSchema.getColumnDataTypes());
        final ResolvedCatalogTable catalogTable =
                new ResolvedCatalogTable(
                        new ExternalCatalogTable(
                                Schema.newBuilder().fromResolvedSchema(schema).build()),
                        schema);
        final ContextResolvedTable contextResolvedTable =
                ContextResolvedTable.anonymous("collect", catalogTable);

        final DataType consumedDataType = fixCollectDataType(dataTypeFactory, schema);

        final String zone = configuration.get(TableConfigOptions.LOCAL_TIME_ZONE);
        final ZoneId zoneId =
                TableConfigOptions.LOCAL_TIME_ZONE.defaultValue().equals(zone)
                        ? ZoneId.systemDefault()
                        : ZoneId.of(zone);

        final CollectDynamicSink tableSink =
                new CollectDynamicSink(
                        contextResolvedTable.getIdentifier(),
                        consumedDataType,
                        configuration.get(CollectSinkOperatorFactory.MAX_BATCH_SIZE),
                        configuration.get(CollectSinkOperatorFactory.SOCKET_TIMEOUT),
                        classLoader,
                        zoneId,
                        configuration
                                .get(ExecutionConfigOptions.TABLE_EXEC_LEGACY_CAST_BEHAVIOUR)
                                .isEnabled());
        collectModifyOperation.setSelectResultProvider(tableSink.getSelectResultProvider());
        collectModifyOperation.setConsumedDataType(consumedDataType);
        return convertSinkToRel(
                relBuilder,
                input,
                Collections.emptyMap(), // dynamicOptions
                contextResolvedTable,
                Collections.emptyMap(), // staticPartitions
                false,
                tableSink);
    }

    /**
     * Converts an external sink (i.e. further {@link DataStream} transformations) to a {@link
     * RelNode}.
     */
    public static RelNode convertExternalToRel(
            FlinkRelBuilder relBuilder,
            RelNode input,
            ExternalModifyOperation externalModifyOperation) {
        final DynamicTableSink tableSink =
                new ExternalDynamicSink(
                        externalModifyOperation.getChangelogMode().orElse(null),
                        externalModifyOperation.getPhysicalDataType());
        return convertSinkToRel(
                relBuilder,
                input,
                Collections.emptyMap(),
                externalModifyOperation.getContextResolvedTable(),
                Collections.emptyMap(),
                false,
                tableSink);
    }

    /**
     * Converts a given {@link DynamicTableSink} to a {@link RelNode}. It adds helper projections if
     * necessary.
     */
    public static RelNode convertSinkToRel(
            FlinkRelBuilder relBuilder,
            RelNode input,
            SinkModifyOperation sinkModifyOperation,
            DynamicTableSink sink) {
        return convertSinkToRel(
                relBuilder,
                input,
                sinkModifyOperation.getDynamicOptions(),
                sinkModifyOperation.getContextResolvedTable(),
                sinkModifyOperation.getStaticPartitions(),
                sinkModifyOperation.isOverwrite(),
                sink);
    }

    private static RelNode convertSinkToRel(
            FlinkRelBuilder relBuilder,
            RelNode input,
            Map<String, String> dynamicOptions,
            ContextResolvedTable contextResolvedTable,
            Map<String, String> staticPartitions,
            boolean isOverwrite,
            DynamicTableSink sink) {
        final DataTypeFactory dataTypeFactory =
                unwrapContext(relBuilder).getCatalogManager().getDataTypeFactory();
        final FlinkTypeFactory typeFactory = unwrapTypeFactory(relBuilder);
        final ResolvedSchema schema = contextResolvedTable.getResolvedSchema();
        final String tableDebugName = contextResolvedTable.getIdentifier().asSummaryString();

        List<SinkAbilitySpec> sinkAbilitySpecs = new ArrayList<>();

        boolean isDelete = false;
        boolean isUpdate = false;
        if (input instanceof LogicalTableModify) {
            LogicalTableModify tableModify = (LogicalTableModify) input;
            isDelete = tableModify.getOperation() == TableModify.Operation.DELETE;
            isUpdate = tableModify.getOperation() == TableModify.Operation.UPDATE;
        }

        // 1. prepare table sink
        prepareDynamicSink(
                tableDebugName,
                staticPartitions,
                isOverwrite,
                sink,
                contextResolvedTable.getResolvedTable(),
                sinkAbilitySpecs);
        sinkAbilitySpecs.forEach(spec -> spec.apply(sink));

        if (isDelete) {
            input =
                    convertDelete(
                            (LogicalTableModify) input,
                            sink,
                            contextResolvedTable,
                            tableDebugName,
                            dataTypeFactory,
                            typeFactory);
        } else if (isUpdate) {
            input =
                    convertUpdate(
                            (LogicalTableModify) input,
                            sink,
                            contextResolvedTable,
                            tableDebugName,
                            dataTypeFactory,
                            typeFactory);
        }

        // 2. validate the query schema to the sink's table schema and apply cast if possible
        final RelNode query = input;
        if (!isDelete && !isUpdate) {
            validateSchemaAndApplyImplicitCast(
                    input, schema, tableDebugName, dataTypeFactory, typeFactory);
        }

        relBuilder.push(query);

        // 3. convert the sink's table schema to the consumed data type of the sink
        final List<Integer> metadataColumns = extractPersistedMetadataColumns(schema);
        if (!metadataColumns.isEmpty()) {
            pushMetadataProjection(relBuilder, typeFactory, schema, sink);
        }

        List<RelHint> hints = new ArrayList<>();
        if (!dynamicOptions.isEmpty()) {
            hints.add(RelHint.builder("OPTIONS").hintOptions(dynamicOptions).build());
        }
        final RelNode finalQuery = relBuilder.build();

        return LogicalSink.create(
                finalQuery,
                hints,
                contextResolvedTable,
                sink,
                staticPartitions,
                sinkAbilitySpecs.toArray(new SinkAbilitySpec[0]));
    }

    /**
     * Checks if the given query can be written into the given sink's table schema.
     *
     * <p>It checks whether field types are compatible (types should be equal including precisions).
     * If types are not compatible, but can be implicitly cast, a cast projection will be applied.
     * Otherwise, an exception will be thrown.
     */
    public static RelNode validateSchemaAndApplyImplicitCast(
            RelNode query,
            ResolvedSchema sinkSchema,
            String tableDebugName,
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory) {
        final RowType sinkType =
                (RowType)
                        fixSinkDataType(dataTypeFactory, sinkSchema.toSinkRowDataType())
                                .getLogicalType();
        return validateSchemaAndApplyImplicitCast(query, sinkType, tableDebugName, typeFactory);
    }

    public static RelNode validateSchemaAndApplyImplicitCast(
            RelNode query,
            List<DataType> targetTypes,
            String tableDebugName,
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory) {
        final RowType sinkType =
                (RowType)
                        fixSinkDataType(
                                        dataTypeFactory,
                                        DataTypes.ROW(targetTypes.toArray(new DataType[0])))
                                .getLogicalType();
        return validateSchemaAndApplyImplicitCast(query, sinkType, tableDebugName, typeFactory);
    }

    private static RelNode validateSchemaAndApplyImplicitCast(
            RelNode query, RowType sinkType, String tableDebugName, FlinkTypeFactory typeFactory) {
        final RowType queryType = FlinkTypeFactory.toLogicalRowType(query.getRowType());
        final List<RowField> queryFields = queryType.getFields();
        final List<RowField> sinkFields = sinkType.getFields();

        if (queryFields.size() != sinkFields.size()) {
            throw createSchemaMismatchException(
                    "Different number of columns.", tableDebugName, queryFields, sinkFields);
        }

        boolean requiresCasting = false;
        for (int i = 0; i < sinkFields.size(); i++) {
            final LogicalType queryColumnType = queryFields.get(i).getType();
            final LogicalType sinkColumnType = sinkFields.get(i).getType();
            if (!supportsImplicitCast(queryColumnType, sinkColumnType)) {
                throw createSchemaMismatchException(
                        String.format(
                                "Incompatible types for sink column '%s' at position %s.",
                                sinkFields.get(i).getName(), i),
                        tableDebugName,
                        queryFields,
                        sinkFields);
            }
            if (!supportsAvoidingCast(queryColumnType, sinkColumnType)) {
                requiresCasting = true;
            }
        }

        if (requiresCasting) {
            final RelDataType castRelDataType = typeFactory.buildRelNodeRowType(sinkType);
            return RelOptUtil.createCastRel(query, castRelDataType, true);
        }
        return query;
    }

    // --------------------------------------------------------------------------------------------

    /** Temporary solution until we drop legacy types. */
    private static DataType fixCollectDataType(
            DataTypeFactory dataTypeFactory, ResolvedSchema schema) {
        final DataType fixedDataType =
                DataTypeUtils.transform(
                        dataTypeFactory,
                        schema.toSourceRowDataType(),
                        TypeTransformations.legacyRawToTypeInfoRaw(),
                        TypeTransformations.legacyToNonLegacy());
        // TODO erase the conversion class earlier when dropping legacy code, esp. FLINK-22321
        return TypeConversions.fromLogicalToDataType(fixedDataType.getLogicalType());
    }

    /**
     * Creates a projection that reorders physical and metadata columns according to the consumed
     * data type of the sink. It casts metadata columns into the expected data type.
     *
     * @see SupportsWritingMetadata
     */
    private static void pushMetadataProjection(
            FlinkRelBuilder relBuilder,
            FlinkTypeFactory typeFactory,
            ResolvedSchema schema,
            DynamicTableSink sink) {
        final RexBuilder rexBuilder = relBuilder.getRexBuilder();
        final List<Column> columns = schema.getColumns();

        final List<Integer> physicalColumns = extractPhysicalColumns(schema);

        final Map<String, Integer> keyToMetadataColumn =
                extractPersistedMetadataColumns(schema).stream()
                        .collect(
                                Collectors.toMap(
                                        pos -> {
                                            final MetadataColumn metadataColumn =
                                                    (MetadataColumn) columns.get(pos);
                                            return metadataColumn
                                                    .getMetadataKey()
                                                    .orElse(metadataColumn.getName());
                                        },
                                        Function.identity()));

        final List<Integer> metadataColumns =
                createRequiredMetadataColumns(schema, sink).stream()
                        .map(col -> col.getMetadataKey().orElse(col.getName()))
                        .map(keyToMetadataColumn::get)
                        .collect(Collectors.toList());

        final List<String> fieldNames =
                Stream.concat(
                                physicalColumns.stream().map(columns::get).map(Column::getName),
                                metadataColumns.stream()
                                        .map(columns::get)
                                        .map(MetadataColumn.class::cast)
                                        .map(c -> c.getMetadataKey().orElse(c.getName())))
                        .collect(Collectors.toList());

        final Map<String, DataType> metadataMap = extractMetadataMap(sink);

        final List<RexNode> fieldNodes =
                Stream.concat(
                                physicalColumns.stream()
                                        .map(
                                                pos -> {
                                                    final int posAdjusted =
                                                            adjustByVirtualColumns(columns, pos);
                                                    return relBuilder.field(posAdjusted);
                                                }),
                                metadataColumns.stream()
                                        .map(
                                                pos -> {
                                                    final MetadataColumn metadataColumn =
                                                            (MetadataColumn) columns.get(pos);
                                                    final String metadataKey =
                                                            metadataColumn
                                                                    .getMetadataKey()
                                                                    .orElse(
                                                                            metadataColumn
                                                                                    .getName());

                                                    final LogicalType expectedType =
                                                            metadataMap
                                                                    .get(metadataKey)
                                                                    .getLogicalType();
                                                    final RelDataType expectedRelDataType =
                                                            typeFactory
                                                                    .createFieldTypeFromLogicalType(
                                                                            expectedType);

                                                    final int posAdjusted =
                                                            adjustByVirtualColumns(columns, pos);
                                                    return rexBuilder.makeAbstractCast(
                                                            expectedRelDataType,
                                                            relBuilder.field(posAdjusted));
                                                }))
                        .collect(Collectors.toList());

        relBuilder.projectNamed(fieldNodes, fieldNames, true);
    }

    /**
     * Prepares the given {@link DynamicTableSink}. It check whether the sink is compatible with the
     * INSERT INTO clause and applies initial parameters.
     */
    private static void prepareDynamicSink(
            String tableDebugName,
            Map<String, String> staticPartitions,
            boolean isOverwrite,
            DynamicTableSink sink,
            ResolvedCatalogTable table,
            List<SinkAbilitySpec> sinkAbilitySpecs) {
        validatePartitioning(tableDebugName, staticPartitions, sink, table.getPartitionKeys());

        validateAndApplyOverwrite(tableDebugName, isOverwrite, sink, sinkAbilitySpecs);

        validateAndApplyMetadata(tableDebugName, sink, table.getResolvedSchema(), sinkAbilitySpecs);
    }

    /**
     * Returns a list of required metadata columns. Ordered by the iteration order of {@link
     * SupportsWritingMetadata#listWritableMetadata()}.
     *
     * <p>This method assumes that sink and schema have been validated via {@link
     * #prepareDynamicSink}.
     */
    private static List<MetadataColumn> createRequiredMetadataColumns(
            ResolvedSchema schema, DynamicTableSink sink) {
        final List<Column> tableColumns = schema.getColumns();
        final List<Integer> metadataColumns = extractPersistedMetadataColumns(schema);

        Map<String, MetadataColumn> metadataKeysToMetadataColumns = new HashMap<>();

        for (Integer columnIndex : metadataColumns) {
            MetadataColumn metadataColumn = (MetadataColumn) tableColumns.get(columnIndex);
            String metadataKey = metadataColumn.getMetadataKey().orElse(metadataColumn.getName());
            // After resolving, every metadata column has the unique metadata key.
            metadataKeysToMetadataColumns.put(metadataKey, metadataColumn);
        }

        final Map<String, DataType> metadataMap = extractMetadataMap(sink);

        return metadataMap.keySet().stream()
                .filter(metadataKeysToMetadataColumns::containsKey)
                .map(metadataKeysToMetadataColumns::get)
                .collect(Collectors.toList());
    }

    private static ValidationException createSchemaMismatchException(
            String cause,
            String tableDebugName,
            List<RowField> queryFields,
            List<RowField> sinkFields) {
        final String querySchema =
                queryFields.stream()
                        .map(f -> f.getName() + ": " + f.getType().asSummaryString())
                        .collect(Collectors.joining(", ", "[", "]"));
        final String sinkSchema =
                sinkFields.stream()
                        .map(
                                sinkField ->
                                        sinkField.getName()
                                                + ": "
                                                + sinkField.getType().asSummaryString())
                        .collect(Collectors.joining(", ", "[", "]"));

        return new ValidationException(
                String.format(
                        "Column types of query result and sink for '%s' do not match.\n"
                                + "Cause: %s\n\n"
                                + "Query schema: %s\n"
                                + "Sink schema:  %s",
                        tableDebugName, cause, querySchema, sinkSchema));
    }

    private static DataType fixSinkDataType(
            DataTypeFactory dataTypeFactory, DataType sinkDataType) {
        // we ignore NULL constraint, the NULL constraint will be checked during runtime
        // see StreamExecSink and BatchExecSink
        return DataTypeUtils.transform(
                dataTypeFactory,
                sinkDataType,
                TypeTransformations.legacyRawToTypeInfoRaw(),
                TypeTransformations.legacyToNonLegacy(),
                TypeTransformations.toNullable());
    }

    private static void validatePartitioning(
            String tableDebugName,
            Map<String, String> staticPartitions,
            DynamicTableSink sink,
            List<String> partitionKeys) {
        if (!partitionKeys.isEmpty()) {
            if (!(sink instanceof SupportsPartitioning)) {
                throw new TableException(
                        String.format(
                                "Table '%s' is a partitioned table, but the underlying %s doesn't "
                                        + "implement the %s interface.",
                                tableDebugName,
                                DynamicTableSink.class.getSimpleName(),
                                SupportsPartitioning.class.getSimpleName()));
            }
        }

        staticPartitions
                .keySet()
                .forEach(
                        p -> {
                            if (!partitionKeys.contains(p)) {
                                throw new ValidationException(
                                        String.format(
                                                "Static partition column '%s' should be in the partition keys list %s for table '%s'.",
                                                p, partitionKeys, tableDebugName));
                            }
                        });
    }

    private static void validateAndApplyOverwrite(
            String tableDebugName,
            boolean isOverwrite,
            DynamicTableSink sink,
            List<SinkAbilitySpec> sinkAbilitySpecs) {
        if (!isOverwrite) {
            return;
        }
        if (!(sink instanceof SupportsOverwrite)) {
            throw new ValidationException(
                    String.format(
                            "INSERT OVERWRITE requires that the underlying %s of table '%s' "
                                    + "implements the %s interface.",
                            DynamicTableSink.class.getSimpleName(),
                            tableDebugName,
                            SupportsOverwrite.class.getSimpleName()));
        }
        sinkAbilitySpecs.add(new OverwriteSpec(true));
    }

    private static List<Integer> extractPhysicalColumns(ResolvedSchema schema) {
        final List<Column> columns = schema.getColumns();
        return IntStream.range(0, schema.getColumnCount())
                .filter(pos -> columns.get(pos).isPhysical())
                .boxed()
                .collect(Collectors.toList());
    }

    private static List<Integer> extractPersistedMetadataColumns(ResolvedSchema schema) {
        final List<Column> columns = schema.getColumns();
        return IntStream.range(0, schema.getColumnCount())
                .filter(
                        pos -> {
                            final Column column = columns.get(pos);
                            return column instanceof MetadataColumn && column.isPersisted();
                        })
                .boxed()
                .collect(Collectors.toList());
    }

    private static int adjustByVirtualColumns(List<Column> columns, int pos) {
        return pos
                - (int) IntStream.range(0, pos).filter(i -> !columns.get(i).isPersisted()).count();
    }

    private static Map<String, DataType> extractMetadataMap(DynamicTableSink sink) {
        if (sink instanceof SupportsWritingMetadata) {
            return ((SupportsWritingMetadata) sink).listWritableMetadata();
        }
        return Collections.emptyMap();
    }

    private static void validateAndApplyMetadata(
            String tableDebugName,
            DynamicTableSink sink,
            ResolvedSchema schema,
            List<SinkAbilitySpec> sinkAbilitySpecs) {
        final List<Column> columns = schema.getColumns();
        final List<Integer> metadataColumns = extractPersistedMetadataColumns(schema);

        if (metadataColumns.isEmpty()) {
            return;
        }

        if (!(sink instanceof SupportsWritingMetadata)) {
            throw new ValidationException(
                    String.format(
                            "Table '%s' declares persistable metadata columns, but the underlying %s "
                                    + "doesn't implement the %s interface. If the column should not "
                                    + "be persisted, it can be declared with the VIRTUAL keyword.",
                            tableDebugName,
                            DynamicTableSink.class.getSimpleName(),
                            SupportsWritingMetadata.class.getSimpleName()));
        }

        final Map<String, DataType> metadataMap =
                ((SupportsWritingMetadata) sink).listWritableMetadata();
        metadataColumns.forEach(
                pos -> {
                    final MetadataColumn metadataColumn = (MetadataColumn) columns.get(pos);
                    final String metadataKey =
                            metadataColumn.getMetadataKey().orElse(metadataColumn.getName());
                    final LogicalType metadataType = metadataColumn.getDataType().getLogicalType();
                    final DataType expectedMetadataDataType = metadataMap.get(metadataKey);
                    // check that metadata key is valid
                    if (expectedMetadataDataType == null) {
                        throw new ValidationException(
                                String.format(
                                        "Invalid metadata key '%s' in column '%s' of table '%s'. "
                                                + "The %s class '%s' supports the following metadata keys for writing:\n%s",
                                        metadataKey,
                                        metadataColumn.getName(),
                                        tableDebugName,
                                        DynamicTableSink.class.getSimpleName(),
                                        sink.getClass().getName(),
                                        String.join("\n", metadataMap.keySet())));
                    }
                    // check that types are compatible
                    if (!supportsExplicitCast(
                            metadataType, expectedMetadataDataType.getLogicalType())) {
                        if (metadataKey.equals(metadataColumn.getName())) {
                            throw new ValidationException(
                                    String.format(
                                            "Invalid data type for metadata column '%s' of table '%s'. "
                                                    + "The column cannot be declared as '%s' because the type must be "
                                                    + "castable to metadata type '%s'.",
                                            metadataColumn.getName(),
                                            tableDebugName,
                                            metadataType,
                                            expectedMetadataDataType.getLogicalType()));
                        } else {
                            throw new ValidationException(
                                    String.format(
                                            "Invalid data type for metadata column '%s' with metadata key '%s' of table '%s'. "
                                                    + "The column cannot be declared as '%s' because the type must be "
                                                    + "castable to metadata type '%s'.",
                                            metadataColumn.getName(),
                                            metadataKey,
                                            tableDebugName,
                                            metadataType,
                                            expectedMetadataDataType.getLogicalType()));
                        }
                    }
                });

        sinkAbilitySpecs.add(
                new WritingMetadataSpec(
                        createRequiredMetadataColumns(schema, sink).stream()
                                .map(col -> col.getMetadataKey().orElse(col.getName()))
                                .collect(Collectors.toList()),
                        createConsumedType(schema, sink)));
    }

    /**
     * Returns the {@link DataType} that a sink should consume as the output from the runtime.
     *
     * <p>The format looks as follows: {@code PHYSICAL COLUMNS + PERSISTED METADATA COLUMNS}
     */
    private static RowType createConsumedType(ResolvedSchema schema, DynamicTableSink sink) {
        final Map<String, DataType> metadataMap = extractMetadataMap(sink);

        final Stream<RowField> physicalFields =
                schema.getColumns().stream()
                        .filter(Column::isPhysical)
                        .map(c -> new RowField(c.getName(), c.getDataType().getLogicalType()));

        final Stream<RowField> metadataFields =
                createRequiredMetadataColumns(schema, sink).stream()
                        .map(
                                column ->
                                        new RowField(
                                                // Use alias to ensures that physical and metadata
                                                // columns don't collide.
                                                column.getName(),
                                                metadataMap
                                                        .get(
                                                                column.getMetadataKey()
                                                                        .orElse(column.getName()))
                                                        .getLogicalType()));

        final List<RowField> rowFields =
                Stream.concat(physicalFields, metadataFields).collect(Collectors.toList());

        return new RowType(false, rowFields);
    }

    private static RelNode convertDelete(
            LogicalTableModify tableModify,
            DynamicTableSink sink,
            ContextResolvedTable contextResolvedTable,
            String tableDebugName,
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory) {
        if (!(sink instanceof SupportsRowLevelDelete)) {
            throw new UnsupportedOperationException("Should implement SupportsRowLevelDelete");
        }
        SupportsRowLevelDelete supportsRowLevelDelete = (SupportsRowLevelDelete) sink;
        SupportsRowLevelDelete.RowLevelDeleteInfo rowLevelDeleteInfo =
                supportsRowLevelDelete.applyRowLevelDelete();
        if (rowLevelDeleteInfo.getRowLevelDeleteMode()
                == SupportsRowLevelDelete.RowLevelDeleteInfo.RowLevelDeleteMode.DELETED_ROWS) {
            return convertToRowLevelDelete(
                    tableModify,
                    contextResolvedTable,
                    rowLevelDeleteInfo,
                    tableDebugName,
                    dataTypeFactory,
                    typeFactory);
        } else if (rowLevelDeleteInfo.getRowLevelDeleteMode()
                == SupportsRowLevelDelete.RowLevelDeleteInfo.RowLevelDeleteMode.REMAINING_ROWS) {
            convertToComplementRowLevelDelete(tableModify);
            return convertToRowLevelDelete(
                    tableModify,
                    contextResolvedTable,
                    rowLevelDeleteInfo,
                    tableDebugName,
                    dataTypeFactory,
                    typeFactory);
        } else {
            throw new IllegalArgumentException(
                    "Unknown delete mode: " + rowLevelDeleteInfo.getRowLevelDeleteMode());
        }
    }

    private static RelNode convertUpdate(
            LogicalTableModify tableModify,
            DynamicTableSink sink,
            ContextResolvedTable contextResolvedTable,
            String tableDebugName,
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory) {
        if (!(sink instanceof SupportsRowLevelUpdate)) {
            throw new UnsupportedOperationException("Should implement SupportsRowLevelUpdate");
        }
        SupportsRowLevelUpdate supportsRowLevelUpdate = (SupportsRowLevelUpdate) sink;
        ResolvedSchema resolvedSchema = contextResolvedTable.getResolvedSchema();
        List<Column> updatedColumns = getUpdatedColumns(tableModify, resolvedSchema);
        SupportsRowLevelUpdate.RowLevelUpdateInfo updateInfo =
                supportsRowLevelUpdate.applyRowLevelUpdate(updatedColumns);
        if (updateInfo.getRowLevelUpdateMode()
                == SupportsRowLevelUpdate.RowLevelUpdateInfo.RowLevelUpdateMode.UPDATED_ROWS) {
            return convertToRowLevelUpdate(
                    tableModify,
                    contextResolvedTable,
                    updateInfo,
                    tableDebugName,
                    dataTypeFactory,
                    typeFactory);
        } else if (updateInfo.getRowLevelUpdateMode()
                == SupportsRowLevelUpdate.RowLevelUpdateInfo.RowLevelUpdateMode.ALL_ROWS) {
            Project project = (Project) tableModify.getInput();
            if (project.getInput() instanceof LogicalFilter) {
                LogicalFilter filter = (LogicalFilter) (project.getInput());
                RexNode condition = filter.getCondition();
                RexBuilder rexBuilder = tableModify.getCluster().getRexBuilder();
                // todo: check whether contains subquery
                List<Column> requiredColumns =
                        updateInfo.requiredColumns().orElse(resolvedSchema.getColumns());
                LogicalTableScan tableScan = getSourceTableScan(tableModify);
                Tuple2<List<Integer>, List<MetadataColumn>> colsIndexAndExtraMetaCols =
                        getRequireColumnsIndexAndExtraMetaCols(
                                tableScan, requiredColumns, resolvedSchema);
                List<Integer> updatedIndexes = colsIndexAndExtraMetaCols.f0;
                List<MetadataColumn> metadataColumns = colsIndexAndExtraMetaCols.f1;
                // if meta columns size is greater than 0, we need to modify the underlying
                // LogicalTableScan to make it can read meta column
                int oldColsCount = resolvedSchema.getColumnCount();
                if (metadataColumns.size() > 0) {
                    resolvedSchema =
                            addExtraMetaCols(
                                    tableModify,
                                    tableScan,
                                    tableDebugName,
                                    metadataColumns,
                                    typeFactory);
                }
                // the updated columns, whose order is same to user's update clause
                List<String> updatedColumnNames = tableModify.getUpdateColumnList();
                List<RexNode> newRexNodeList = new ArrayList<>();
                List<String> newFieldNames = new ArrayList<>();
                List<DataType> updateTargetDataTypes = new ArrayList<>();
                // the rex nodes for the project are like: index for all col, update expressions for
                // the updated columns
                List<RexNode> oldRexNodes = project.getProjects();
                for (int index : updatedIndexes) {
                    String colName = resolvedSchema.getColumnNames().get(index);
                    // if the updated cols contain the col to be selected, the updated expression
                    // should be in the project node
                    if (updatedColumnNames.contains(colName)) {
                        // get the index of the updated column in all updated columns
                        int i = updatedColumnNames.indexOf(colName);
                        // get the update expression
                        RexNode rexNode = oldRexNodes.get(oldColsCount + i);
                        rexNode =
                                rexBuilder.makeCall(
                                        FlinkSqlOperatorTable.IF,
                                        Arrays.asList(
                                                condition,
                                                rexNode,
                                                rexBuilder.makeInputRef(
                                                        project.getInput(), index)));
                        newRexNodeList.add(rexNode);
                    } else {
                        newRexNodeList.add(rexBuilder.makeInputRef(project.getInput(), index));
                    }
                    newFieldNames.add(resolvedSchema.getColumnNames().get(index));
                    updateTargetDataTypes.add(resolvedSchema.getColumnDataTypes().get(index));
                }

                project =
                        project.copy(
                                project.getTraitSet(),
                                filter.getInput(),
                                newRexNodeList,
                                RexUtil.createStructType(
                                        typeFactory, newRexNodeList, newFieldNames, null));
                return validateSchemaAndApplyImplicitCast(
                        project,
                        updateTargetDataTypes,
                        tableDebugName,
                        dataTypeFactory,
                        typeFactory);

            } else {
            }
            return null;
        } else {
            throw new IllegalArgumentException(
                    "Unknown update mode:" + updateInfo.getRowLevelUpdateMode());
        }
    }

    private static List<Column> getUpdatedColumns(
            LogicalTableModify tableModify, ResolvedSchema resolvedSchema) {
        List<Column> updatedColumns = new ArrayList<>();
        List<String> updatedColumnNames = tableModify.getUpdateColumnList();
        for (Column column : resolvedSchema.getColumns()) {
            if (updatedColumnNames.contains(column.getName())) {
                updatedColumns.add(column);
            }
        }
        return updatedColumns;
    }

    private static RelNode convertToRowLevelDelete(
            LogicalTableModify tableModify,
            ContextResolvedTable contextResolvedTable,
            SupportsRowLevelDelete.RowLevelDeleteInfo rowLevelDeleteInfo,
            String tableDebugName,
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory) {
        ResolvedSchema resolvedSchema = contextResolvedTable.getResolvedSchema();
        Optional<List<Column>> optionalColumns = rowLevelDeleteInfo.requiredColumns();
        List<Column> requiredColumns = optionalColumns.orElse(resolvedSchema.getColumns());
        LogicalTableScan tableScan = getSourceTableScan(tableModify);
        Tuple2<List<Integer>, List<MetadataColumn>> colsIndexAndExtraMetaCols =
                getRequireColumnsIndexAndExtraMetaCols(tableScan, requiredColumns, resolvedSchema);
        List<Integer> colIndexes = colsIndexAndExtraMetaCols.f0;
        List<MetadataColumn> metadataColumns = colsIndexAndExtraMetaCols.f1;
        // if meta columns size is greater than 0, we need to modify the underlying
        // LogicalTableScan to make it can read meta column
        if (metadataColumns.size() > 0) {
            resolvedSchema =
                    addExtraMetaCols(
                            tableModify, tableScan, tableDebugName, metadataColumns, typeFactory);
        }
        return createNewRelNodeForDelete(
                tableModify,
                resolvedSchema,
                colIndexes,
                tableDebugName,
                dataTypeFactory,
                typeFactory);
    }

    private static void convertToComplementRowLevelDelete(LogicalTableModify tableModify) {
        RexBuilder rexBuilder = tableModify.getCluster().getRexBuilder();
        RelNode input = tableModify.getInput();
        LogicalFilter newFilter;
        if (input.getInput(0) instanceof LogicalTableScan) {
            newFilter = LogicalFilter.create(input.getInput(0), rexBuilder.makeLiteral(false));
        } else {
            LogicalFilter filter = (LogicalFilter) input.getInput(0);
            RexNode complementFilter =
                    rexBuilder.makeCall(
                            filter.getCondition().getType(),
                            FlinkSqlOperatorTable.NOT,
                            Collections.singletonList(filter.getCondition()));

            newFilter = filter.copy(filter.getTraitSet(), filter.getInput(), complementFilter);
        }
        input.replaceInput(0, newFilter);
    }

    private static RelNode convertToRowLevelUpdate(
            LogicalTableModify tableModify,
            ContextResolvedTable contextResolvedTable,
            SupportsRowLevelUpdate.RowLevelUpdateInfo rowLevelUpdateInfo,
            String tableDebugName,
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory) {
        ResolvedSchema resolvedSchema = contextResolvedTable.getResolvedSchema();
        Optional<List<Column>> optionalColumns = rowLevelUpdateInfo.requiredColumns();
        List<Column> requiredColumns = optionalColumns.orElse(resolvedSchema.getColumns());
        LogicalTableScan tableScan = getSourceTableScan(tableModify);
        Tuple2<List<Integer>, List<MetadataColumn>> colsIndexAndExtraMetaCols =
                getRequireColumnsIndexAndExtraMetaCols(tableScan, requiredColumns, resolvedSchema);
        List<Integer> updatedIndexes = colsIndexAndExtraMetaCols.f0;
        List<MetadataColumn> metadataColumns = colsIndexAndExtraMetaCols.f1;
        // if meta columns size is greater than 0, we need to modify the underlying
        // LogicalTableScan to make it can read meta column
        int oldColsCount = resolvedSchema.getColumnCount();
        if (metadataColumns.size() > 0) {
            resolvedSchema =
                    addExtraMetaCols(
                            tableModify, tableScan, tableDebugName, metadataColumns, typeFactory);
        }
        return createNewRelNodeForUpdate(
                tableModify,
                oldColsCount,
                resolvedSchema,
                updatedIndexes,
                tableDebugName,
                dataTypeFactory,
                typeFactory);
    }

    private static Tuple2<List<Integer>, List<MetadataColumn>>
            getRequireColumnsIndexAndExtraMetaCols(
                    LogicalTableScan tableScan,
                    List<Column> requiredColumns,
                    ResolvedSchema resolvedSchema) {
        List<Integer> columnIndexList = new ArrayList<>();
        List<MetadataColumn> extraMetadataColumns = new ArrayList<>();
        List<String> fieldNames = resolvedSchema.getColumnNames();
        final TableSourceTable sourceTable = tableScan.getTable().unwrap(TableSourceTable.class);
        DynamicTableSource dynamicTableSource = sourceTable.tableSource();
        int additionCols = 0;
        for (Column column : requiredColumns) {
            int index = fieldNames.indexOf(column.getName());
            if (index <= -1) {
                if (column instanceof Column.MetadataColumn) {
                    // need to add meta column
                    columnIndexList.add(fieldNames.size() + additionCols);
                    if (!(dynamicTableSource instanceof SupportsReadingMetadata)) {
                        throw new IllegalArgumentException("don't support reading metadata.");
                    }
                    SupportsReadingMetadata supportsReadingMetadata =
                            (SupportsReadingMetadata) dynamicTableSource;
                    Map<String, DataType> readableMetadata =
                            supportsReadingMetadata.listReadableMetadata();
                    if (!readableMetadata.containsKey(
                            ((MetadataColumn) column).getMetadataKey().orElse(column.getName()))) {
                        throw new IllegalArgumentException(
                                "expect to read the meta column {}, but the source doesn't "
                                        + "support read metadata column {}.");
                    }
                    additionCols += 1;
                    DataType dataType =
                            readableMetadata.get(
                                    ((MetadataColumn) column)
                                            .getMetadataKey()
                                            .orElse(column.getName()));
                    if (!dataType.equals(column.getDataType())) {
                        throw new IllegalArgumentException("un-matched data type.");
                    }
                    extraMetadataColumns.add((MetadataColumn) column);
                } else {
                    throw new IllegalArgumentException("Unknown require column " + column);
                }
            } else {
                columnIndexList.add(index);
            }
        }
        return Tuple2.of(columnIndexList, extraMetadataColumns);
    }

    private static LogicalTableScan getSourceTableScan(RelNode relNode) {
        while (!(relNode instanceof LogicalTableScan)) {
            relNode = relNode.getInput(0);
        }
        return (LogicalTableScan) relNode;
    }

    private static RelNode createNewRelNodeForDelete(
            LogicalTableModify tableModify,
            ResolvedSchema resolvedSchema,
            List<Integer> colIndexes,
            String tableDebugName,
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory) {
        // now we know which columns we may need
        List<RexNode> newRexNodeList = new ArrayList<>();
        List<String> newFieldNames = new ArrayList<>();
        List<DataType> deleteTargetDataTypes = new ArrayList<>();
        Project project = (Project) (tableModify.getInput());
        RexBuilder rexBuilder = tableModify.getCluster().getRexBuilder();
        for (int index : colIndexes) {
            newRexNodeList.add(rexBuilder.makeInputRef(project.getInput(), index));
            newFieldNames.add(resolvedSchema.getColumnNames().get(index));
            deleteTargetDataTypes.add(resolvedSchema.getColumnDataTypes().get(index));
        }
        project =
                project.copy(
                        project.getTraitSet(),
                        project.getInput(),
                        newRexNodeList,
                        RexUtil.createStructType(typeFactory, newRexNodeList, newFieldNames, null));
        return validateSchemaAndApplyImplicitCast(
                project, deleteTargetDataTypes, tableDebugName, dataTypeFactory, typeFactory);
    }

    private static RelNode createNewRelNodeForUpdate(
            LogicalTableModify tableModify,
            int oldColsCount,
            ResolvedSchema resolvedSchema,
            List<Integer> updatedIndexes,
            String tableDebugName,
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory) {
        RexBuilder rexBuilder = tableModify.getCluster().getRexBuilder();
        // the updated columns, whose order is same to user's update clause
        List<String> updatedColumnNames = tableModify.getUpdateColumnList();
        List<RexNode> newRexNodeList = new ArrayList<>();
        List<String> newFieldNames = new ArrayList<>();
        List<DataType> updateTargetDataTypes = new ArrayList<>();
        Project project = (Project) (tableModify.getInput());
        // the rex nodes for the project are like: index for all col, update expressions for the
        // updated columns
        List<RexNode> oldRexNodes = project.getProjects();
        for (int index : updatedIndexes) {
            String colName = resolvedSchema.getColumnNames().get(index);
            // if the updated cols contain the col to be selected, the updated expression should
            // be in the project node
            if (updatedColumnNames.contains(colName)) {
                // get the index of the updated column in all updated columns
                int i = updatedColumnNames.indexOf(colName);
                // get the update expression
                RexNode rexNode = oldRexNodes.get(oldColsCount + i);
                newRexNodeList.add(rexNode);
            } else {
                newRexNodeList.add(rexBuilder.makeInputRef(project.getInput(), index));
            }
            newFieldNames.add(resolvedSchema.getColumnNames().get(index));
            updateTargetDataTypes.add(resolvedSchema.getColumnDataTypes().get(index));
        }

        project =
                project.copy(
                        project.getTraitSet(),
                        project.getInput(),
                        newRexNodeList,
                        RexUtil.createStructType(typeFactory, newRexNodeList, newFieldNames, null));
        return validateSchemaAndApplyImplicitCast(
                project, updateTargetDataTypes, tableDebugName, dataTypeFactory, typeFactory);
    }

    private static ResolvedSchema addExtraMetaCols(
            LogicalTableModify tableModify,
            LogicalTableScan tableScan,
            String tableDebugName,
            List<MetadataColumn> metadataColumns,
            FlinkTypeFactory typeFactory) {
        final TableSourceTable sourceTable = tableScan.getTable().unwrap(TableSourceTable.class);
        DynamicTableSource dynamicTableSource = sourceTable.tableSource();
        // get old schema and new schema after add some cols
        ResolvedSchema oldSchema = sourceTable.contextResolvedTable().getResolvedSchema();
        List<Column> newColumns = new ArrayList<>(oldSchema.getColumns());
        newColumns.addAll(metadataColumns);
        ResolvedSchema resolvedSchema = ResolvedSchema.of(newColumns);

        List<RelDataTypeField> oldFields = sourceTable.getRowType().getFieldList();
        List<RelDataTypeField> newFields = new ArrayList<>(sourceTable.getRowType().getFieldList());
        for (int i = 0; i < metadataColumns.size(); i++) {
            MetadataColumn column = metadataColumns.get(i);
            newFields.add(
                    new RelDataTypeFieldImpl(
                            column.getMetadataKey().orElse(column.getName()),
                            oldFields.size() + i,
                            typeFactory.createFieldTypeFromLogicalType(
                                    column.getDataType().getLogicalType())));
        }
        TableSourceTable newTableSourceTab =
                sourceTable.copy(
                        dynamicTableSource,
                        sourceTable.contextResolvedTable().copy(resolvedSchema),
                        new RelRecordType(StructKind.FULLY_QUALIFIED, newFields, false),
                        sourceTable.abilitySpecs());

        LogicalTableScan newTableScan =
                new LogicalTableScan(
                        tableScan.getCluster(),
                        tableScan.getTraitSet(),
                        tableScan.getHints(),
                        newTableSourceTab);
        Project project = (Project) tableModify.getInput();
        if (project.getInput() instanceof LogicalFilter) {
            LogicalFilter logicalFilter = (LogicalFilter) project.getInput();
            project.replaceInput(
                    0,
                    logicalFilter.copy(
                            logicalFilter.getTraitSet(),
                            newTableScan,
                            logicalFilter.getCondition()));
        } else {
            project.replaceInput(0, newTableScan);
        }
        DynamicSourceUtils.validateAndApplyMetadata(
                tableDebugName, resolvedSchema, newTableSourceTab.tableSource());
        return resolvedSchema;
    }

    private DynamicSinkUtils() {
        // no instantiation
    }
}
