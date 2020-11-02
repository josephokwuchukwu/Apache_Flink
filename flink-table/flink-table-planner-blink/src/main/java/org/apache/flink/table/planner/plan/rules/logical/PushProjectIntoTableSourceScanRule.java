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

import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.RexNodeExtractor;
import org.apache.flink.table.planner.plan.utils.RexNodeNestedField;
import org.apache.flink.table.planner.plan.utils.RexNodeNestedFields;
import org.apache.flink.table.planner.sources.DynamicSourceUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.RowKind;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Planner rule that pushes a {@link LogicalProject} into a {@link LogicalTableScan}
 * which wraps a {@link SupportsProjectionPushDown} dynamic table source.
 */
public class PushProjectIntoTableSourceScanRule extends RelOptRule {
	public static final PushProjectIntoTableSourceScanRule INSTANCE = new PushProjectIntoTableSourceScanRule();

	public PushProjectIntoTableSourceScanRule() {
		super(operand(LogicalProject.class,
				operand(LogicalTableScan.class, none())),
				"PushProjectIntoTableSourceScanRule");
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		LogicalTableScan scan = call.rel(1);
		TableSourceTable tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
		if (tableSourceTable == null || !(tableSourceTable.tableSource() instanceof SupportsProjectionPushDown)) {
			return false;
		}
		return Arrays.stream(tableSourceTable.extraDigests()).noneMatch(digest -> digest.startsWith("project=["));
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		final LogicalProject project = call.rel(0);
		final LogicalTableScan scan = call.rel(1);

		final int[] refFields = RexNodeExtractor.extractRefInputFields(project.getProjects());
		TableSourceTable oldTableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
		final TableSchema oldSchema = oldTableSourceTable.catalogTable().getSchema();
		final DynamicTableSource oldSource = oldTableSourceTable.tableSource();

		final boolean supportsNestedProjection =
				((SupportsProjectionPushDown) oldTableSourceTable.tableSource()).supportsNestedProjection();
		List<String> fieldNames = scan.getRowType().getFieldNames();

		if (!supportsNestedProjection && refFields.length == fieldNames.size()) {
			// just keep as same as the old plan
			// TODO: refactor the affected plan
			return;
		}

		List<RexNode> oldProjectsWithPK = new ArrayList<>(project.getProjects());
		FlinkTypeFactory flinkTypeFactory = (FlinkTypeFactory) oldTableSourceTable.getRelOptSchema().getTypeFactory();
		if (isUpsertSource(oldTableSourceTable)) {
			// add pk into projects
			oldSchema.getPrimaryKey().ifPresent(
					pks -> {
						for (String name: pks.getColumns()) {
							int index = fieldNames.indexOf(name);
							TableColumn col = oldSchema.getTableColumn(index).get();
							oldProjectsWithPK.add(
									new RexInputRef(index,
											flinkTypeFactory.createFieldTypeFromLogicalType(col.getType().getLogicalType())));
						}
					}
			);
		}
		// build used schema tree
		RowType originType =
				DynamicSourceUtils.createProducedType(oldSchema, oldSource);
		RexNodeNestedFields root = RexNodeNestedFields.build(
				oldProjectsWithPK, flinkTypeFactory.buildRelNodeRowType(originType));
		if (!supportsNestedProjection) {
			// mark the fields in the top level as useall
			for (RexNodeNestedField column: root.columns().values()) {
				column.isLeaf_$eq(true);
			}
		}
		final DynamicTableSource newSource = oldSource.copy();
		final int[][] projectedFields;
		final DataType newProducedDataType;
		DataType producedDataType = TypeConversions.fromLogicalToDataType(originType);

		if (oldSource instanceof SupportsReadingMetadata) {
			//TODO: supports nested projection for metadata
			final List<String> metadataKeys = DynamicSourceUtils.createRequiredMetadataKeys(oldSchema, oldSource);
			List<RexNodeNestedField> usedMetaDataFields = new LinkedList<>();
			int physicalCount = fieldNames.size() - metadataKeys.size();
			// rm metadata in the tree
			for (int i = 0; i < metadataKeys.size(); i++) {
				final RexInputRef key = new RexInputRef(i + physicalCount,
						flinkTypeFactory.createFieldTypeFromLogicalType(originType.getChildren().get(i + physicalCount)));
				RexNodeNestedField usedMetadata = root.columns().remove(key.getName());
				if (usedMetadata != null) {
					usedMetaDataFields.add(usedMetadata);
				}
			}
			// label the tree and get path
			int[][] projectedPhysicalFields = RexNodeNestedFields.labelAndConvert(root);
			((SupportsProjectionPushDown) newSource).applyProjection(projectedPhysicalFields);
			// push the metadata back for later rewrite and extract the location in the origin row
			int order = projectedPhysicalFields.length;
			List<String> usedMetadataNames = new LinkedList<>();
			for (RexNodeNestedField metadata: usedMetaDataFields) {
				metadata.indexInNewSchema_$eq(order++);
				root.columns().put(metadata.name(), metadata);
				usedMetadataNames.add(metadataKeys.get(metadata.indexInOriginSchema() - physicalCount));
			}
			// apply metadata push down
			projectedFields = Stream.concat(
					Stream.of(projectedPhysicalFields),
					usedMetaDataFields.stream().map(field -> new int[]{field.indexInOriginSchema()})
			).toArray(int[][]::new);
			newProducedDataType = DataTypeUtils.projectRow(producedDataType, projectedFields);
			((SupportsReadingMetadata) newSource).applyReadableMetadata(
					usedMetadataNames, newProducedDataType);
		} else {
			projectedFields = RexNodeNestedFields.labelAndConvert(root);
			((SupportsProjectionPushDown) newSource).applyProjection(projectedFields);
			newProducedDataType = DataTypeUtils.projectRow(producedDataType, projectedFields);
		}

		RelDataType newRowType = flinkTypeFactory.buildRelNodeRowType((RowType) newProducedDataType.getLogicalType());

		// project push down does not change the statistic, we can reuse origin statistic
		TableSourceTable newTableSourceTable = oldTableSourceTable.copy(
				newSource, newRowType, new String[] {
						("project=[" + String.join(", ", newRowType.getFieldNames()) + "]") });
		LogicalTableScan newScan = new LogicalTableScan(
				scan.getCluster(), scan.getTraitSet(), scan.getHints(), newTableSourceTable);
		// rewrite input field in projections
		// the origin projections are enough. Because the upsert source only uses pk info in the deduplication node.
		List<RexNode> newProjects =
				RexNodeNestedFields.rewrite(project.getProjects(), root, call.builder().getRexBuilder());
		// rewrite new source
		LogicalProject newProject = project.copy(
				project.getTraitSet(),
				newScan,
				newProjects,
				project.getRowType());

		if (ProjectRemoveRule.isTrivial(newProject)) {
			// drop project if the transformed program merely returns its input
			call.transformTo(newScan);
		} else {
			call.transformTo(newProject);
		}
	}

	/**
	 * Returns true if the table is a upsert source when it is works in scan mode.
	 */
	private static boolean isUpsertSource(TableSourceTable table) {
		TableSchema schema = table.catalogTable().getSchema();
		if (!schema.getPrimaryKey().isPresent()) {
			return false;
		}
		DynamicTableSource tableSource = table.tableSource();
		if (tableSource instanceof ScanTableSource) {
			ChangelogMode mode = ((ScanTableSource) tableSource).getChangelogMode();
			return mode.contains(RowKind.UPDATE_AFTER) && !mode.contains(RowKind.UPDATE_BEFORE);
		}
		return false;
	}
}
