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

package org.apache.flink.table.filesystem;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.factories.BulkFormatFactory;
import org.apache.flink.table.factories.BulkWriterFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.EncoderFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_DEFAULT_NAME;
import static org.apache.flink.table.filesystem.FileSystemOptions.PATH;

/**
 * Abstract File system table for providing some common methods.
 */
abstract class AbstractFileSystemTable {

	final DynamicTableFactory.Context context;
	final ObjectIdentifier tableIdentifier;
	final Configuration tableOptions;
	final TableSchema schema;
	final List<String> partitionKeys;
	final Path path;
	final String defaultPartName;

	AbstractFileSystemTable(DynamicTableFactory.Context context) {
		this.context = context;
		this.tableIdentifier = context.getObjectIdentifier();
		this.tableOptions = new Configuration();
		context.getCatalogTable().getOptions().forEach(tableOptions::setString);
		this.schema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
		this.partitionKeys = context.getCatalogTable().getPartitionKeys();
		this.path = new Path(tableOptions.get(PATH));
		this.defaultPartName = tableOptions.get(PARTITION_DEFAULT_NAME);
	}

	ReadableConfig formatOptions(String identifier) {
		return new DelegatingConfiguration(tableOptions, identifier + ".");
	}

	FileSystemFormatFactory createFormatFactory() {
		return FactoryUtil.discoverFactory(
				Thread.currentThread().getContextClassLoader(),
				FileSystemFormatFactory.class,
				tableOptions.get(FactoryUtil.FORMAT));
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	Optional<BulkFormatFactory> createBulkFormatFactory() {
		return tryCreateFormatFactory(BulkFormatFactory.class);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	Optional<BulkWriterFactory> createBulkWriterFactory() {
		return tryCreateFormatFactory(BulkWriterFactory.class);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	Optional<EncoderFactory> createEncoderFactory() {
		return tryCreateFormatFactory(EncoderFactory.class);
	}

	<T extends Factory> Optional<T> tryCreateFormatFactory(Class<T> clazz) {
		String format = tableOptions.get(FactoryUtil.FORMAT);
		if (format == null) {
			throw new ValidationException(String.format(
					"Table options do not contain an option key '%s' for discovering a format.",
					FactoryUtil.FORMAT.key()));
		}
		try {
			return Optional.of(FactoryUtil.discoverFactory(
					Thread.currentThread().getContextClassLoader(),
					clazz,
					format));
		} catch (ValidationException e) {
			return Optional.empty();
		}
	}
}
