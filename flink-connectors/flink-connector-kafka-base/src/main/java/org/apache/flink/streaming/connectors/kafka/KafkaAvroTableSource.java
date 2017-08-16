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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.avro.AvroRowDeserializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedFieldMapping;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.Utf8;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A version-agnostic Kafka Avro {@link StreamTableSource}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and
 * override {@link #getKafkaConsumer(String, Properties, DeserializationSchema)}}.
 */
public abstract class KafkaAvroTableSource extends KafkaTableSource implements DefinedFieldMapping {

	private final Class<? extends SpecificRecordBase> avroRecordClass;

	private Map<String, String> fieldMapping;

	/**
	 * Creates a generic Kafka Avro {@link StreamTableSource} using a given {@link SpecificRecord}.
	 *
	 * @param topic            Kafka topic to consume.
	 * @param properties       Properties for the Kafka consumer.
	 * @param schema           Schema of the produced table.
	 * @param avroRecordClass  Class of the Avro record that is read from the Kafka topic.
	 */
	protected KafkaAvroTableSource(
		String topic,
		Properties properties,
		TableSchema schema,
		Class<? extends SpecificRecordBase> avroRecordClass) {

		super(
			topic,
			properties,
			schema,
			convertToRowTypeInformation(avroRecordClass));

		this.avroRecordClass = avroRecordClass;
	}

	@Override
	public Map<String, String> getFieldMapping() {
		return fieldMapping;
	}

	@Override
	public String explainSource() {
		return "KafkaAvroTableSource(" + this.avroRecordClass.getSimpleName() + ")";
	}

	@Override
	protected AvroRowDeserializationSchema getDeserializationSchema() {
		return new AvroRowDeserializationSchema(avroRecordClass);
	}

	//////// SETTERS FOR OPTIONAL PARAMETERS

	/**
	 * Configures a field mapping for this TableSource.
	 *
	 * @param fieldMapping The field mapping.
	 */
	protected void setFieldMapping(Map<String, String> fieldMapping) {
		this.fieldMapping = fieldMapping;
	}

	//////// HELPER METHODS

	/**
	 * Converts the extracted AvroTypeInfo into a RowTypeInfo nested structure with deterministic field order.
	 * Replaces generic Utf8 with basic String type information.
	 */
	@SuppressWarnings("unchecked")
	private static <T extends SpecificRecordBase> TypeInformation<Row> convertToRowTypeInformation(Class<T> avroClass) {
		final AvroTypeInfo<T> avroTypeInfo = new AvroTypeInfo<>(avroClass);
		// determine schema to retrieve deterministic field order
		final Schema schema = SpecificData.get().getSchema(avroClass);
		return (TypeInformation<Row>) convertToTypeInformation(avroTypeInfo, schema);
	}

	/**
	 * Recursively converts extracted AvroTypeInfo into a RowTypeInfo nested structure with deterministic field order.
	 * Replaces generic Utf8 with basic String type information.
	 */
	private static TypeInformation<?> convertToTypeInformation(TypeInformation<?> extracted, Schema schema) {
		if (schema.getType() == Schema.Type.RECORD) {
			final List<Schema.Field> fields = schema.getFields();
			final AvroTypeInfo<?> avroTypeInfo = (AvroTypeInfo<?>) extracted;

			final TypeInformation<?>[] types = new TypeInformation<?>[fields.size()];
			final String[] names = new String[fields.size()];
			for (int i = 0; i < fields.size(); i++) {
				final Schema.Field field = fields.get(i);
				types[i] = convertToTypeInformation(avroTypeInfo.getTypeAt(field.name()), field.schema());
				names[i] = field.name();
			}
			return new RowTypeInfo(types, names);
		} else if (extracted instanceof GenericTypeInfo<?>) {
			final GenericTypeInfo<?> genericTypeInfo = (GenericTypeInfo<?>) extracted;
			if (genericTypeInfo.getTypeClass() == Utf8.class) {
				return BasicTypeInfo.STRING_TYPE_INFO;
			}
		}
		return extracted;
	}

	/**
	 * Abstract builder for a {@link KafkaAvroTableSource} to be extended by builders of subclasses of
	 * KafkaAvroTableSource.
	 *
	 * @param <T> Type of the KafkaAvroTableSource produced by the builder.
	 * @param <B> Type of the KafkaAvroTableSource.Builder subclass.
	 */
	protected abstract static class Builder<T extends KafkaAvroTableSource, B extends KafkaAvroTableSource.Builder>
		extends KafkaTableSource.Builder<T, B> {

		private Class<? extends SpecificRecordBase> avroClass;

		private Map<String, String> fieldMapping;

		/**
		 * Sets the class of the Avro records that aree read from the Kafka topic.
		 *
		 * @param avroClass The class of the Avro records that are read from the Kafka topic.
		 * @return The builder.
		 */
		public B forAvroRecordClass(Class<? extends SpecificRecordBase> avroClass) {
			this.avroClass = avroClass;
			return builder();
		}

		/**
		 * Sets a mapping from schema fields to fields of the produced Avro record.
		 *
		 * <p>A field mapping is required if the fields of produced tables should be named different than
		 * the fields of the Avro record.
		 * The key of the provided Map refers to the field of the table schema,
		 * the value to the field of the Avro record.</p>
		 *
		 * @param schemaToAvroMapping A mapping from schema fields to Avro fields.
		 * @return The builder.
		 */
		public B withTableToAvroMapping(Map<String, String> schemaToAvroMapping) {
			this.fieldMapping = schemaToAvroMapping;
			return builder();
		}

		/**
		 * Returns the configured Avro class.
		 *
		 * @return The configured Avro class.
		 */
		protected Class<? extends SpecificRecordBase> getAvroRecordClass() {
			return this.avroClass;
		}

		@Override
		protected void configureTableSource(T source) {
			super.configureTableSource(source);
			source.setFieldMapping(this.fieldMapping);
		}
	}
}
