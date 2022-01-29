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

package org.apache.flink.connector.pulsar.sink.writer.serializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.connector.pulsar.common.schema.PulsarSchema;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.sink.writer.context.PulsarSinkContext;
import org.apache.flink.connector.pulsar.sink.writer.message.RawMessage;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;

import java.io.Serializable;

/**
 * The serialization schema for how to serialize record into Pulsar.
 *
 * @param <IN> The message type send to Pulsar.
 */
@PublicEvolving
public interface PulsarSerializationSchema<IN> extends Serializable {

    /**
     * Initialization method for the schema. It is called before the actual working methods {@link
     * #serialize(Object, PulsarSinkContext)} and thus suitable for one time setup work.
     *
     * <p>The provided {@link InitializationContext} can be used to access additional features such
     * as e.g. registering user metrics.
     *
     * @param initializationContext Contextual information that can be used during initialization.
     * @param sinkContext runtime information i.e. partitions, subtaskId
     * @param sinkConfiguration All the configure options for Pulsar sink. You can add custom
     *     options.
     */
    default void open(
            InitializationContext initializationContext,
            PulsarSinkContext sinkContext,
            SinkConfiguration sinkConfiguration)
            throws Exception {}

    /**
     * Serializes given element and returns it as a {@link RawMessage}.
     *
     * @param element element to be serialized
     * @param sinkContext context to provide extra information.
     */
    RawMessage<byte[]> serialize(IN element, PulsarSinkContext sinkContext);

    /** @return The related Pulsar Schema for this serializer. */
    default Schema<IN> schema() {
        throw new UnsupportedOperationException(
                "Implement this method if you need Pulsar schema evolution.");
    }

    /**
     * Create a PulsarSerializationSchema by using the flink's {@link SerializationSchema}. It would
     * consume the pulsar message as byte array and decode the message by using flink's logic.
     */
    static <T> PulsarSerializationSchema<T> flinkSchema(
            SerializationSchema<T> serializationSchema) {
        return new PulsarSerializationSchemaWrapper<>(serializationSchema);
    }

    /**
     * Create a PulsarSerializationSchema by using the Pulsar {@link Schema} instance. The message
     * bytes must be encoded by pulsar Schema.
     *
     * <p>We only support <a
     * href="https://pulsar.apache.org/docs/en/schema-understand/#primitive-type">primitive
     * types</a> here.
     */
    static <T> PulsarSerializationSchema<T> pulsarSchema(Schema<T> schema) {
        PulsarSchema<T> pulsarSchema = new PulsarSchema<>(schema);
        return new PulsarSchemaWrapper<>(pulsarSchema);
    }

    /**
     * Create a PulsarSerializationSchema by using the Pulsar {@link Schema} instance. The message
     * bytes must be encoded by pulsar Schema.
     *
     * <p>We only support <a
     * href="https://pulsar.apache.org/docs/en/schema-understand/#struct">struct types</a> here.
     */
    static <T> PulsarSerializationSchema<T> pulsarSchema(Schema<T> schema, Class<T> typeClass) {
        PulsarSchema<T> pulsarSchema = new PulsarSchema<>(schema, typeClass);
        return new PulsarSchemaWrapper<>(pulsarSchema);
    }

    /**
     * Create a PulsarSerializationSchema by using the Pulsar {@link Schema} instance. The message
     * bytes must be encoded by pulsar Schema.
     *
     * <p>We only support <a
     * href="https://pulsar.apache.org/docs/en/schema-understand/#keyvalue">keyvalue types</a> here.
     */
    static <K, V> PulsarSerializationSchema<KeyValue<K, V>> pulsarSchema(
            Schema<KeyValue<K, V>> schema, Class<K> keyClass, Class<V> valueClass) {
        PulsarSchema<KeyValue<K, V>> pulsarSchema =
                new PulsarSchema<>(schema, keyClass, valueClass);
        return new PulsarSchemaWrapper<>(pulsarSchema);
    }
}
