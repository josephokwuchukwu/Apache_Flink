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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.functions.UserDefinedFunction;

import static org.apache.flink.api.common.RuntimeExecutionMode.BATCH;
import static org.apache.flink.api.common.RuntimeExecutionMode.STREAMING;
import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_CATALOG_NAME;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_DATABASE_NAME;

/**
 * Defines all parameters that initialize a table environment. Those parameters are used only during
 * instantiation of a {@link TableEnvironment} and cannot be changed afterwards.
 *
 * <p>Example:
 *
 * <pre>{@code
 * EnvironmentSettings.newInstance()
 *   .inStreamingMode()
 *   .withBuiltInCatalogName("default_catalog")
 *   .withBuiltInDatabaseName("default_database")
 *   .build()
 * }</pre>
 *
 * <p>{@link EnvironmentSettings#inStreamingMode()} or {@link EnvironmentSettings#inBatchMode()}
 * might be convenient as shortcuts.
 */
@PublicEvolving
public class EnvironmentSettings {

    /**
     * Holds all the configuration generated by the builder, together with any required additional
     * configuration.
     */
    private final Configuration configuration;

    private EnvironmentSettings(Configuration configuration) {
        this.configuration = configuration;
    }

    /**
     * Creates a default instance of {@link EnvironmentSettings} in streaming execution mode.
     *
     * <p>In this mode, both bounded and unbounded data streams can be processed.
     *
     * <p>This method is a shortcut for creating a {@link TableEnvironment} with little code. Use
     * the builder provided in {@link EnvironmentSettings#newInstance()} for advanced settings.
     */
    public static EnvironmentSettings inStreamingMode() {
        return EnvironmentSettings.newInstance().inStreamingMode().build();
    }

    /**
     * Creates a default instance of {@link EnvironmentSettings} in batch execution mode.
     *
     * <p>This mode is highly optimized for batch scenarios. Only bounded data streams can be
     * processed in this mode.
     *
     * <p>This method is a shortcut for creating a {@link TableEnvironment} with little code. Use
     * the builder provided in {@link EnvironmentSettings#newInstance()} for advanced settings.
     */
    public static EnvironmentSettings inBatchMode() {
        return EnvironmentSettings.newInstance().inBatchMode().build();
    }

    /** Creates a builder for creating an instance of {@link EnvironmentSettings}. */
    public static Builder newInstance() {
        return new Builder();
    }

    /**
     * Creates an instance of {@link EnvironmentSettings} from configuration.
     *
     * @deprecated use {@link Builder#withConfiguration(Configuration)} instead.
     */
    @Deprecated
    public static EnvironmentSettings fromConfiguration(ReadableConfig configuration) {
        return new EnvironmentSettings((Configuration) configuration);
    }

    /**
     * Convert the environment setting to the {@link Configuration}.
     *
     * @deprecated use {@link #getConfiguration} instead.
     */
    @Deprecated
    public Configuration toConfiguration() {
        return configuration;
    }

    /** Get the underlying {@link Configuration}. */
    public Configuration getConfiguration() {
        return configuration;
    }

    /**
     * Gets the specified name of the initial catalog to be created when instantiating a {@link
     * TableEnvironment}.
     */
    public String getBuiltInCatalogName() {
        return configuration.get(TABLE_CATALOG_NAME);
    }

    /**
     * Gets the specified name of the default database in the initial catalog to be created when
     * instantiating a {@link TableEnvironment}.
     */
    public String getBuiltInDatabaseName() {
        return configuration.get(TABLE_DATABASE_NAME);
    }

    /** Tells if the {@link TableEnvironment} should work in a batch or streaming mode. */
    public boolean isStreamingMode() {
        return configuration.get(RUNTIME_MODE) == STREAMING;
    }

    /** A builder for {@link EnvironmentSettings}. */
    @PublicEvolving
    public static class Builder {

        private final Configuration configuration = new Configuration();

        public Builder() {}

        /** Sets that the components should work in a batch mode. Streaming mode by default. */
        public Builder inBatchMode() {
            configuration.set(RUNTIME_MODE, BATCH);
            return this;
        }

        /** Sets that the components should work in a streaming mode. Enabled by default. */
        public Builder inStreamingMode() {
            configuration.set(RUNTIME_MODE, STREAMING);
            return this;
        }

        /**
         * Specifies the name of the initial catalog to be created when instantiating a {@link
         * TableEnvironment}.
         *
         * <p>This catalog is an in-memory catalog that will be used to store all temporary objects
         * (e.g. from {@link TableEnvironment#createTemporaryView(String, Table)} or {@link
         * TableEnvironment#createTemporarySystemFunction(String, UserDefinedFunction)}) that cannot
         * be persisted because they have no serializable representation.
         *
         * <p>It will also be the initial value for the current catalog which can be altered via
         * {@link TableEnvironment#useCatalog(String)}.
         *
         * <p>Default: {@link TableConfigOptions#TABLE_DATABASE_NAME}{@code .defaultValue()}.
         */
        public Builder withBuiltInCatalogName(String builtInCatalogName) {
            configuration.set(TABLE_CATALOG_NAME, builtInCatalogName);
            return this;
        }

        /**
         * Specifies the name of the default database in the initial catalog to be created when
         * instantiating a {@link TableEnvironment}.
         *
         * <p>This database is an in-memory database that will be used to store all temporary
         * objects (e.g. from {@link TableEnvironment#createTemporaryView(String, Table)} or {@link
         * TableEnvironment#createTemporarySystemFunction(String, UserDefinedFunction)}) that cannot
         * be persisted because they have no serializable representation.
         *
         * <p>It will also be the initial value for the current database which can be altered via
         * {@link TableEnvironment#useDatabase(String)}.
         *
         * <p>Default: {@link TableConfigOptions#TABLE_DATABASE_NAME}{@code .defaultValue()}.
         */
        public Builder withBuiltInDatabaseName(String builtInDatabaseName) {
            configuration.set(TABLE_DATABASE_NAME, builtInDatabaseName);
            return this;
        }

        /** Add extra configuration to {@link EnvironmentSettings}. */
        public Builder withConfiguration(Configuration configuration) {
            this.configuration.addAll(configuration);
            return this;
        }

        /** Returns an immutable instance of {@link EnvironmentSettings}. */
        public EnvironmentSettings build() {
            return new EnvironmentSettings(configuration);
        }
    }
}
