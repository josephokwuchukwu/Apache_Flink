/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.plugin;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;

import java.io.File;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Stores the configuration for plugins mechanism.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class PluginConfig {
	private final Optional<Path> pluginsPath;

	private final String[] alwaysParentFirstPatterns;

	private PluginConfig(Optional<Path> pluginsPath, String[] alwaysParentFirstPatterns) {
		this.pluginsPath = pluginsPath;
		this.alwaysParentFirstPatterns = alwaysParentFirstPatterns;
	}

	public Optional<Path> getPluginsPath() {
		return pluginsPath;
	}

	public String[] getAlwaysParentFirstPatterns() {
		return alwaysParentFirstPatterns;
	}

	public static PluginConfig fromConfiguration(Configuration configuration) {
		return new PluginConfig(
			getPluginsDirPath(configuration),
			CoreOptions.getParentFirstLoaderPatterns(configuration));
	}

	private static Optional<Path> getPluginsDirPath(Configuration configuration) {
		String pluginsDir = configuration.getString(ConfigConstants.ENV_FLINK_PLUGINS_DIR, null);
		if (pluginsDir == null) {
			return Optional.empty();
		}
		File pluginsDirFile = new File(pluginsDir);
		if (!pluginsDirFile.isDirectory()) {
			return Optional.empty();
		}
		return Optional.of(pluginsDirFile.toPath());
	}
}
