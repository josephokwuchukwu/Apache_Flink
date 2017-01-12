/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.elasticsearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;

import java.io.File;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class EmbeddedElasticsearchNodeEnvironmentImpl implements EmbeddedElasticsearchNodeEnvironment {

	private Node node;

	@Override
	public void start(File tmpDataFolder, String clusterName) throws Exception {
		if (node == null) {
			node = nodeBuilder()
				.settings(ImmutableSettings.settingsBuilder()
					.put("http.enabled", false)
					.put("path.data", tmpDataFolder.getAbsolutePath()))
				.clusterName(clusterName)
				.local(true)
				.node();

			node.start();
		}
	}

	@Override
	public void close() throws Exception {
		if (node != null && !node.isClosed()) {
			node.close();
			node = null;
		}
	}

	@Override
	public Client getClient() {
		return node.client();
	}
}
