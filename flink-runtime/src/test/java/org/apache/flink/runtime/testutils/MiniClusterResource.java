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

package org.apache.flink.runtime.testutils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER;

/**
 * Resource which starts a {@link MiniCluster} for testing purposes.
 */
public class MiniClusterResource extends ExternalResource {

	private static final String DEFAULT_MANAGED_MEMORY_SIZE = "80m";

	protected final Logger log = LoggerFactory.getLogger(getClass());

	private final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private final MiniClusterResourceConfiguration miniClusterResourceConfiguration;

	private MiniCluster miniCluster = null;

	private int numberSlots = -1;

	private UnmodifiableConfiguration restClusterClientConfig;

	public MiniClusterResource(final MiniClusterResourceConfiguration miniClusterResourceConfiguration) {
		this.miniClusterResourceConfiguration = Preconditions.checkNotNull(miniClusterResourceConfiguration);
	}

	public int getNumberSlots() {
		return numberSlots;
	}

	public MiniCluster getMiniCluster() {
		return miniCluster;
	}

	public UnmodifiableConfiguration getClientConfiguration() {
		return restClusterClientConfig;
	}

	public URI getRestAddres() {
		return miniCluster.getRestAddress();
	}

	@Override
	public void before() throws Exception {
		temporaryFolder.create();

		startMiniCluster();

		numberSlots = miniClusterResourceConfiguration.getNumberSlotsPerTaskManager() * miniClusterResourceConfiguration.getNumberTaskManagers();
	}

	@Override
	public void after() {
		temporaryFolder.delete();

		Exception exception = null;

		if (miniCluster != null) {
			final CompletableFuture<?> terminationFuture = miniCluster.closeAsync();

			try {
				terminationFuture.get(
					miniClusterResourceConfiguration.getShutdownTimeout().toMilliseconds(),
					TimeUnit.MILLISECONDS);
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			miniCluster = null;
		}

		if (exception != null) {
			log.warn("Could not properly shut down the MiniClusterResource.", exception);
		}
	}

	private void startMiniCluster() throws Exception {
		final Configuration configuration = new Configuration(miniClusterResourceConfiguration.getConfiguration());
		configuration.setBoolean(ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
		configuration.setString(CoreOptions.TMP_DIRS, temporaryFolder.newFolder().getAbsolutePath());

		// we need to set this since a lot of test expect this because TestBaseUtils.startCluster()
		// enabled this by default
		if (!configuration.contains(CoreOptions.FILESYTEM_DEFAULT_OVERRIDE)) {
			configuration.setBoolean(CoreOptions.FILESYTEM_DEFAULT_OVERRIDE, true);
		}

		if (!configuration.contains(TaskManagerOptions.MANAGED_MEMORY_SIZE)) {
			configuration.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, DEFAULT_MANAGED_MEMORY_SIZE);
		}

		// set rest and rpc port to 0 to avoid clashes with concurrent MiniClusters
		configuration.setInteger(JobManagerOptions.PORT, 0);
		configuration.setInteger(RestOptions.PORT, 0);

		final MiniClusterConfiguration miniClusterConfiguration = new MiniClusterConfiguration.Builder()
			.setConfiguration(configuration)
			.setNumTaskManagers(miniClusterResourceConfiguration.getNumberTaskManagers())
			.setNumSlotsPerTaskManager(miniClusterResourceConfiguration.getNumberSlotsPerTaskManager())
			.build();

		miniCluster = new MiniCluster(miniClusterConfiguration);

		miniCluster.start();

		final URI restAddress = miniCluster.getRestAddress();
		createClientConfiguration(restAddress);
	}

	private void createClientConfiguration(URI restAddress) {
		Configuration restClientConfig = new Configuration();
		restClientConfig.setString(JobManagerOptions.ADDRESS, restAddress.getHost());
		restClientConfig.setInteger(RestOptions.PORT, restAddress.getPort());
		this.restClusterClientConfig = new UnmodifiableConfiguration(restClientConfig);
	}
}
