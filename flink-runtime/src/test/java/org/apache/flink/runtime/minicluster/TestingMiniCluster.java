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

package org.apache.flink.runtime.minicluster;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.MemoryArchivedExecutionGraphStore;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponent;
import org.apache.flink.runtime.entrypoint.component.SessionDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * {@link MiniCluster} extension which allows to set a custom {@link HighAvailabilityServices}.
 */
public class TestingMiniCluster extends MiniCluster {

	private final int numberDispatcherResourceManagerComponents;

	private final boolean localCommunication;

	private final List<ResourceID> taskManagersInOrder;

	private final Object manipulationLock;

	@Nullable
	private final Supplier<HighAvailabilityServices> highAvailabilityServicesSupplier;

	public TestingMiniCluster(
			TestingMiniClusterConfiguration miniClusterConfiguration,
			@Nullable Supplier<HighAvailabilityServices> highAvailabilityServicesSupplier) {
		super(miniClusterConfiguration);
		this.numberDispatcherResourceManagerComponents = miniClusterConfiguration.getNumberDispatcherResourceManagerComponents();
		this.highAvailabilityServicesSupplier = highAvailabilityServicesSupplier;
		this.localCommunication = miniClusterConfiguration.isLocalCommunication();
		this.taskManagersInOrder = new LinkedList<>();
		this.manipulationLock = getManipulationLock();
	}

	public TestingMiniCluster(TestingMiniClusterConfiguration miniClusterConfiguration) {
		this(miniClusterConfiguration, null);
	}

	@Nonnull
	@Override
	public Collection<DispatcherResourceManagerComponent<?>> getDispatcherResourceManagerComponents() {
		return super.getDispatcherResourceManagerComponents();
	}

	public CompletableFuture<Void> terminateAndRemoveTaskExecutor(int index) {
		synchronized (manipulationLock) {
			final ResourceID taskManagerId = taskManagersInOrder.remove(index);
			final TaskExecutor taskExecutor = getTaskManagers().remove(taskManagerId);
			if (taskExecutor == null) {
				throw new RuntimeException("Task manager with index " + index + " has been shut down");
			}
			return taskExecutor.closeAsync();
		}
	}

	@Override
	public ResourceID startTaskExecutor() throws Exception {
		synchronized (manipulationLock) {
			final ResourceID taskManagerId = super.startTaskExecutor();
			taskManagersInOrder.add(taskManagerId);
			return taskManagerId;
		}
	}

	@Override
	protected boolean useLocalCommunication() {
		return localCommunication;
	}

	@Override
	protected HighAvailabilityServices createHighAvailabilityServices(Configuration configuration, Executor executor) throws Exception {
		if (highAvailabilityServicesSupplier != null) {
			return highAvailabilityServicesSupplier.get();
		} else {
			return super.createHighAvailabilityServices(configuration, executor);
		}
	}

	@Override
	protected Collection<? extends DispatcherResourceManagerComponent<?>> createDispatcherResourceManagerComponents(
			Configuration configuration,
			RpcServiceFactory rpcServiceFactory,
			HighAvailabilityServices haServices,
			BlobServer blobServer,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			MetricQueryServiceRetriever metricQueryServiceRetriever,
			FatalErrorHandler fatalErrorHandler) throws Exception {
		SessionDispatcherResourceManagerComponentFactory dispatcherResourceManagerComponentFactory = createTestingDispatcherResourceManagerComponentFactory();

		final List<DispatcherResourceManagerComponent<?>> result = new ArrayList<>(numberDispatcherResourceManagerComponents);

		for (int i = 0; i < numberDispatcherResourceManagerComponents; i++) {
			result.add(
				dispatcherResourceManagerComponentFactory.create(
					configuration,
					rpcServiceFactory.createRpcService(),
					haServices,
					blobServer,
					heartbeatServices,
					metricRegistry,
					new MemoryArchivedExecutionGraphStore(),
					metricQueryServiceRetriever,
					fatalErrorHandler));
		}

		return result;
	}

	@Override
	public CompletableFuture<DispatcherGateway> getDispatcherGatewayFuture() {
		return super.getDispatcherGatewayFuture();
	}

	protected Collection<? extends CompletableFuture<Void>> terminateTaskExecutors() {
		synchronized (manipulationLock) {
			taskManagersInOrder.clear();
			return super.terminateTaskExecutors();
		}
	}

	private SessionDispatcherResourceManagerComponentFactory createTestingDispatcherResourceManagerComponentFactory() {
		return new SessionDispatcherResourceManagerComponentFactory(
			SessionDispatcherWithUUIDFactory.INSTANCE,
			StandaloneResourceManagerWithUUIDFactory.INSTANCE);
	}
}
