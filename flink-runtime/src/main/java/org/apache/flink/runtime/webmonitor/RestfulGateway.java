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

package org.apache.flink.runtime.webmonitor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.client.SerializedJobExecutionResult;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.metrics.dump.MetricQueryService;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.types.Either;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Gateway for restful endpoints.
 *
 * <p>Gateways which implement this method run a REST endpoint which is reachable
 * under the returned address.
 */
public interface RestfulGateway extends RpcGateway {

	/**
	 * Requests the REST address of this {@link RpcEndpoint}.
	 *
	 * @param timeout for this operation
	 * @return Future REST endpoint address
	 */
	CompletableFuture<String> requestRestAddress(@RpcTimeout  Time timeout);

	/**
	 * Requests the AccessExecutionGraph for the given jobId. If there is no such graph, then
	 * the future is completed with a {@link FlinkJobNotFoundException}.
	 *
	 * @param jobId identifying the job whose AccessExecutionGraph is requested
	 * @param timeout for the asynchronous operation
	 * @return Future containing the AccessExecutionGraph for the given jobId, otherwise {@link FlinkJobNotFoundException}
	 */
	CompletableFuture<AccessExecutionGraph> requestJob(JobID jobId, @RpcTimeout Time timeout);

	/**
	 * Requests job details currently being executed on the Flink cluster.
	 *
	 * @param timeout for the asynchronous operation
	 * @return Future containing the job details
	 */
	CompletableFuture<MultipleJobsDetails> requestMultipleJobDetails(
		@RpcTimeout Time timeout);

	/**
	 * Requests the cluster status overview.
	 *
	 * @param timeout for the asynchronous operation
	 * @return Future containing the status overview
	 */
	CompletableFuture<ClusterOverview> requestClusterOverview(@RpcTimeout Time timeout);

	/**
	 * Requests the paths for the {@link MetricQueryService} to query.
	 *
	 * @param timeout for the asynchronous operation
	 * @return Future containing the collection of metric query service paths to query
	 */
	CompletableFuture<Collection<String>> requestMetricQueryServicePaths(@RpcTimeout Time timeout);

	/**
	 * Requests the paths for the TaskManager's {@link MetricQueryService} to query.
	 *
	 * @param timeout for the asynchronous operation
	 * @return Future containing the collection of instance ids and the corresponding metric query service path
	 */
	CompletableFuture<Collection<Tuple2<ResourceID, String>>> requestTaskManagerMetricQueryServicePaths(@RpcTimeout Time timeout);

	/**
	 * Returns the {@link SerializedJobExecutionResult} for a job, or in case the job failed, the
	 * failure cause.
	 *
	 * @param jobId ID of the job that we are interested in.
	 * @param timeout Timeout for the asynchronous operation.
	 *
	 * @see #isJobExecutionResultPresent(JobID, Time)
	 *
	 * @return {@link CompletableFuture} containing the {@link SerializedJobExecutionResult} or a
	 * {@link Throwable} which represents the failure cause. If there is no result, the future will
	 * be completed exceptionally with
	 * {@link org.apache.flink.runtime.messages.JobExecutionResultNotFoundException}
	 */
	default CompletableFuture<Either<Throwable, SerializedJobExecutionResult>> getJobExecutionResult(
		JobID jobId,
		@RpcTimeout Time timeout) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Tests if the {@link SerializedJobExecutionResult} is present.
	 *
	 * @param jobId ID of the job that we are interested in.
	 * @param timeout Timeout for the asynchronous operation.
	 *
	 * @see #getJobExecutionResult(JobID, Time)
	 *
	 * @return {@link CompletableFuture} containing {@code true} when then the
	 * {@link SerializedJobExecutionResult} is present. The future is completed exceptionally with
	 * {@link FlinkJobNotFoundException} if there is no job running with the specified ID, or if the
	 * result has expired.
	 */
	default CompletableFuture<Boolean> isJobExecutionResultPresent(
		JobID jobId, @RpcTimeout Time timeout) {
		throw new UnsupportedOperationException();
	}
}
