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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.resourcemanager.messages.taskexecutor.TMSlotRequestReply;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.taskmanager.Task;

import java.util.Collection;
import java.util.UUID;

/**
 * {@link TaskExecutor} RPC gateway interface
 */
public interface TaskExecutorGateway extends RpcGateway {

	/**
	 * Requests a slot from the TaskManager
	 *
	 * @param slotID slot id for the request
	 * @param allocationID id for the request
	 * @param resourceManagerLeaderID current leader id of the ResourceManager
	 * @return answer to the slot request
	 */
	Future<TMSlotRequestReply> requestSlot(
		SlotID slotID,
		AllocationID allocationID,
		UUID resourceManagerLeaderID,
		@RpcTimeout Time timeout);

	/**
	 * Submit a {@link Task} to the {@link TaskExecutor}.
	 *
	 * @param tdd describing the task to submit
	 * @param jobManagerID identifying the submitting JobManager
	 * @param timeout of the submit operation
	 * @return Future acknowledge of the successful operation
	 */
	Future<Acknowledge> submitTask(
		TaskDeploymentDescriptor tdd,
		ResourceID jobManagerID,
		@RpcTimeout Time timeout);

	/**
	 * Update the task where the given partitions can be found.
	 *
	 * @param executionAttemptID identifying the task
	 * @param partitionInfos telling where the partition can be retrieved from
	 * @return Future acknowledge if the partitions have been successfully updated
	 */
	Future<Acknowledge> updatePartitions(ExecutionAttemptID executionAttemptID, Collection<PartitionInfo> partitionInfos);

	/**
	 * Fail all intermediate result partitions of the given task.
	 *
	 * @param executionAttemptID identifying the task
	 */
	void failPartition(ExecutionAttemptID executionAttemptID);

	/**
	 * Trigger the checkpoint for the given task. The checkpoint is identified by the checkpoint ID
	 * and the checkpoint timestamp.
	 *
	 * @param executionAttemptID identifying the task
	 * @param checkpointID unique id for the checkpoint
	 * @param checkpointTimestamp is the timestamp when the checkpoint has been initiated
	 * @return Future acknowledge if the checkpoint has been successfully triggered
	 */
	Future<Acknowledge> triggerCheckpoint(ExecutionAttemptID executionAttemptID, long checkpointID, long checkpointTimestamp);

	/**
	 * Confirm a checkpoint for the given task. The checkpoint is identified by the checkpoint ID
	 * and the checkpoint timestamp.
	 *
	 * @param executionAttemptID identifying the task
	 * @param checkpointId unique id for the checkpoint
	 * @param checkpointTimestamp is the timestamp when the checkpoint has been initiated
	 * @return Future acknowledge if the checkpoint has been successfully confirmed
	 */
	Future<Acknowledge> confirmCheckpoint(ExecutionAttemptID executionAttemptID, long checkpointId, long checkpointTimestamp);

	/**
	 * Stop the given task.
	 *
	 * @param executionAttemptID identifying the task
	 * @return Future acknowledge if the task is successfully stopped
	 */
	Future<Acknowledge> stopTask(ExecutionAttemptID executionAttemptID);

	/**
	 * Cancel the given task.
	 *
	 * @param executionAttemptID identifying the task
	 * @return Future acknowledge if the task is successfully canceled
	 */
	Future<Acknowledge> cancelTask(ExecutionAttemptID executionAttemptID);
}
