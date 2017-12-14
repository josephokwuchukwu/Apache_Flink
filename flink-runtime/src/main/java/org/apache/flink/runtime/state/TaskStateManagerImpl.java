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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.PrioritizedOperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collections;

/**
 * This class is the default implementation of {@link TaskStateManager} and collaborates with the job manager
 * through {@link CheckpointResponder}) as well as a task-manager-local state store. Like this, client code does
 * not have to deal with the differences between remote or local state on recovery because this class handles both
 * cases transparently.
 *
 * Reported state is tagged by clients so that this class can properly forward to the right receiver for the
 * checkpointed state.
 *
 * TODO: all interaction with local state store must still be implemented! It is currently just a placeholder.
 */
public class TaskStateManagerImpl implements TaskStateManager {

	/** The id of the job for which this manager was created, can report, and recover. */
	private final JobID jobId;

	/** The execution attempt id that this manager reports for. */
	private final ExecutionAttemptID executionAttemptID;

	/** The data given by the job manager to restore the job. This is null for a new job without previous state. */
	@Nullable
	private final JobManagerTaskRestore jobManagerTaskRestore;

	/** The local state store to which this manager reports local state snapshots. */
	private final TaskLocalStateStore localStateStore;

	/** The checkpoint responder through which this manager can report to the job manager. */
	private final CheckpointResponder checkpointResponder;

	public TaskStateManagerImpl(
		@Nonnull JobID jobId,
		@Nonnull ExecutionAttemptID executionAttemptID,
		@Nonnull TaskLocalStateStore localStateStore,
		@Nullable JobManagerTaskRestore jobManagerTaskRestore,
		@Nonnull CheckpointResponder checkpointResponder) {

		this.jobId = jobId;
		this.localStateStore = localStateStore;
		this.jobManagerTaskRestore = jobManagerTaskRestore;
		this.executionAttemptID = executionAttemptID;
		this.checkpointResponder = checkpointResponder;
	}

	@Override
	public void reportTaskStateSnapshots(
		@Nonnull CheckpointMetaData checkpointMetaData,
		@Nonnull CheckpointMetrics checkpointMetrics,
		@Nullable TaskStateSnapshot acknowledgedState,
		@Nullable TaskStateSnapshot localState) {

		long checkpointId = checkpointMetaData.getCheckpointId();

		localStateStore.storeLocalState(checkpointId, localState);

		checkpointResponder.acknowledgeCheckpoint(
			jobId,
			executionAttemptID,
			checkpointId,
			checkpointMetrics,
			acknowledgedState);
	}

	@Nonnull
	@Override
	public PrioritizedOperatorSubtaskState prioritizedOperatorState(OperatorID operatorID) {

		if (jobManagerTaskRestore == null) {
			return PrioritizedOperatorSubtaskState.emptyNotRestored();
		}

		TaskStateSnapshot jobManagerStateSnapshot =
			jobManagerTaskRestore.getTaskStateSnapshot();

		OperatorSubtaskState jobManagerSubtaskState =
			jobManagerStateSnapshot.getSubtaskStateByOperatorID(operatorID);

		if (jobManagerSubtaskState == null) {
			return PrioritizedOperatorSubtaskState.emptyNotRestored();
		}

		TaskStateSnapshot localStateSnapshot =
			localStateStore.retrieveLocalState(jobManagerTaskRestore.getRestoreCheckpointId());

		if (localStateSnapshot != null) {
			OperatorSubtaskState localSubtaskState = localStateSnapshot.getSubtaskStateByOperatorID(operatorID);

			if (localSubtaskState != null) {
				return new PrioritizedOperatorSubtaskState(
					jobManagerSubtaskState,
					Collections.singletonList(localSubtaskState));
			}
		}

		return new PrioritizedOperatorSubtaskState(jobManagerSubtaskState, Collections.emptyList(), true);
	}

	@Nonnull
	@Override
	public LocalRecoveryDirectoryProvider createLocalRecoveryRootDirectoryProvider() {
		return localStateStore.getLocalRecoveryRootDirectoryProvider();
	}

	/**
	 * Tracking when local state can be disposed.
	 */
	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		localStateStore.confirmCheckpoint(checkpointId);
	}
}
