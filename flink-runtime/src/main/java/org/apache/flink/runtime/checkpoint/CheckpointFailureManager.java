/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.util.FlinkRuntimeException;

import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The checkpoint failure manager which centralized manage checkpoint failure processing logic.
 */
public class CheckpointFailureManager {

	private final static int UNLIMITED_TOLERABLE_FAILURE_NUMBER = Integer.MAX_VALUE;

	private final int tolerableCpFailureNumber;
	private final FailJobCallback failureCallback;
	private final AtomicInteger continuousFailureCounter;

	public CheckpointFailureManager(int tolerableCpFailureNumber, FailJobCallback failureCallback) {
		checkArgument(tolerableCpFailureNumber >= 0
				&& tolerableCpFailureNumber <= UNLIMITED_TOLERABLE_FAILURE_NUMBER,
			"The tolerable checkpoint failure number is illegal, " +
				"it must be greater than or equal to 0 and less than or equal to " + UNLIMITED_TOLERABLE_FAILURE_NUMBER + ".");
		this.tolerableCpFailureNumber = tolerableCpFailureNumber;
		this.continuousFailureCounter = new AtomicInteger(0);
		this.failureCallback = checkNotNull(failureCallback);
	}

	/**
	 * Handle checkpoint exception with a handler callback.
	 *
	 * @param exception the checkpoint exception.
	 * @param checkpointId the failed checkpoint id used to count the continuous failure number based on
	 *                     checkpoint id sequence. In trigger phase, we may not get the checkpoint id when the failure
	 *                     happens before the checkpoint id generation. In this case, it will be specified a negative
	 *                      latest generated checkpoint id as a special flag.
	 */
	public void handleCheckpointException(CheckpointException exception, long checkpointId) {
		CheckpointFailureReason reason = exception.getCheckpointFailureReason();
		switch (reason) {
			case PERIODIC_SCHEDULER_SHUTDOWN:
			case ALREADY_QUEUED:
			case TOO_MANY_CONCURRENT_CHECKPOINTS:
			case MINIMUM_TIME_BETWEEN_CHECKPOINTS:
			case NOT_ALL_REQUIRED_TASKS_RUNNING:
			case CHECKPOINT_SUBSUMED:
			case CHECKPOINT_COORDINATOR_SUSPEND:
			case CHECKPOINT_COORDINATOR_SHUTDOWN:
			case JOB_FAILURE:
			case JOB_FAILOVER_REGION:
			//for compatibility purposes with user job behavior
			case CHECKPOINT_DECLINED_TASK_NOT_READY:
				//ignore
				break;

			case EXCEPTION:
			case CHECKPOINT_EXPIRED:
			case CHECKPOINT_DECLINED:
			case TASK_CHECKPOINT_FAILURE:
			case TRIGGER_CHECKPOINT_FAILURE:
			case FINALIZE_CHECKPOINT_FAILURE:
				continuousFailureCounter.incrementAndGet();
				break;

			default:
				throw new FlinkRuntimeException("Unknown checkpoint failure reason : " + reason.name());
		}

		if (tolerableCpFailureNumber != UNLIMITED_TOLERABLE_FAILURE_NUMBER
			&& continuousFailureCounter.get() > tolerableCpFailureNumber) {
			//clear the counter
			continuousFailureCounter.set(0);
			failureCallback.failJob();
		}
	}

	/**
	 * Handle checkpoint success.
	 *
	 * @param checkpointId the failed checkpoint id used to count the continuous failure number based on
	 *                     checkpoint id sequence.
	 */
	public void handleCheckpointSuccess(long checkpointId) {
		continuousFailureCounter.set(0);
	}

	/**
	 * A callback interface about how to fail a job.
	 */
	public interface FailJobCallback {

		void failJob();

	}

}
