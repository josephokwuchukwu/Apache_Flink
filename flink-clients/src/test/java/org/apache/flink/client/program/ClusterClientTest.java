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

package org.apache.flink.client.program;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.DummyActorGateway;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import scala.concurrent.Future;
import scala.concurrent.Future$;
import scala.concurrent.duration.FiniteDuration;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Tests for the {@link ClusterClient}.
 */
public class ClusterClientTest extends TestLogger {

	/**
	 * FLINK-6641
	 *
	 * <p>Tests that the {@link ClusterClient} does not clean up HA data when being shut down.
	 */
	@Test
	public void testClusterClientShutdown() throws Exception {
		Configuration config = new Configuration();
		HighAvailabilityServices highAvailabilityServices = mock(HighAvailabilityServices.class);

		ClusterClient clusterClient = new StandaloneClusterClient(config, highAvailabilityServices);

		clusterClient.shutdown();

		// check that the client does not clean up HA data but closes the services
		verify(highAvailabilityServices, never()).closeAndCleanupAllData();
		verify(highAvailabilityServices).close();
	}

	@Test
	public void testClusterClientStop() throws Exception {
		Configuration config = new Configuration();
		config.setString(JobManagerOptions.ADDRESS, "localhost");

		JobID jobID = new JobID();
		TestStopActorGateway gateway = new TestStopActorGateway(jobID);
		ClusterClient clusterClient = new TestClusterClient(config, gateway);
		try {
			clusterClient.stop(jobID);
			Assert.assertTrue(gateway.messageArrived);
		} finally {
			clusterClient.shutdown();
		}
	}

	@Test
	public void testClusterClientCancel() throws Exception {
		Configuration config = new Configuration();
		config.setString(JobManagerOptions.ADDRESS, "localhost");

		JobID jobID = new JobID();
		TestCancelActorGateway gateway = new TestCancelActorGateway(jobID);
		ClusterClient clusterClient = new TestClusterClient(config, gateway);
		try {
			clusterClient.cancel(jobID);
			Assert.assertTrue(gateway.messageArrived);
		} finally {
			clusterClient.shutdown();
		}
	}

	@Test
	public void testClusterClientCancelWithSavepoint() throws Exception {
		Configuration config = new Configuration();
		config.setString(JobManagerOptions.ADDRESS, "localhost");

		JobID jobID = new JobID();
		String savepointPath = "/test/path";
		TestCancelWithSavepointActorGateway gateway = new TestCancelWithSavepointActorGateway(jobID, savepointPath);
		ClusterClient clusterClient = new TestClusterClient(config, gateway);
		try {
			clusterClient.cancelWithSavepoint(jobID, savepointPath);
			Assert.assertTrue(gateway.messageArrived);
		} finally {
			clusterClient.shutdown();
		}
	}

	private static class TestStopActorGateway extends DummyActorGateway {

		private final JobID expectedJobID;
		private volatile boolean messageArrived = false;

		TestStopActorGateway(JobID expectedJobID) {
			this.expectedJobID = expectedJobID;
		}

		@Override
		public Future<Object> ask(Object message, FiniteDuration timeout) {
			messageArrived = true;
			if (message instanceof JobManagerMessages.StopJob) {
				JobManagerMessages.StopJob stopJob = (JobManagerMessages.StopJob) message;
				Assert.assertEquals(expectedJobID, stopJob.jobID());
				return Future$.MODULE$.successful(new JobManagerMessages.StoppingSuccess(stopJob.jobID()));
			}
			Assert.fail("Expected StopJob message, got: " + message.getClass());
			return null;
		}
	}

	private static class TestCancelActorGateway extends TestActorGateway<JobManagerMessages.CancelJob, JobManagerMessages.CancellationSuccess> {

		private final JobID expectedJobID;

		TestCancelActorGateway(JobID expectedJobID) {
			super(JobManagerMessages.CancelJob.class);
			this.expectedJobID = expectedJobID;
		}

		@Override
		public JobManagerMessages.CancellationSuccess process(JobManagerMessages.CancelJob message) {
			Assert.assertEquals(expectedJobID, message.jobID());
			return new JobManagerMessages.CancellationSuccess(message.jobID(), null);
		}
	}

	private static class TestCancelWithSavepointActorGateway extends TestActorGateway<JobManagerMessages.CancelJobWithSavepoint, JobManagerMessages.CancellationSuccess> {

		private final JobID expectedJobID;
		private final String expectedTargetDirectory;

		TestCancelWithSavepointActorGateway(JobID expectedJobID, String expectedTargetDirectory) {
			super(JobManagerMessages.CancelJobWithSavepoint.class);
			this.expectedJobID = expectedJobID;
			this.expectedTargetDirectory = expectedTargetDirectory;
		}

		@Override
		public JobManagerMessages.CancellationSuccess process(JobManagerMessages.CancelJobWithSavepoint message) {
			Assert.assertEquals(expectedJobID, message.jobID());
			Assert.assertEquals(expectedTargetDirectory, message.savepointDirectory());
			return new JobManagerMessages.CancellationSuccess(message.jobID(), null);
		}
	}

	private static class TestClusterClient extends StandaloneClusterClient {

		private final ActorGateway jobmanagerGateway;

		TestClusterClient(Configuration config, ActorGateway jobmanagerGateway) throws Exception {
			super(config);
			this.jobmanagerGateway = jobmanagerGateway;
		}

		@Override
		public ActorGateway getJobManagerGateway() {
			return jobmanagerGateway;
		}
	}

	/**
	 * Utility class for hiding akka/scala details.
	 *
	 * @param <M> expected type of incoming requests
	 * @param <R> type of outgoing requests
	 */
	private abstract static class TestActorGateway<M, R> extends DummyActorGateway {
		private final Class<M> messageClass;
		volatile boolean messageArrived = false;

		TestActorGateway(Class<M> messageClass) {
			this.messageClass = messageClass;
		}

		@Override
		@SuppressWarnings("unchecked")
		public Future<Object> ask(Object message, FiniteDuration timeout) {
			messageArrived = true;
			if (message.getClass().isAssignableFrom(messageClass)) {
				return Future$.MODULE$.successful(process((M) message));
			}
			Assert.fail("Expected TriggerSavepoint message, got: " + message.getClass());
			return null;
		}

		/**
		 * Processes the incoming message and verifies it's correctness. Implementations may directly throw unchecked
		 * exceptions (like JUnit asserts) in case of errors or faulty behaviors.
		 *
		 * @param message incoming message
		 * @return response in case of success
		 */
		public abstract R process(M message);
	}
}
