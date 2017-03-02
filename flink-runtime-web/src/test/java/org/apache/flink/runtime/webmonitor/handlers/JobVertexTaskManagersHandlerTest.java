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
package org.apache.flink.runtime.webmonitor.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.utils.ArchivedJobGenerationUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JobVertexTaskManagersHandlerTest {
	@Test
	public void testJsonGeneration() throws Exception {
		AccessExecutionJobVertex originalTask = ArchivedJobGenerationUtils.getTestTask();
		AccessExecutionVertex originalSubtask = ArchivedJobGenerationUtils.getTestSubtask();
		String json = JobVertexTaskManagersHandler.createVertexDetailsByTaskManagerJson(
			originalTask, ArchivedJobGenerationUtils.getTestJob().getJobID().toString(), null);

		JsonNode result = ArchivedJobGenerationUtils.mapper.readTree(json);

		assertEquals(originalTask.getJobVertexId().toString(), result.get("id").asText());
		assertEquals(originalTask.getName(), result.get("name").asText());
		assertTrue(result.get("now").asLong() > 0);

		ArrayNode taskmanagers = (ArrayNode) result.get("taskmanagers");

		JsonNode taskManager = taskmanagers.get(0);

		TaskManagerLocation location = originalSubtask.getCurrentAssignedResourceLocation();
		String expectedLocationString = location.getHostname() + ':' + location.dataPort();
		assertEquals(expectedLocationString, taskManager.get("host").asText());
		assertEquals(ExecutionState.FINISHED.name(), taskManager.get("status").asText());

		assertEquals(3, taskManager.get("start-time").asLong());
		assertEquals(5, taskManager.get("end-time").asLong());
		assertEquals(2, taskManager.get("duration").asLong());

		JsonNode statusCounts = taskManager.get("status-counts");
		assertEquals(0, statusCounts.get(ExecutionState.CREATED.name()).asInt());
		assertEquals(0, statusCounts.get(ExecutionState.SCHEDULED.name()).asInt());
		assertEquals(0, statusCounts.get(ExecutionState.DEPLOYING.name()).asInt());
		assertEquals(0, statusCounts.get(ExecutionState.RUNNING.name()).asInt());
		assertEquals(1, statusCounts.get(ExecutionState.FINISHED.name()).asInt());
		assertEquals(0, statusCounts.get(ExecutionState.CANCELING.name()).asInt());
		assertEquals(0, statusCounts.get(ExecutionState.CANCELED.name()).asInt());
		assertEquals(0, statusCounts.get(ExecutionState.FAILED.name()).asInt());

		long expectedNumBytesIn = 0;
		long expectedNumBytesOut = 0;
		long expectedNumRecordsIn = 0;
		long expectedNumRecordsOut = 0;

		for (AccessExecutionVertex vertex : originalTask.getTaskVertices()) {
			IOMetrics ioMetrics = vertex.getCurrentExecutionAttempt().getIOMetrics();

			expectedNumBytesIn += ioMetrics.getNumBytesInLocal() + ioMetrics.getNumBytesInRemote();
			expectedNumBytesOut += ioMetrics.getNumBytesOut();
			expectedNumRecordsIn += ioMetrics.getNumRecordsIn();
			expectedNumRecordsOut += ioMetrics.getNumRecordsOut();
		}

		JsonNode metrics = taskManager.get("metrics");

		assertEquals(expectedNumBytesIn, metrics.get("read-bytes").asLong());
		assertEquals(expectedNumBytesOut, metrics.get("write-bytes").asLong());
		assertEquals(expectedNumRecordsIn, metrics.get("read-records").asLong());
		assertEquals(expectedNumRecordsOut, metrics.get("write-records").asLong());
	}
}
