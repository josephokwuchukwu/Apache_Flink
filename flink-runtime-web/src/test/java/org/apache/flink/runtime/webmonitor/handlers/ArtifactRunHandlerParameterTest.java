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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.testutils.ParameterProgram;

import org.junit.Assert;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Tests for the parameter handling of the {@link ArtifactRunHandler}.
 */
public class ArtifactRunHandlerParameterTest
	extends ArtifactHandlerParameterTest<ArtifactRunRequestBody, ArtifactRunMessageParameters> {
	private static final boolean ALLOW_NON_RESTORED_STATE_QUERY = true;
	private static final String RESTORE_PATH = "/foo/bar";

	private static ArtifactRunHandler handler;

	@BeforeClass
	public static void setup() throws Exception {
		init();
		final GatewayRetriever<TestingDispatcherGateway> gatewayRetriever = () -> CompletableFuture.completedFuture(restfulGateway);
		final Time timeout = Time.seconds(10);
		final Map<String, String> responseHeaders = Collections.emptyMap();
		final Executor executor = TestingUtils.defaultExecutor();

		handler = new ArtifactRunHandler(
			gatewayRetriever,
			timeout,
			responseHeaders,
			ArtifactRunHeaders.getInstance(),
			artifactDir,
			new Configuration(),
			executor);
	}

	@Override
	ArtifactRunMessageParameters getUnresolvedArtifactMessageParameters() {
		return handler.getMessageHeaders().getUnresolvedMessageParameters();
	}

	@Override
	ArtifactRunMessageParameters getArtifactMessageParameters(ProgramArgsParType programArgsParType) {
		final ArtifactRunMessageParameters parameters = getUnresolvedArtifactMessageParameters();
		parameters.allowNonRestoredStateQueryParameter.resolve(Collections.singletonList(ALLOW_NON_RESTORED_STATE_QUERY));
		parameters.savepointPathQueryParameter.resolve(Collections.singletonList(RESTORE_PATH));
		parameters.entryClassQueryParameter.resolve(Collections.singletonList(ParameterProgram.class.getCanonicalName()));
		parameters.parallelismQueryParameter.resolve(Collections.singletonList(PARALLELISM));
		if (programArgsParType == ProgramArgsParType.String ||
			programArgsParType == ProgramArgsParType.Both) {
			parameters.programArgsQueryParameter.resolve(Collections.singletonList(String.join(" ", PROG_ARGS)));
		}
		if (programArgsParType == ProgramArgsParType.List ||
			programArgsParType == ProgramArgsParType.Both) {
			parameters.programArgQueryParameter.resolve(Arrays.asList(PROG_ARGS));
		}
		return parameters;
	}

	@Override
	ArtifactRunMessageParameters getWrongArtifactMessageParameters(ProgramArgsParType programArgsParType) {
		List<String> wrongArgs = Arrays.stream(PROG_ARGS).map(a -> a + "wrong").collect(Collectors.toList());
		String argsWrongStr = String.join(" ", wrongArgs);
		final ArtifactRunMessageParameters parameters = getUnresolvedArtifactMessageParameters();
		parameters.allowNonRestoredStateQueryParameter.resolve(Collections.singletonList(false));
		parameters.savepointPathQueryParameter.resolve(Collections.singletonList("/no/uh"));
		parameters.entryClassQueryParameter.resolve(Collections.singletonList("please.dont.run.me"));
		parameters.parallelismQueryParameter.resolve(Collections.singletonList(64));
		if (programArgsParType == ProgramArgsParType.String ||
			programArgsParType == ProgramArgsParType.Both) {
			parameters.programArgsQueryParameter.resolve(Collections.singletonList(argsWrongStr));
		}
		if (programArgsParType == ProgramArgsParType.List ||
			programArgsParType == ProgramArgsParType.Both) {
			parameters.programArgQueryParameter.resolve(wrongArgs);
		}
		return parameters;
	}

	@Override
	ArtifactRunRequestBody getDefaultArtifactRequestBody() {
		return new ArtifactRunRequestBody();
	}

	@Override
	ArtifactRunRequestBody getArtifactRequestBody(ProgramArgsParType programArgsParType) {
		return new ArtifactRunRequestBody(
			ParameterProgram.class.getCanonicalName(),
			getProgramArgsString(programArgsParType),
			getProgramArgsList(programArgsParType),
			PARALLELISM,
			null,
			ALLOW_NON_RESTORED_STATE_QUERY,
			RESTORE_PATH
		);
	}

	@Override
	ArtifactRunRequestBody getArtifactRequestBodyWithJobId(JobID jobId) {
		return new ArtifactRunRequestBody(null, null, null, null, jobId, null, null);
	}

	@Override
	void handleRequest(HandlerRequest<ArtifactRunRequestBody, ArtifactRunMessageParameters> request)
		throws Exception {
		handler.handleRequest(request, restfulGateway).get();
	}

	@Override
	JobGraph validateDefaultGraph() {
		JobGraph jobGraph = super.validateDefaultGraph();
		final SavepointRestoreSettings savepointRestoreSettings = jobGraph.getSavepointRestoreSettings();
		Assert.assertFalse(savepointRestoreSettings.allowNonRestoredState());
		Assert.assertNull(savepointRestoreSettings.getRestorePath());
		return jobGraph;
	}

	@Override
	JobGraph validateGraph() {
		JobGraph jobGraph = super.validateGraph();
		final SavepointRestoreSettings savepointRestoreSettings = jobGraph.getSavepointRestoreSettings();
		Assert.assertTrue(savepointRestoreSettings.allowNonRestoredState());
		Assert.assertEquals(RESTORE_PATH, savepointRestoreSettings.getRestorePath());
		return jobGraph;
	}
}
