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

package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.elasticsearch.util.NoOpFailureHandler;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Suite of tests for {@link ElasticsearchSinkBase}.
 */
public class ElasticsearchSinkBaseTest {

	/** Tests that any item failure in the listener callbacks is rethrown on an immediately following invoke call. */
	@Test
	public void testItemFailureRethrownOnInvoke() throws Throwable {
		final DummyElasticsearchSink<String> sink = new DummyElasticsearchSink<>(
			new HashMap<String, String>(), new SimpleSinkFunction<String>(), new NoOpFailureHandler());

		final OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

		testHarness.open();

		// setup the next bulk request, and its mock item failures
		sink.setMockItemFailuresListForNextBulkItemResponses(Collections.singletonList(new Exception("artificial failure for record")));
		testHarness.processElement(new StreamRecord<>("msg"));
		verify(sink.getMockBulkProcessor(), times(1)).add(any(ActionRequest.class));

		// manually execute the next bulk request
		sink.manualBulkRequestWithAllPendingRequests();

		try {
			testHarness.processElement(new StreamRecord<>("next msg"));
		} catch (Exception e) {
			// the invoke should have failed with the failure
			Assert.assertTrue(e.getCause().getMessage().contains("artificial failure for record"));

			// test succeeded
			return;
		}

		Assert.fail();
	}

	/** Tests that any item failure in the listener callbacks is rethrown on an immediately following checkpoint. */
	@Test
	public void testItemFailureRethrownOnCheckpoint() throws Throwable {
		final DummyElasticsearchSink<String> sink = new DummyElasticsearchSink<>(
			new HashMap<String, String>(), new SimpleSinkFunction<String>(), new NoOpFailureHandler());

		final OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

		testHarness.open();

		// setup the next bulk request, and its mock item failures
		sink.setMockItemFailuresListForNextBulkItemResponses(Collections.singletonList(new Exception("artificial failure for record")));
		testHarness.processElement(new StreamRecord<>("msg"));
		verify(sink.getMockBulkProcessor(), times(1)).add(any(ActionRequest.class));

		// manually execute the next bulk request
		sink.manualBulkRequestWithAllPendingRequests();

		try {
			testHarness.snapshot(1L, 1000L);
		} catch (Exception e) {
			// the snapshot should have failed with the failure
			Assert.assertTrue(e.getCause().getCause().getMessage().contains("artificial failure for record"));

			// test succeeded
			return;
		}

		Assert.fail();
	}

	/**
	 * Tests that any item failure in the listener callbacks due to flushing on an immediately following checkpoint
	 * is rethrown; we set a timeout because the test will not finish if the logic is broken
	 */
	@Test(timeout=5000)
	public void testItemFailureRethrownOnCheckpointAfterFlush() throws Throwable {
		final DummyElasticsearchSink<String> sink = new DummyElasticsearchSink<>(
			new HashMap<String, String>(), new SimpleSinkFunction<String>(), new NoOpFailureHandler());

		final OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

		testHarness.open();

		// setup the next bulk request, and its mock item failures

		List<Exception> mockResponsesList = new ArrayList<>(2);
		mockResponsesList.add(null); // the first request in a bulk will succeed
		mockResponsesList.add(new Exception("artificial failure for record")); // the second request in a bulk will fail
		sink.setMockItemFailuresListForNextBulkItemResponses(mockResponsesList);

		testHarness.processElement(new StreamRecord<>("msg-1"));
		verify(sink.getMockBulkProcessor(), times(1)).add(any(ActionRequest.class));

		// manually execute the next bulk request (1 request only, thus should succeed)
		sink.manualBulkRequestWithAllPendingRequests();

		CheckedThread snapshotThread = new CheckedThread() {
			@Override
			public void go() throws Exception {
				testHarness.snapshot(1L, 1000L);
			}
		};
		snapshotThread.start();

		// the snapshot should eventually be blocked before snapshot triggers flushing
		while (snapshotThread.getState() != Thread.State.WAITING) {
			Thread.sleep(10);
		}

		// setup the requests to be flushed
		testHarness.processElement(new StreamRecord<>("msg-2"));
		testHarness.processElement(new StreamRecord<>("msg-3"));
		verify(sink.getMockBulkProcessor(), times(3)).add(any(ActionRequest.class));

		// let the snapshot-triggered flush continue (2 records in the bulk, so the 2nd one should fail)
		sink.continueFlush();

		try {
			snapshotThread.sync();
		} catch (Exception e) {
			// the snapshot should have failed with the failure from the 2nd request
			Assert.assertTrue(e.getCause().getCause().getMessage().contains("artificial failure for record"));

			// test succeeded
			return;
		}

		Assert.fail();
	}

	/** Tests that any bulk failure in the listener callbacks is rethrown on an immediately following invoke call. */
	@Test
	public void testBulkFailureRethrownOnInvoke() throws Throwable {
		final DummyElasticsearchSink<String> sink = new DummyElasticsearchSink<>(
			new HashMap<String, String>(), new SimpleSinkFunction<String>(), new NoOpFailureHandler());

		final OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

		testHarness.open();

		// setup the next bulk request, and let the whole bulk request fail
		sink.setFailNextBulkRequestCompletely(new Exception("artificial failure for bulk request"));
		testHarness.processElement(new StreamRecord<>("msg"));
		verify(sink.getMockBulkProcessor(), times(1)).add(any(ActionRequest.class));

		// manually execute the next bulk request
		sink.manualBulkRequestWithAllPendingRequests();

		try {
			testHarness.processElement(new StreamRecord<>("next msg"));
		} catch (Exception e) {
			// the invoke should have failed with the bulk request failure
			Assert.assertTrue(e.getCause().getMessage().contains("artificial failure for bulk request"));

			// test succeeded
			return;
		}

		Assert.fail();
	}

	/** Tests that any bulk failure in the listener callbacks is rethrown on an immediately following checkpoint. */
	@Test
	public void testBulkFailureRethrownOnCheckpoint() throws Throwable {
		final DummyElasticsearchSink<String> sink = new DummyElasticsearchSink<>(
			new HashMap<String, String>(), new SimpleSinkFunction<String>(), new NoOpFailureHandler());

		final OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

		testHarness.open();

		// setup the next bulk request, and let the whole bulk request fail
		sink.setFailNextBulkRequestCompletely(new Exception("artificial failure for bulk request"));
		testHarness.processElement(new StreamRecord<>("msg"));
		verify(sink.getMockBulkProcessor(), times(1)).add(any(ActionRequest.class));

		// manually execute the next bulk request
		sink.manualBulkRequestWithAllPendingRequests();

		try {
			testHarness.snapshot(1L, 1000L);
		} catch (Exception e) {
			// the snapshot should have failed with the bulk request failure
			Assert.assertTrue(e.getCause().getCause().getMessage().contains("artificial failure for bulk request"));

			// test succeeded
			return;
		}

		Assert.fail();
	}

	/**
	 * Tests that any bulk failure in the listener callbacks due to flushing on an immediately following checkpoint
	 * is rethrown; we set a timeout because the test will not finish if the logic is broken.
	 */
	@Test(timeout=5000)
	public void testBulkFailureRethrownOnOnCheckpointAfterFlush() throws Throwable {
		final DummyElasticsearchSink<String> sink = new DummyElasticsearchSink<>(
			new HashMap<String, String>(), new SimpleSinkFunction<String>(), new NoOpFailureHandler());

		final OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

		testHarness.open();

		// setup the next bulk request, and let bulk request succeed
		sink.setMockItemFailuresListForNextBulkItemResponses(Collections.singletonList((Exception) null));
		testHarness.processElement(new StreamRecord<>("msg-1"));
		verify(sink.getMockBulkProcessor(), times(1)).add(any(ActionRequest.class));

		// manually execute the next bulk request
		sink.manualBulkRequestWithAllPendingRequests();

		CheckedThread snapshotThread = new CheckedThread() {
			@Override
			public void go() throws Exception {
				testHarness.snapshot(1L, 1000L);
			}
		};
		snapshotThread.start();

		// the snapshot should eventually be blocked before snapshot triggers flushing
		while (snapshotThread.getState() != Thread.State.WAITING) {
			Thread.sleep(10);
		}

		// for the snapshot-triggered flush, we let the bulk request fail completely
		sink.setFailNextBulkRequestCompletely(new Exception("artificial failure for bulk request"));

		// setup the requests to be flushed
		testHarness.processElement(new StreamRecord<>("msg-2"));
		testHarness.processElement(new StreamRecord<>("msg-3"));
		verify(sink.getMockBulkProcessor(), times(3)).add(any(ActionRequest.class));

		// let the snapshot-triggered flush continue (bulk request should fail completely)
		sink.continueFlush();

		try {
			snapshotThread.sync();
		} catch (Exception e) {
			// the snapshot should have failed with the bulk request failure
			Assert.assertTrue(e.getCause().getCause().getMessage().contains("artificial failure for bulk request"));

			// test succeeded
			return;
		}

		Assert.fail();
	}

	/**
	 * Tests that the sink correctly waits for pending requests (including re-added requests) on checkpoints;
	 * we set a timeout because the test will not finish if the logic is broken
	 */
	@Test(timeout=5000)
	public void testAtLeastOnceSink() throws Throwable {
		final DummyElasticsearchSink<String> sink = new DummyElasticsearchSink<>(
				new HashMap<String, String>(),
				new SimpleSinkFunction<String>(),
				new DummyRetryFailureHandler()); // use a failure handler that simply re-adds requests

		final OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

		testHarness.open();

		// setup the next bulk request, and its mock item failures;
		// it contains 1 request, which will fail and re-added to the next bulk request
		sink.setMockItemFailuresListForNextBulkItemResponses(Collections.singletonList(new Exception("artificial failure for record")));
		testHarness.processElement(new StreamRecord<>("msg"));
		verify(sink.getMockBulkProcessor(), times(1)).add(any(ActionRequest.class));

		CheckedThread snapshotThread = new CheckedThread() {
			@Override
			public void go() throws Exception {
				testHarness.snapshot(1L, 1000L);
			}
		};
		snapshotThread.start();

		// the snapshot should eventually be blocked before snapshot triggers flushing
		while (snapshotThread.getState() != Thread.State.WAITING) {
			Thread.sleep(10);
		}

		sink.continueFlush();

		// since the previous flush should have resulted in a request re-add from the failure handler,
		// we should have flushed again, and eventually be blocked before snapshot triggers the 2nd flush
		while (snapshotThread.getState() != Thread.State.WAITING) {
			Thread.sleep(10);
		}

		// current number of pending request should be 1 due to the re-add
		Assert.assertEquals(1, sink.getNumPendingRequests());

		// this time, let the bulk request succeed, so no-more requests are re-added
		sink.setMockItemFailuresListForNextBulkItemResponses(Collections.singletonList((Exception) null));

		sink.continueFlush();

		// the snapshot should finish with no exceptions
		snapshotThread.sync();

		testHarness.close();
	}

	/**
	 * This test is meant to assure that testAtLeastOnceSink is valid by testing that if flushing is disabled,
	 * the snapshot method does indeed finishes without waiting for pending requests;
	 * we set a timeout because the test will not finish if the logic is broken
	 */
	@Test(timeout=5000)
	public void testDoesNotWaitForPendingRequestsIfFlushingDisabled() throws Exception {
		final DummyElasticsearchSink<String> sink = new DummyElasticsearchSink<>(
			new HashMap<String, String>(), new SimpleSinkFunction<String>(), new DummyRetryFailureHandler());
		sink.disableFlushOnCheckpoint(); // disable flushing

		final OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

		testHarness.open();

		// setup the next bulk request, and let bulk request succeed
		sink.setMockItemFailuresListForNextBulkItemResponses(Collections.singletonList(new Exception("artificial failure for record")));
		testHarness.processElement(new StreamRecord<>("msg-1"));
		verify(sink.getMockBulkProcessor(), times(1)).add(any(ActionRequest.class));

		// the snapshot should not block even though we haven't flushed the bulk request
		testHarness.snapshot(1L, 1000L);

		testHarness.close();
	}

	private static class DummyElasticsearchSink<T> extends ElasticsearchSinkBase<T> {

		private static final long serialVersionUID = 5051907841570096991L;

		private transient BulkProcessor mockBulkProcessor;
		private transient BulkRequest nextBulkRequest = new BulkRequest();
		private transient MultiShotLatch flushLatch = new MultiShotLatch();

		private List<? extends Throwable> mockItemFailuresList;
		private Throwable nextBulkFailure;

		public DummyElasticsearchSink(
				Map<String, String> userConfig,
				ElasticsearchSinkFunction<T> sinkFunction,
				ActionRequestFailureHandler failureHandler) {
			super(new DummyElasticsearchApiCallBridge(), userConfig, sinkFunction, failureHandler);
		}

		/**
		 * This method is used to mimic a scheduled bulk request; we need to do this
		 * manually because we are mocking the BulkProcessor
		 */
		public void manualBulkRequestWithAllPendingRequests() {
			flushLatch.trigger(); // let the flush
			mockBulkProcessor.flush();
		}

		/**
		 * On non-manual flushes, i.e. when flush is called in the snapshot method implementation,
		 * usages need to explicitly call this to allow the flush to continue. This is useful
		 * to make sure that specific requests get added to the the next bulk request for flushing.
		 */
		public void continueFlush() {
			flushLatch.trigger();
		}

		/**
		 * Set the list of mock failures to use for the next bulk of item responses. A {@code null}
		 * means that the response is successful, failed otherwise.
		 *
		 * The list is used with corresponding order to the requests in the bulk, i.e. the first
		 * request uses the response at index 0, the second requests uses the response at index 1, etc.
		 */
		public void setMockItemFailuresListForNextBulkItemResponses(List<? extends Throwable> mockItemFailuresList) {
			this.mockItemFailuresList = mockItemFailuresList;
		}

		/**
		 * Let the next bulk request fail completely with the provided throwable.
		 * If this is set, the failures list provided with setMockItemFailuresListForNextBulkItemResponses is not respected.
		 */
		public void setFailNextBulkRequestCompletely(Throwable failure) {
			this.nextBulkFailure = failure;
		}

		public BulkProcessor getMockBulkProcessor() {
			return mockBulkProcessor;
		}

		/**
		 * Override the bulk processor build process to provide a mock implementation,
		 * but reuse the listener implementation in our mock to test that the listener logic
		 * works correctly with request flushing logic.
		 */
		@Override
		protected BulkProcessor buildBulkProcessor(final BulkProcessor.Listener listener) {
			this.mockBulkProcessor = mock(BulkProcessor.class);

			when(mockBulkProcessor.add(any(ActionRequest.class))).thenAnswer(new Answer<Object>() {
				@Override
				public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
					// intercept the request and add it to our mock bulk request
					nextBulkRequest.add(invocationOnMock.getArgumentAt(0, ActionRequest.class));

					return null;
				}
			});

			doAnswer(new Answer() {
				@Override
				public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
					// wait until we are allowed to continue with the flushing
					flushLatch.await();

					// create a copy of the accumulated mock requests, so that
					// re-added requests from the failure handler are included in the next bulk
					BulkRequest currentBulkRequest = nextBulkRequest;
					nextBulkRequest = new BulkRequest();

					listener.beforeBulk(123L, currentBulkRequest);

					if (nextBulkFailure == null) {
						BulkItemResponse[] mockResponses = new BulkItemResponse[currentBulkRequest.requests().size()];
						for (int i = 0; i < currentBulkRequest.requests().size(); i++) {
							Throwable mockItemFailure = mockItemFailuresList.get(i);

							if (mockItemFailure == null) {
								// the mock response for the item is success
								mockResponses[i] = new BulkItemResponse(i, "opType", mock(ActionResponse.class));
							} else {
								// the mock response for the item is failure
								mockResponses[i] = new BulkItemResponse(i, "opType", new BulkItemResponse.Failure("index", "type", "id", mockItemFailure));
							}
						}

						listener.afterBulk(123L, currentBulkRequest, new BulkResponse(mockResponses, 1000L));
					} else {
						listener.afterBulk(123L, currentBulkRequest, nextBulkFailure);
					}

					return null;
				}
			}).when(mockBulkProcessor).flush();

			return mockBulkProcessor;
		}
	}

	private static class DummyElasticsearchApiCallBridge implements ElasticsearchApiCallBridge {

		private static final long serialVersionUID = -4272760730959041699L;

		@Override
		public Client createClient(Map<String, String> clientConfig) {
			return mock(Client.class);
		}

		@Nullable
		@Override
		public Throwable extractFailureCauseFromBulkItemResponse(BulkItemResponse bulkItemResponse) {
			if (bulkItemResponse.isFailed()) {
				return new Exception(bulkItemResponse.getFailure().getMessage());
			} else {
				return null;
			}
		}

		@Override
		public void configureBulkProcessorBackoff(BulkProcessor.Builder builder, @Nullable ElasticsearchSinkBase.BulkFlushBackoffPolicy flushBackoffPolicy) {
			// no need for this in the test cases here
		}

		@Override
		public void cleanup() {
			// nothing to cleanup
		}
	}

	private static class SimpleSinkFunction<String> implements ElasticsearchSinkFunction<String> {

		private static final long serialVersionUID = -176739293659135148L;

		@Override
		public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
			Map<java.lang.String, Object> json = new HashMap<>();
			json.put("data", element);

			indexer.add(
				Requests.indexRequest()
					.index("index")
					.type("type")
					.id("id")
					.source(json)
			);
		}
	}

	private static class DummyRetryFailureHandler implements ActionRequestFailureHandler {

		private static final long serialVersionUID = 5400023700099200745L;

		@Override
		public void onFailure(ActionRequest action, Throwable failure, RequestIndexer indexer) throws Throwable {
			indexer.add(action);
		}
	}
}
