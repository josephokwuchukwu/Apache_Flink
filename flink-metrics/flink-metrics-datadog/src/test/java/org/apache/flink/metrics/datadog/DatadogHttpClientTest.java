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

package org.apache.flink.metrics.datadog;

import org.apache.flink.metrics.util.TestCounter;
import org.apache.flink.metrics.util.TestMeter;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the DatadogHttpClient.
 */
public class DatadogHttpClientTest {

	private static final List<String> tags = Arrays.asList("tag1", "tag2");

	private static final long MOCKED_SYSTEM_MILLIS = 123L;

	@Test(expected = IllegalArgumentException.class)
	public void testClientWithEmptyKey() {
		new DatadogHttpClient("", null, 123, DataCenter.US, false);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testClientWithNullKey() {
		new DatadogHttpClient(null, null, 123, DataCenter.US, false);
	}

	@Test
	public void testGetProxyWithNullProxyHost() {
		DatadogHttpClient client = new DatadogHttpClient("anApiKey", null, 123, DataCenter.US, false);
		assert(client.getProxy() == Proxy.NO_PROXY);
	}

	@Test
	public void testGetProxy() {
		DatadogHttpClient client = new DatadogHttpClient("anApiKey", "localhost", 123, DataCenter.US, false);

		assertTrue(client.getProxy().address() instanceof InetSocketAddress);

		InetSocketAddress proxyAddress = (InetSocketAddress) client.getProxy().address();

		assertEquals(123, proxyAddress.getPort());
		assertEquals("localhost", proxyAddress.getHostString());
	}

	@Test
	public void serializeGauge() throws JsonProcessingException {
		DGauge g = new DGauge(() -> 1, "testCounter", "localhost", tags, () -> MOCKED_SYSTEM_MILLIS);

		assertEquals(
			"{\"metric\":\"testCounter\",\"type\":\"gauge\",\"host\":\"localhost\",\"tags\":[\"tag1\",\"tag2\"],\"points\":[[123,1]]}",
			DatadogHttpClient.serialize(g));
	}

	@Test
	public void serializeGaugeWithoutHost() throws JsonProcessingException {
		DGauge g = new DGauge(() -> 1, "testCounter", null, tags, () -> MOCKED_SYSTEM_MILLIS);

		assertEquals(
			"{\"metric\":\"testCounter\",\"type\":\"gauge\",\"tags\":[\"tag1\",\"tag2\"],\"points\":[[123,1]]}",
			DatadogHttpClient.serialize(g));
	}

	@Test
	public void serializeCounter() throws JsonProcessingException {
		DCounter c = new DCounter(new TestCounter(1), "testCounter", "localhost", tags, () -> MOCKED_SYSTEM_MILLIS);

		assertEquals(
			"{\"metric\":\"testCounter\",\"type\":\"count\",\"host\":\"localhost\",\"tags\":[\"tag1\",\"tag2\"],\"points\":[[123,1]]}",
			DatadogHttpClient.serialize(c));
	}

	@Test
	public void serializeCounterWithoutHost() throws JsonProcessingException {
		DCounter c = new DCounter(new TestCounter(1), "testCounter", null, tags, () -> MOCKED_SYSTEM_MILLIS);

		assertEquals(
			"{\"metric\":\"testCounter\",\"type\":\"count\",\"tags\":[\"tag1\",\"tag2\"],\"points\":[[123,1]]}",
			DatadogHttpClient.serialize(c));
	}

	@Test
	public void serializeMeter() throws JsonProcessingException {
		DMeter m = new DMeter(new TestMeter(0, 1), "testMeter", "localhost", tags, () -> MOCKED_SYSTEM_MILLIS);

		assertEquals(
			"{\"metric\":\"testMeter\",\"type\":\"gauge\",\"host\":\"localhost\",\"tags\":[\"tag1\",\"tag2\"],\"points\":[[123,1.0]]}",
			DatadogHttpClient.serialize(m));
	}

	@Test
	public void serializeMeterWithoutHost() throws JsonProcessingException {
		DMeter m = new DMeter(new TestMeter(0, 1), "testMeter", null, tags, () -> MOCKED_SYSTEM_MILLIS);

		assertEquals(
			"{\"metric\":\"testMeter\",\"type\":\"gauge\",\"tags\":[\"tag1\",\"tag2\"],\"points\":[[123,1.0]]}",
			DatadogHttpClient.serialize(m));
	}
}
