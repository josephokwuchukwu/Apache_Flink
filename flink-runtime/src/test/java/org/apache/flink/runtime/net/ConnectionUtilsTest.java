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

package org.apache.flink.runtime.net;

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.net.Inet4Address;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the network utilities.
 */
@RunWith(PowerMockRunner.class)
public class ConnectionUtilsTest {

	@Test
	public void testReturnLocalHostAddressUsingHeuristics() throws Exception {
		try (ServerSocket blocker = new ServerSocket(0, 1, InetAddress.getLocalHost())) {
			// the "blocker" server socket simply does not accept connections
			// this address is consequently "unreachable"
			InetSocketAddress unreachable = new InetSocketAddress("localhost", blocker.getLocalPort());

			final long start = System.nanoTime();
			InetAddress add = ConnectionUtils.findConnectingAddress(unreachable, 2000, 400);

			// we should have found a heuristic address
			assertNotNull(add);

			// make sure that the InetAddress.getLocalHost is not a loopback address
			Assume.assumeFalse(InetAddress.getLocalHost().isLoopbackAddress());

			// make sure the connection address is not a loopback address
			Assume.assumeFalse(add.isLoopbackAddress());

			Socket socket = new Socket();

			SocketAddress socketAddress = new InetSocketAddress(add, 0);

			socket.bind(socketAddress);

			// check whether can bind to this address
			assertTrue(socket.isBound());

			// check that it did not take forever (max 30 seconds)
			// this check can unfortunately not be too tight, or it will be flaky on some CI infrastructure
			assertTrue(System.nanoTime() - start < 30_000_000_000L);

			// make sure that we returned the InetAddress.getLocalHost as a heuristic
			assertEquals(InetAddress.getLocalHost(), add);
		}
	}

	@Test
	public void testFindConnectingAddressWhenGetLocalHostThrows() throws Exception {
		PowerMockito.mockStatic(InetAddress.class);
		Mockito.when(InetAddress.getLocalHost()).thenThrow(new UnknownHostException()).thenCallRealMethod();

		final InetAddress loopbackAddress = Inet4Address.getByName("127.0.0.1");
		Thread socketServerThread;
		try (ServerSocket socket = new ServerSocket(0, 1, loopbackAddress)) {
			// Make sure that the thread will eventually die even if something else goes wrong
			socket.setSoTimeout(10_000);
			socketServerThread = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						socket.accept();
					} catch (IOException e) {
						// ignore
					}
				}
			});
			socketServerThread.start();

			final InetSocketAddress socketAddress = new InetSocketAddress(loopbackAddress, socket.getLocalPort());
			final InetAddress address = ConnectionUtils.findConnectingAddress(
				socketAddress, 2000, 400);

			// Make sure we got an address via alternative means
			assertNotNull(address);
		}
	}
}
