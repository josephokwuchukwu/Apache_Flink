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

package org.apache.flink.runtime.client;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.TestLogger;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ClientUtils}.
 */
public class ClientUtilsTest extends TestLogger {

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static BlobServer blobServer;

	@BeforeClass
	public static void setup() throws IOException {
		Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());
		blobServer = new BlobServer(config, new VoidBlobStore());
		blobServer.start();
	}

	@Test
	public void uploadAndSetUserJars() throws IOException {
		java.nio.file.Path tmpDir = temporaryFolder.newFolder().toPath();
		JobGraph jobGraph = new JobGraph();

		Collection<Path> jars = Arrays.asList(
			new Path(Files.createFile(tmpDir.resolve("jar1.jar")).toString()),
			new Path(Files.createFile(tmpDir.resolve("jar2.jar")).toString()));

		jars.forEach(jobGraph::addJar);

		assertEquals(jars.size(), jobGraph.getUserJars().size());
		assertEquals(0, jobGraph.getUserJarBlobKeys().size());

		try (BlobClient blobClient = new BlobClient(new InetSocketAddress("localhost", blobServer.getPort()), new Configuration())) {
			ClientUtils.uploadAndSetUserJars(jobGraph, blobClient);
		}

		assertEquals(jars.size(), jobGraph.getUserJars().size());
		assertEquals(jars.size(), jobGraph.getUserJarBlobKeys().size());
		assertEquals(jars.size(), jobGraph.getUserJarBlobKeys().stream().distinct().count());
	}

	@Test
	public void uploadAndSetUserArtifacts() throws IOException {
		java.nio.file.Path tmpDir = temporaryFolder.newFolder().toPath();
		JobGraph jobGraph = new JobGraph();

		Collection<DistributedCache.DistributedCacheEntry> localArtifacts = Arrays.asList(
			new DistributedCache.DistributedCacheEntry(Files.createFile(tmpDir.resolve("art1")).toString(), true, true),
			new DistributedCache.DistributedCacheEntry(Files.createFile(tmpDir.resolve("art2")).toString(), true, false),
			new DistributedCache.DistributedCacheEntry(Files.createFile(tmpDir.resolve("art3")).toString(), false, true),
			new DistributedCache.DistributedCacheEntry(Files.createFile(tmpDir.resolve("art4")).toString(), true, false)
		);

		Collection<DistributedCache.DistributedCacheEntry> distributedArtifacts = Arrays.asList(
			new DistributedCache.DistributedCacheEntry("hdfs://localhost:1234/test", true, false)
		);

		for (DistributedCache.DistributedCacheEntry entry : localArtifacts) {
			jobGraph.addUserArtifact(entry.filePath, entry);
		}
		for (DistributedCache.DistributedCacheEntry entry : distributedArtifacts) {
			jobGraph.addUserArtifact(entry.filePath, entry);
		}

		final int totalNumArtifacts = localArtifacts.size() + distributedArtifacts.size();

		assertEquals(totalNumArtifacts, jobGraph.getUserArtifacts().size());
		assertEquals(0, jobGraph.getUserArtifacts().values().stream().filter(entry -> entry.blobKey != null).count());

		try (BlobClient blobClient = new BlobClient(new InetSocketAddress("localhost", blobServer.getPort()), new Configuration())) {
			ClientUtils.uploadAndSetUserArtifacts(jobGraph, blobClient);
		}

		assertEquals(totalNumArtifacts, jobGraph.getUserArtifacts().size());
		assertEquals(localArtifacts.size(), jobGraph.getUserArtifacts().values().stream().filter(entry -> entry.blobKey != null).count());
		assertEquals(distributedArtifacts.size(), jobGraph.getUserArtifacts().values().stream().filter(entry -> entry.blobKey == null).count());
		// 1 unique key for each local artifact, and null for distributed artifacts
		assertEquals(localArtifacts.size() + 1, jobGraph.getUserArtifacts().values().stream().map(entry -> entry.blobKey).distinct().count());
		for (DistributedCache.DistributedCacheEntry original : localArtifacts) {
			assertState(original, jobGraph.getUserArtifacts().get(original.filePath), false);
		}
		for (DistributedCache.DistributedCacheEntry original : distributedArtifacts) {
			assertState(original, jobGraph.getUserArtifacts().get(original.filePath), true);
		}
	}

	private static void assertState(DistributedCache.DistributedCacheEntry original, DistributedCache.DistributedCacheEntry actual, boolean isBlobKeyNull) {
		assertEquals(original.isZipped, actual.isZipped);
		assertEquals(original.isExecutable, actual.isExecutable);
		assertEquals(original.filePath, actual.filePath);
		assertEquals(isBlobKeyNull, actual.blobKey == null);
	}

}
