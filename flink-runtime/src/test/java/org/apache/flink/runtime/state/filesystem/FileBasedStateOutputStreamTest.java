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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;

/**
 * Tests for the {@link FileBasedStateOutputStream}.
 */
public class FileBasedStateOutputStreamTest extends AbstractCheckpointStateOutputStreamTestBase {

	@Override
	protected FSDataOutputStream createTestStream(FileSystem fs, Path dir, String fileName) throws IOException {
		return new FileBasedStateOutputStream(fs, new Path(dir, fileName));
	}

	@Override
	protected FileStateHandle closeAndGetResult(FSDataOutputStream stream) throws IOException {
		return ((FileBasedStateOutputStream) stream).closeAndGetHandle();
	}

	@Override
	Path getFlyingPath(Path path) {
		return path;
	}

	@Override
	Path getTargetPath(Path path) {
		return path;
	}
}
