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

package org.apache.flink.table.client.cli;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertTrue;

/** Test {@link CliUtils}. */
public class CliUtilsTest {

    @Rule public TemporaryFolder realFolder = new TemporaryFolder();

    @Rule public TemporaryFolder linkFolder = new TemporaryFolder();

    @Test
    public void testCreateFileRealDir() throws IOException {
        Path realDirHistoryFile = Paths.get(realFolder.getRoot().toString(), "history.file");
        CliUtils.createFile(realDirHistoryFile);
        assertTrue(Files.exists(realDirHistoryFile));
    }

    @Test
    public void testCreateFileLinkDir() throws IOException {
        Path link = Paths.get(linkFolder.getRoot().getAbsolutePath(), "link");
        Files.createSymbolicLink(link, realFolder.getRoot().toPath());
        Path linkDirHistoryFile = Paths.get(link.toAbsolutePath().toString(), "history.file");
        Path realLinkDirHistoryFile = Paths.get(realFolder.getRoot().toString(), "history.file");
        CliUtils.createFile(linkDirHistoryFile);
        assertTrue(Files.exists(linkDirHistoryFile));
        assertTrue(Files.exists(realLinkDirHistoryFile));
    }

    @Test
    public void testCreateFileSubDir() throws IOException {
        Path subDirHistoryFile =
                Paths.get(realFolder.getRoot().toString(), "subdir", "history.file");
        CliUtils.createFile(subDirHistoryFile);
        assertTrue(Files.exists(subDirHistoryFile));
    }
}
