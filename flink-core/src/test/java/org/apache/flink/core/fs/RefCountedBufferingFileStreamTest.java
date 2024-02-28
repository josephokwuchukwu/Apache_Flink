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

package org.apache.flink.core.fs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for the {@link RefCountedBufferingFileStream}. */
public class RefCountedBufferingFileStreamTest {

    private static final int BUFFER_SIZE = 10;

    @TempDir
    public File temporaryFolder;

    @Test
    void testSmallWritesGoToBuffer() throws IOException {
        RefCountedBufferingFileStream stream = getStreamToTest();

        final byte[] contentToWrite = bytesOf("hello");
        stream.write(contentToWrite);

        assertThat(stream.getPositionInBuffer()).isEqualTo(contentToWrite.length);
        assertThat(stream.getPos()).isEqualTo(contentToWrite.length);

        stream.close();
        stream.release();
    }

    @Test
    void testExceptionWhenWritingToClosedFile() throws IOException {
        assertThatExceptionOfType(IOException.class).isThrownBy(() -> {
            RefCountedBufferingFileStream stream = getStreamToTest();

            final byte[] contentToWrite = bytesOf("hello");
            stream.write(contentToWrite);

            assertEquals(contentToWrite.length, stream.getPositionInBuffer());
            assertEquals(contentToWrite.length, stream.getPos());

            stream.close();

            stream.write(contentToWrite);
        });
    }

    @Test
    void testBigWritesGoToFile() throws IOException {
        RefCountedBufferingFileStream stream = getStreamToTest();

        final byte[] contentToWrite = bytesOf("hello big world");
        stream.write(contentToWrite);

        assertThat(stream.getPositionInBuffer()).isZero();
        assertThat(stream.getPos()).isEqualTo(contentToWrite.length);

        stream.close();
        stream.release();
    }

    @Test
    void testSpillingWhenBufferGetsFull() throws IOException {
        RefCountedBufferingFileStream stream = getStreamToTest();

        final byte[] firstContentToWrite = bytesOf("hello");
        stream.write(firstContentToWrite);

        assertThat(stream.getPositionInBuffer()).isEqualTo(firstContentToWrite.length);
        assertThat(stream.getPos()).isEqualTo(firstContentToWrite.length);

        final byte[] secondContentToWrite = bytesOf(" world!");
        stream.write(secondContentToWrite);

        assertThat(stream.getPositionInBuffer()).isEqualTo(secondContentToWrite.length);
        assertThat(stream.getPos()).isEqualTo(firstContentToWrite.length + secondContentToWrite.length);

        stream.close();
        stream.release();
    }

    @Test
    void testFlush() throws IOException {
        RefCountedBufferingFileStream stream = getStreamToTest();

        final byte[] contentToWrite = bytesOf("hello");
        stream.write(contentToWrite);

        assertThat(stream.getPositionInBuffer()).isEqualTo(contentToWrite.length);
        assertThat(stream.getPos()).isEqualTo(contentToWrite.length);

        stream.flush();

        assertThat(stream.getPositionInBuffer()).isZero();
        assertThat(stream.getPos()).isEqualTo(contentToWrite.length);

        final byte[] contentRead = new byte[contentToWrite.length];
        new FileInputStream(stream.getInputFile()).read(contentRead, 0, contentRead.length);
        assertThat(Arrays.equals(contentToWrite, contentRead)).isTrue();

        stream.release();
    }

    // ---------------------------- Utility Classes ----------------------------

    private RefCountedBufferingFileStream getStreamToTest() throws IOException {
        return new RefCountedBufferingFileStream(getRefCountedFileWithContent(), BUFFER_SIZE);
    }

    private RefCountedFileWithStream getRefCountedFileWithContent() throws IOException {
        final File newFile = new File(temporaryFolder, ".tmp_" + UUID.randomUUID());
        final OutputStream out =
                Files.newOutputStream(newFile.toPath(), StandardOpenOption.CREATE_NEW);

        return RefCountedFileWithStream.newFile(newFile, out);
    }

    private static byte[] bytesOf(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }
}
