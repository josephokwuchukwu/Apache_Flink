/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A CheckpointStateOutputStream that wraps a primary and a secondary CheckpointStateOutputStream and duplicates
 * all writes into both streams. This stream applies buffering to reduce the amount of dual-method calling. Furthermore,
 * exceptions that happen in interactions with the secondary stream are not exposed, until the user calls
 * {@link #closeAndGetSecondaryHandle()}. In contrast to that, exceptions from interactions with the primary stream
 * are immediately returned to the user. This class is used to write state for local recovery as a local (secondary)
 * copy of the (primary) state snapshot that is written to a (slower but highly-available) remote filesystem.
 */
public class DuplicatingCheckpointOutputStream extends CheckpointStreamFactory.CheckpointStateOutputStream {

	private static final boolean STRICT_ALIGNMENT_CHECKS = false;
	private static final int DEFAULT_BUFFER_SIZER = 8 * 1024;

	private final byte[] buffer;
	private int count;

	/**
	 * Primary stream for writing the checkpoint data. Failures from this stream are forwarded.
	 */
	private final CheckpointStreamFactory.CheckpointStateOutputStream primaryOutputStream;

	/**
	 * Primary stream for writing the checkpoint data. Failures from this stream are not forwarded until
	 * {@link #closeAndGetSecondaryHandle()}.
	 */
	private final CheckpointStreamFactory.CheckpointStateOutputStream secondaryOutputStream;

	/**
	 * Stores a potential exception that occurred while interacting with {@link #secondaryOutputStream}
	 */
	private IOException secondaryStreamException;

	public DuplicatingCheckpointOutputStream(
		CheckpointStreamFactory.CheckpointStateOutputStream primaryOutputStream,
		CheckpointStreamFactory.CheckpointStateOutputStream secondaryOutputStream) throws IOException {
		this(primaryOutputStream, secondaryOutputStream, DEFAULT_BUFFER_SIZER);
	}

	public DuplicatingCheckpointOutputStream(
		CheckpointStreamFactory.CheckpointStateOutputStream primaryOutputStream,
		CheckpointStreamFactory.CheckpointStateOutputStream secondaryOutputStream,
		int bufferSize) throws IOException {

		this.primaryOutputStream = Preconditions.checkNotNull(primaryOutputStream);
		this.secondaryOutputStream = Preconditions.checkNotNull(secondaryOutputStream);

		this.buffer = new byte[bufferSize];
		this.count = 0;

		this.secondaryStreamException = null;

		checkForAlignedStreamPositions();
	}

	@Override
	public void write(int b) throws IOException {

		if (buffer.length <= count) {
			flushInternalBuffer();
		}

		buffer[count] = (byte) b;
		++count;
	}

	@Override
	public void write(byte[] b) throws IOException {

		write(b, 0, b.length);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {

		if (buffer.length <= len) {

			flushInternalBuffer();
			writeThroughInternal(b, off, len);
		} else {

			if (buffer.length < len + count) {
				flushInternalBuffer();
			}

			System.arraycopy(b, off, buffer, count, len);
			count += len;
		}
	}

	@Override
	public long getPos() throws IOException {
		final long referencePos = primaryOutputStream.getPos();
		return referencePos + count;
	}

	@Override
	public void flush() throws IOException {

		flushInternalBuffer();
		primaryOutputStream.flush();

		if (secondaryStreamException == null) {
			try {
				secondaryOutputStream.flush();
			} catch (IOException flushEx) {
				handleSecondaryStreamOnException(flushEx);
			}
		}
	}

	@Override
	public void sync() throws IOException {

		flushInternalBuffer();
		primaryOutputStream.sync();

		if (secondaryStreamException == null) {
			try {
				secondaryOutputStream.sync();
			} catch (IOException syncEx) {
				handleSecondaryStreamOnException(syncEx);
			}
		}
	}

	@Override
	public void close() throws IOException {

		IOException exCollector = null;

		try {
			flushInternalBuffer();
		} catch (IOException flushEx) {
			exCollector = flushEx;
		}

		try {
			primaryOutputStream.close();
		} catch (IOException closeEx) {
			exCollector = ExceptionUtils.firstOrSuppressed(closeEx, exCollector);
		}

		if (secondaryStreamException == null) {
			try {
				secondaryOutputStream.close();
			} catch (IOException closeEx) {
				handleSecondaryStreamOnException(closeEx);
			}
		}

		if (exCollector != null) {
			throw exCollector;
		}
	}

	private void checkForAlignedStreamPositions() throws IOException {

		final long primaryPos = primaryOutputStream.getPos();

		if (secondaryStreamException != null) {
			return;
		}

		try {
			final long secondaryPos = secondaryOutputStream.getPos();

			if (primaryPos != secondaryPos) {
				handleSecondaryStreamOnException(
					new IOException("Stream positions are out of sync between primary stream and secondary stream. " +
						"Reported positions are " + primaryPos + " (primary) and " + secondaryPos + " (secondary)."));
			}
		} catch (IOException posEx) {
			handleSecondaryStreamOnException(posEx);
		}
	}

	private void flushInternalBuffer() throws IOException {

		if (count > 0) {
			writeThroughInternal(buffer, 0, count);
			count = 0;
		}
	}

	private void writeThroughInternal(byte[] b, int off, int len) throws IOException {

		if (STRICT_ALIGNMENT_CHECKS) {
			checkForAlignedStreamPositions();
		}

		primaryOutputStream.write(b, off, len);

		if (secondaryStreamException == null) {
			try {
				secondaryOutputStream.write(b, off, len);
			} catch (IOException writeEx) {
				handleSecondaryStreamOnException(writeEx);
			}
		}
	}

	private void handleSecondaryStreamOnException(IOException ex) {

		Preconditions.checkState(secondaryStreamException == null,
			"Secondary stream already failed from previous exception!");

		try {
			secondaryOutputStream.close();
		} catch (IOException closeEx) {
			ex = ExceptionUtils.firstOrSuppressed(closeEx, ex);
		}

		secondaryStreamException = Preconditions.checkNotNull(ex);
	}

	@Override
	public StreamStateHandle closeAndGetHandle() throws IOException {
		return closeAndGetPrimaryHandle();
	}

	/**
	 * Returns the state handle from the {@link #primaryOutputStream}.
	 */
	public StreamStateHandle closeAndGetPrimaryHandle() throws IOException {
		flushInternalBuffer();
		return primaryOutputStream.closeAndGetHandle();
	}

	/**
	 * Returns the state handle from the {@link #secondaryOutputStream}. Also reports suppressed exceptions from earlier
	 * interactions with that stream.
	 */
	public StreamStateHandle closeAndGetSecondaryHandle() throws IOException {
		if (secondaryStreamException == null) {
			flushInternalBuffer();
			return secondaryOutputStream.closeAndGetHandle();
		} else {
			throw new IOException("Secondary stream previously failed exceptionally", secondaryStreamException);
		}
	}

	public IOException getSecondaryStreamException() {
		return secondaryStreamException;
	}

	@VisibleForTesting
	CheckpointStreamFactory.CheckpointStateOutputStream getPrimaryOutputStream() {
		return primaryOutputStream;
	}

	@VisibleForTesting
	CheckpointStreamFactory.CheckpointStateOutputStream getSecondaryOutputStream() {
		return secondaryOutputStream;
	}
}
