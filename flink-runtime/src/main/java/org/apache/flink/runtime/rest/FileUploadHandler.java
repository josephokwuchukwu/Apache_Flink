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

package org.apache.flink.runtime.rest;

import org.apache.flink.annotation.VisibleForTesting;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPipeline;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpContent;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObject;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.LastHttpContent;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.DiskFileUpload;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.HttpDataFactory;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.InterfaceHttpData;
import org.apache.flink.shaded.netty4.io.netty.util.AttributeKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Writes multipart/form-data to disk. Delegates all other requests to the next
 * {@link ChannelInboundHandler} in the {@link ChannelPipeline}.
 */
public class FileUploadHandler extends SimpleChannelInboundHandler<HttpObject> {

	private static final Logger LOG = LoggerFactory.getLogger(FileUploadHandler.class);

	static final AttributeKey<Path> UPLOADED_FILE = AttributeKey.valueOf("UPLOADED_FILE");

	private static final HttpDataFactory DATA_FACTORY = new DefaultHttpDataFactory(true);

	private final Path uploadDir;

	private HttpPostRequestDecoder currentHttpPostRequestDecoder;

	private HttpRequest currentHttpRequest;

	public FileUploadHandler(final Path uploadDir) {
		super(false);
		DiskFileUpload.baseDirectory = uploadDir.normalize().toAbsolutePath().toString();
		this.uploadDir = requireNonNull(uploadDir);
	}

	@Override
	protected void channelRead0(final ChannelHandlerContext ctx, final HttpObject msg) throws Exception {
		if (msg instanceof HttpRequest) {
			final HttpRequest httpRequest = (HttpRequest) msg;
			if (httpRequest.getMethod().equals(HttpMethod.POST)) {
				if (HttpPostRequestDecoder.isMultipart(httpRequest)) {
					currentHttpPostRequestDecoder = new HttpPostRequestDecoder(DATA_FACTORY, httpRequest);
					currentHttpRequest = httpRequest;
				} else {
					ctx.fireChannelRead(msg);
				}
			} else {
				ctx.fireChannelRead(msg);
			}
		} else if (msg instanceof HttpContent && currentHttpPostRequestDecoder != null) {
			final HttpContent httpContent = (HttpContent) msg;
			currentHttpPostRequestDecoder.offer(httpContent);

			while (currentHttpPostRequestDecoder.hasNext()) {
				final InterfaceHttpData data = currentHttpPostRequestDecoder.next();
				if (data.getHttpDataType() == InterfaceHttpData.HttpDataType.FileUpload) {
					final DiskFileUpload fileUpload = (DiskFileUpload) data;
					checkState(fileUpload.isCompleted());

					createUploadDir(uploadDir);

					final Path dest = uploadDir.resolve(Paths.get(UUID.randomUUID().toString() +
						"_" + fileUpload.getFilename()));
					fileUpload.renameTo(dest.toFile());
					ctx.channel().attr(UPLOADED_FILE).set(dest);
				}
				data.release();
			}

			if (httpContent instanceof LastHttpContent) {
				ctx.fireChannelRead(currentHttpRequest);
				ctx.fireChannelRead(httpContent);
				reset();
			}
		} else {
			ctx.fireChannelRead(msg);
		}
	}

	private void reset() {
		currentHttpPostRequestDecoder.destroy();
		currentHttpPostRequestDecoder = null;
		currentHttpRequest = null;
	}

	/**
	 * Creates the upload dir if needed.
	 */
	@VisibleForTesting
	static void createUploadDir(final Path uploadDir) throws IOException {
		if (!Files.exists(uploadDir)) {
			LOG.warn("Upload directory {} does not exist, or has been deleted externally. " +
				"Previously uploaded files are no longer available.", uploadDir);
			checkAndCreateUploadDir(uploadDir);
		}
	}

	/**
	 * Checks whether the given directory exists and is writable. If it doesn't exist, this method
	 * will attempt to create it.
	 *
	 * @param uploadDir directory to check
	 * @throws IOException if the directory does not exist and cannot be created, or if the
	 *                     directory isn't writable
	 */
	private static synchronized void checkAndCreateUploadDir(final Path uploadDir) throws IOException {
		if (Files.exists(uploadDir) && Files.isWritable(uploadDir)) {
			LOG.info("Using directory {} for file uploads.", uploadDir);
		} else if (Files.isWritable(Files.createDirectories(uploadDir))) {
			LOG.info("Created directory {} for file uploads.", uploadDir);
		} else {
			LOG.warn("Upload directory {} cannot be created or is not writable.", uploadDir);
			throw new IOException(
				String.format("Upload directory %s cannot be created or is not writable.",
					uploadDir));
		}
	}
}
