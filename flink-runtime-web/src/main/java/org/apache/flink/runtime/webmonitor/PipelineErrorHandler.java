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

package org.apache.flink.runtime.webmonitor;

import com.fasterxml.jackson.core.JsonGenerator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.flink.runtime.webmonitor.handlers.JsonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;

/**
 * This is the last handler in the pipeline and logs all error messages.
 */
@ChannelHandler.Sharable
public class PipelineErrorHandler extends SimpleChannelInboundHandler<Object> {

	private static final Logger LOG = LoggerFactory.getLogger(PipelineErrorHandler.class);

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Object message) {
		// we can't deal with this message. No one in the pipeline handled it. Log it.
		LOG.debug("Unknown message received: {}", message);
		sendError(ctx, "Unknown message received.");
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		LOG.debug("Unhandled exception: {}", cause);
		sendError(ctx, cause.getMessage());
	}

	private void sendError(ChannelHandlerContext ctx, String error) {
		DefaultFullHttpResponse response;
		StringWriter writer = new StringWriter();
		try {
			JsonGenerator gen = JsonFactory.jacksonFactory.createJsonGenerator(writer);
			gen.writeStartObject();
			gen.writeStringField("error", error);
			gen.writeEndObject();
			gen.close();
			// send a bad request status code.
			response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
					HttpResponseStatus.BAD_REQUEST, Unpooled.wrappedBuffer(writer.toString().getBytes()));
			response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "application/json");
		} catch (IOException e) {
			// seriously? Let's just send some plain text.
			response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
					HttpResponseStatus.BAD_REQUEST, Unpooled.wrappedBuffer(error.getBytes()));
			response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain");
		}
		response.headers().set(HttpHeaders.Names.CONTENT_ENCODING, "utf-8");
		response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());
		if (ctx.channel().isActive()) {
			ctx.writeAndFlush(response);
		}
	}
}
