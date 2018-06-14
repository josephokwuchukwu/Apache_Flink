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

package org.apache.flink.runtime.rest.handler;

import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collections;
import java.util.List;

/**
 * An exception that is thrown if the failure of a REST operation was detected by a handler.
 */
public class RestHandlerException extends FlinkException {
	private static final long serialVersionUID = -1358206297964070876L;

	private final int responseCode;
	private final List<String> errorMessages;

	public RestHandlerException(String errorMessage, HttpResponseStatus httpResponseStatus) {
		this(Collections.singletonList(errorMessage), httpResponseStatus);
	}

	public RestHandlerException(List<String> errorMessages, HttpResponseStatus httpResponseStatus) {
		super(errorMessages.toString());
		this.errorMessages = errorMessages;
		this.responseCode = httpResponseStatus.code();
	}

	public RestHandlerException(String errorMessage, HttpResponseStatus httpResponseStatus, Throwable cause) {
		super(errorMessage, cause);
		this.errorMessages = Collections.singletonList(errorMessage);
		this.responseCode = httpResponseStatus.code();
	}

	public List<String> getMessages() {
		return errorMessages;
	}

	public HttpResponseStatus getHttpResponseStatus() {
		return HttpResponseStatus.valueOf(responseCode);
	}
}
