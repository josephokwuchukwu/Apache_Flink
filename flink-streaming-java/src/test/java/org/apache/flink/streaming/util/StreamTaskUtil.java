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

package org.apache.flink.streaming.util;

import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.fail;

/**
 * Utils for working with StreamTask.
 */
public class StreamTaskUtil {

	public static void waitTaskIsRunning(StreamTask<?, ?> task, CompletableFuture<Void> taskInvocation) throws InterruptedException, ExecutionException {
		while (!task.isRunning()) {
			if (taskInvocation.isDone()) {
				taskInvocation.get();
				fail("Task has stopped");
			}
			Thread.sleep(10L);
		}
	}
}
