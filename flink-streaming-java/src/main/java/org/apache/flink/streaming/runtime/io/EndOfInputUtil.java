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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;

/**
 * Utils for forwarding the end of input signal to the {@link BoundedOneInput} and {@link BoundedMultiInput}.
 */
public final class EndOfInputUtil {

	/**
	 * Notifies the given input about end of input.
	 */
	public static void endInput(Input<?> input) throws Exception {
		if (input instanceof BoundedOneInput) {
			((BoundedOneInput) input).endInput();
		}
	}

	/**
	 * Notifies the given operator about end of input.
	 */
	public static void endInput(OneInputStreamOperator<?, ?> operator) throws Exception {
		if (operator instanceof BoundedOneInput) {
			((BoundedOneInput) operator).endInput();
		}
	}

	/**
	 * Notifies the given operator about end of input of given index.
	 */
	public static void endInput(TwoInputStreamOperator<?, ?, ?> operator, int inputIdx) throws Exception {
		if (operator instanceof BoundedMultiInput) {
			((BoundedMultiInput) operator).endInput(inputIdx);
		}
	}

	/**
	 * Notifies the given operator about end of input of given index.
	 */
	public static void endInput(MultipleInputStreamOperator<?> operator, int inputIdx) throws Exception {
		if (operator instanceof BoundedOneInput && inputIdx == 1) {
			((BoundedOneInput) operator).endInput();
		}
		if (operator instanceof BoundedMultiInput) {
			((BoundedMultiInput) operator).endInput(inputIdx);
		}
	}

	private EndOfInputUtil() {
	}
}
