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

package org.apache.flink.cep.pattern.conditions;

import org.apache.flink.annotation.Internal;

/**
 * A {@link IterativeCondition condition} which negates the condition it wraps
 * and returns {@code true} if the original condition returns {@code false}.
 *
 * @param <T> Type of the element to filter
 * @deprecated Please use {@link RichNotCondition} instead. This class exists just for
 * backwards compatibility and will be removed in FLINK-10113.
 */
@Internal
@Deprecated
public class NotCondition<T> extends IterativeCondition<T> {
	private static final long serialVersionUID = -2109562093871155005L;

	private final IterativeCondition<T> original;

	public NotCondition(final IterativeCondition<T> original) {
		this.original = original;
	}

	@Override
	public boolean filter(T value, Context<T> ctx) throws Exception {
		return original != null && !original.filter(value, ctx);
	}
}
