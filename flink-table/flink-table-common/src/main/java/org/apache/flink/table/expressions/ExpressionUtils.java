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

package org.apache.flink.table.expressions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.Optional;

/**
 * Utility methods for working with {@link Expression}s.
 */
@Internal
public final class ExpressionUtils {

	/**
	 * Extracts value of given type from expression assuming it is a {@link ValueLiteralExpression}.
	 *
	 * @param expr literal to extract the value from
	 * @param type expected type to extract from the literal
	 * @param <V> type of extracted value
	 * @return extracted value or empty if could not extract value of given type
	 */
	@SuppressWarnings("unchecked")
	public static <V> Optional<V> extractValue(Expression expr, TypeInformation<V> type) {
		if (expr instanceof ValueLiteralExpression) {
			final ValueLiteralExpression valueLiteral = (ValueLiteralExpression) expr;
			if (valueLiteral.getType().equals(type)) {
				return Optional.of((V) valueLiteral.getValue());
			}
		}
		return Optional.empty();
	}

	/**
	 * Checks if the expression is a function call of given type.
	 *
	 * @param expr expression to check
	 * @param type expected type of function
	 * @return true if the expression is function call of given type, false otherwise
	 */
	public static boolean isFunctionOfType(Expression expr, FunctionDefinition.Type type) {
		return expr instanceof CallExpression &&
			((CallExpression) expr).getFunctionDefinition().getType() == type;
	}

	private ExpressionUtils() {
	}
}
