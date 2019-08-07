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

package org.apache.flink.api.common.typeutils.base.array;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.DoublePrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.LongPrimitiveArraySerializer;

import java.util.Arrays;

/**
 * A test for the {@link LongPrimitiveArraySerializer}.
 */
public class DoublePrimitiveArraySerializerTest extends SerializerTestBase<double[]> {

	@Override
	protected TypeSerializer<double[]> createSerializer() {
		return new DoublePrimitiveArraySerializer();
	}

	@Override
	protected Class<double[]> getTypeClass() {
		return double[].class;
	}
	
	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected double[][] getTestData() {
		return new double[][] {
			new double[] {0, 1, 2, 3, -1, -2, -3, Integer.MAX_VALUE, Integer.MIN_VALUE, Double.MAX_VALUE, Double.MIN_VALUE},
			new double[] {Double.NEGATIVE_INFINITY},
			new double[] {},
			new double[] {-1, -2, 96769243, Double.NaN, Double.POSITIVE_INFINITY, 26782, Double.MIN_NORMAL, 0, 0, 0}
		};
	}

	@Override
	protected boolean isObjectEquals(double[] originalArray, double[] deserializedArray) {
		return Arrays.equals(originalArray, deserializedArray);
	}
}
