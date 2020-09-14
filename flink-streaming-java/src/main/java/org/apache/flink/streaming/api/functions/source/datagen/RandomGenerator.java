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

package org.apache.flink.streaming.api.functions.source.datagen;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;

import org.apache.commons.math3.random.RandomDataGenerator;

import java.math.BigDecimal;

/**
 * Random generator.
 */
@Experimental
public abstract class RandomGenerator<T> implements DataGenerator<T> {

	protected transient RandomDataGenerator random;

	@Override
	public void open(
			String name,
			FunctionInitializationContext context,
			RuntimeContext runtimeContext) throws Exception {
		this.random = new RandomDataGenerator();
	}

	@Override
	public boolean hasNext() {
		return true;
	}

	public static RandomGenerator<Long> longGenerator(long min, long max) {
		return new RandomGenerator<Long>() {
			@Override
			public Long next() {
				return random.nextLong(min, max);
			}
		};
	}

	public static RandomGenerator<Integer> intGenerator(int min, int max) {
		return new RandomGenerator<Integer>() {
			@Override
			public Integer next() {
				return random.nextInt(min, max);
			}
		};
	}

	public static RandomGenerator<Short> shortGenerator(short min, short max) {
		return new RandomGenerator<Short>() {
			@Override
			public Short next() {
				return (short) random.nextInt(min, max);
			}
		};
	}

	public static RandomGenerator<Byte> byteGenerator(byte min, byte max) {
		return new RandomGenerator<Byte>() {
			@Override
			public Byte next() {
				return (byte) random.nextInt(min, max);
			}
		};
	}

	public static RandomGenerator<Float> floatGenerator(float min, float max) {
		return new RandomGenerator<Float>() {
			@Override
			public Float next() {
				return (float) random.nextUniform(min, max);
			}
		};
	}

	public static RandomGenerator<Double> doubleGenerator(double min, double max) {
		return new RandomGenerator<Double>() {
			@Override
			public Double next() {
				return random.nextUniform(min, max);
			}
		};
	}

	public static RandomGenerator<BigDecimal> decimalGenerator(BigDecimal min, BigDecimal max) {
		return new RandomGenerator<BigDecimal>() {
			@Override
			public BigDecimal next() {
				return BigDecimal.valueOf(random.nextUniform(min.doubleValue(), max.doubleValue()));
			}
		};
	}

	public static RandomGenerator<String> stringGenerator(int len) {
		return new RandomGenerator<String>() {
			@Override
			public String next() {
				return random.nextHexString(len);
			}
		};
	}

	public static RandomGenerator<Boolean> booleanGenerator() {
		return new RandomGenerator<Boolean>() {
			@Override
			public Boolean next() {
				return random.nextInt(0, 1) == 0;
			}
		};
	}
}
