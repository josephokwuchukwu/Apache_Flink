/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.KeyGroupAssigner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Partitioner selects the target channel based on the virtual partition ID. The virtual parititon
 * ID is derived from the key of the elements using the virtual state partitioner.
 *
 * @param <T> Type of the elements in the Stream being partitioned
 */
@Internal
public class KeyGroupPartitioner<T, K> extends StreamPartitioner<T> implements ConfigurablePartitioner {
	private static final long serialVersionUID = 1L;

	private final int[] returnArray = new int[1];

	private final KeySelector<T, K> keySelector;

	private final KeyGroupAssigner<K> keyGroupAssigner;

	public KeyGroupPartitioner(KeySelector<T, K> keySelector, KeyGroupAssigner<K> keyGroupAssigner) {
		this.keySelector = keySelector;
		this.keyGroupAssigner = keyGroupAssigner;
	}

	public void setup(int maxParallelism) {
		keyGroupAssigner.setup(maxParallelism);
	}

	@Override
	public int[] selectChannels(SerializationDelegate<StreamRecord<T>> record,
			int numberOfOutputChannels) {
		K key;
		try {
			key = keySelector.getKey(record.getInstance().getValue());
		} catch (Exception e) {
			throw new RuntimeException("Could not extract key from " + record.getInstance().getValue(), e);
		}
		returnArray[0] = keyGroupAssigner.getKeyGroupIndex(key) % numberOfOutputChannels;

		return returnArray;
	}

	@Override
	public StreamPartitioner<T> copy() {
		return this;
	}

	@Override
	public String toString() {
		return "HASH";
	}

	@Override
	public void configure(int maxParallelism) {
		keyGroupAssigner.setup(maxParallelism);
	}
}
