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

package org.apache.flink.graph;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.NullValue;

/**
 * Represents the graph's nodes. It carries an ID and a value.
 * For vertices with no value, use {@link org.apache.flink.types.NullValue} as the value type.
 *
 * @param <K> type of vertex id
 * @param <V> type of vertex value
 */
public class Vertex<K, V> extends Tuple2<K, V> {

	private static final long serialVersionUID = 1L;

	public static <K> Vertex<K, NullValue> create(K id) {
		return new Vertex<>(id, NullValue.getInstance());
	}

	public static <K, V> Vertex<K, V> create(K id, V value) {
		return new Vertex<>(id, value);
	}

	public Vertex(){}

	public Vertex(K id, V value) {
		this.f0 = id;
		this.f1 = value;
	}

	public K getId() {
		return this.f0;
	}

	public V getValue() {
		return this.f1;
	}

	public void setId(K id) {
		this.f0 = id;
	}

	public void setValue(V value) {
		this.f1 = value;
	}
}
