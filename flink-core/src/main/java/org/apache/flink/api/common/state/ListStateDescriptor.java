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

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.java.typeutils.ListTypeInfo;

import java.util.List;

/**
 * A {@link StateDescriptor} for {@link ListState}. This can be used to create a partitioned
 * list state using
 * {@link org.apache.flink.api.common.functions.RuntimeContext#getListState(ListStateDescriptor)}.
 *
 * @param <T> The type of the values that can be added to the list state.
 */
@PublicEvolving
public class ListStateDescriptor<T> extends StateDescriptor<ListState<T>, List<T>> {
	private static final long serialVersionUID = 2L;

	/**
	 * Creates a new {@code ListStateDescriptor} with the given name and list element type.
	 *
	 * <p>If this constructor fails (because it is not possible to describe the type via a class),
	 * consider using the {@link #ListStateDescriptor(String, TypeInformation)} constructor.
	 *
	 * @param name The (unique) name for the state.
	 * @param elementTypeClass The type of the elements in the state.
	 */
	@SuppressWarnings("unchecked")
	public ListStateDescriptor(String name, Class<T> elementTypeClass) {
		super(name, new ListTypeInfo<>(elementTypeClass), null);
	}

	/**
	 * Creates a new {@code ListStateDescriptor} with the given name and list element type.
	 *
	 * @param name The (unique) name for the state.
	 * @param elementTypeInfo The type of the elements in the state.
	 */
	@SuppressWarnings("unchecked")
	public ListStateDescriptor(String name, TypeInformation<T> elementTypeInfo) {
		super(name, new ListTypeInfo<>(elementTypeInfo), null);
	}

	/**
	 * Creates a new {@code ListStateDescriptor} with the given name and list element type.
	 *
	 * @param name The (unique) name for the state.
	 * @param typeSerializer The type serializer for the list values.
	 */
	@SuppressWarnings("unchecked")
	public ListStateDescriptor(String name, TypeSerializer<T> typeSerializer) {
		super(name, new ListSerializer<>(typeSerializer), null);
	}

	public TypeSerializer<T> getElementSerializer() {
		if (!(serializer instanceof ListSerializer)) {
			throw new IllegalStateException();
		}

		return ((ListSerializer<T>)serializer).getElementSerializer();
	}

	// ------------------------------------------------------------------------

	@Override
	public ListState<T> bind(StateBackend stateBackend) throws Exception {
		return stateBackend.createListState(this);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		ListStateDescriptor<?> that = (ListStateDescriptor<?>) o;

		return serializer.equals(that.serializer) && name.equals(that.name);

	}

	@Override
	public int hashCode() {
		int result = serializer.hashCode();
		result = 31 * result + name.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "ListStateDescriptor{" +
				"serializer=" + serializer +
				'}';
	}

	@Override
	public Type getType() {
		return Type.LIST;
	}
}
