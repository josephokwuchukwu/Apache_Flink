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

/**
 * A {@link StateDescriptor} for {@link ListState}. This can be used to create a partitioned
 * list state using
 * {@link org.apache.flink.api.common.functions.RuntimeContext#getListState(ListStateDescriptor)}.
 *
 * @param <T> The type of the elements in the list state.
 */
@PublicEvolving
public class ListStateDescriptor<T> extends SimpleStateDescriptor<T, ListState<T>> {
	private static final long serialVersionUID = 1L;

	/**
	 * Creates a new {@code ListStateDescriptor} with the given name and list element type.
	 *
	 * <p>If this constructor fails (because it is not possible to describe the type via a class),
	 * consider using the {@link #ListStateDescriptor(String, TypeInformation)} constructor.
	 *
	 * @param name The (unique) name for the state.
	 * @param elementTypeClass The type of the elements in the state.
	 */
	public ListStateDescriptor(String name, Class<T> elementTypeClass) {
		super(name, elementTypeClass);
	}

	/**
	 * Creates a new {@code ListStateDescriptor} with the given name and list element type.
	 *
	 * @param name The (unique) name for the state.
	 * @param elementTypeInfo The type of the elements in the state.
	 */
	public ListStateDescriptor(String name, TypeInformation<T> elementTypeInfo) {
		super(name, elementTypeInfo);
	}

	/**
	 * Creates a new {@code ListStateDescriptor} with the given name and list element type.
	 *
	 * @param name The (unique) name for the state.
	 * @param elementTypeSerializer The type serializer for the elements in the state.
	 */
	public ListStateDescriptor(String name, TypeSerializer<T> elementTypeSerializer) {
		super(name, elementTypeSerializer);
	}

	// ------------------------------------------------------------------------
	//  List State Descriptor
	// ------------------------------------------------------------------------

	@Override
	public Type getType() {
		return Type.LIST;
	}

	@Override
	public ListState<T> bind(StateBackend stateBackend) throws Exception {
		return stateBackend.createListState(this);
	}

	// ------------------------------------------------------------------------
	//  Standard utils
	// ------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		ListStateDescriptor<?> that = (ListStateDescriptor<?>) o;
		return name.equals(that.name) && simpleStateDescrEquals(that);
	}

	@Override
	public int hashCode() {
		int result = simpleStateDescrHashCode();
		result = 31 * result + name.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "ListStateDescriptor{" +
				simpleStateDescrToString() +
				'}';
	}
}
