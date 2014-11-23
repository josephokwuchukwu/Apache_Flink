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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.EnumComparator;
import org.apache.flink.api.common.typeutils.base.EnumSerializer;

public class EnumTypeInfo<T extends Enum<T>> extends TypeInformation<T> implements AtomicType<T> {

	private final Class<T> typeClass;

	public EnumTypeInfo(Class<T> typeClass) {
		if (typeClass == null) {
			throw new NullPointerException();
		}
		if (!Enum.class.isAssignableFrom(typeClass) ) {
			throw new IllegalArgumentException("EnumTypeInfo can only be used for subclasses of " + Enum.class.getName());
		}
		this.typeClass = typeClass;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public TypeComparator<T> createComparator(boolean sortOrderAscending) {
		return new EnumComparator<T>(sortOrderAscending);
	}

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return 1;
	}
	
	@Override
	public int getTotalFields() {
		return 1;
	}

	@Override
	public Class<T> getTypeClass() {
		return this.typeClass;
	}

	@Override
	public boolean isKeyType() {
		return true;
	}

	@Override
	public TypeSerializer<T> createSerializer() {
		return new EnumSerializer<T>(typeClass);
	}
	
	@Override
	public String toString() {
		return "EnumTypeInfo<" + typeClass.getName() + ">";
	}	
	
	@Override
	public int hashCode() {
		return typeClass.hashCode() ^ 0xd3a2646c;
	}
	
	@Override
	public boolean equals(Object obj) {
		return obj instanceof EnumTypeInfo && typeClass == ((EnumTypeInfo<?>) obj).typeClass;
	}
}
