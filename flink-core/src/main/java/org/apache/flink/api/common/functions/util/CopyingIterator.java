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

package org.apache.flink.api.common.functions.util;

import java.util.Iterator;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.TraversableOnceException;

public class CopyingIterator<E> implements Iterator<E>, Iterable<E> {

	private final Iterator<E> source;
	private final TypeSerializer<E> serializer;
	
	private boolean available = true;
	
	public CopyingIterator(Iterator<E> source, TypeSerializer<E> serializer) {
		this.source = source;
		this.serializer = serializer;
	}

	@Override
	public Iterator<E> iterator() {
		if (available) {
			available = false;
			return this;
		} else {
			throw new TraversableOnceException();
		}
	}

	@Override
	public boolean hasNext() {
		return source.hasNext();
	}

	@Override
	public E next() {
		E next = source.next();
		return serializer.copy(next);
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
