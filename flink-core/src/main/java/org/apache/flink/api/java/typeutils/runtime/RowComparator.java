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
package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.common.typeutils.CompositeTypeComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.types.KeyFieldOutOfBoundsException;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.api.java.typeutils.runtime.NullMaskUtils.readIntoNullMask;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Comparator for {@link Row}
 */
public class RowComparator extends CompositeTypeComparator<Row> {

	private static final long serialVersionUID = 1L;
	/** The number of fields of the Row */
	private final int arity;
	/** key positions describe which fields are keys in what order */
	private final int[] keyPositions;
	/** null-aware comparators for the key fields, in the same order as the key fields */
	private final NullAwareComparator<Object>[] comparators;
	/** serializers to deserialize the first n fields for comparison */
	private final TypeSerializer<Object>[] serializers;
	/** auxiliary fields for normalized key support */
	private final int[] normalizedKeyLengths;
	private final int numLeadingNormalizableKeys;
	private final int normalizableKeyPrefixLen;
	private final boolean invertNormKey;

	// null masks for serialized comparison
	private final boolean[] nullMask1;
	private final boolean[] nullMask2;

	// cache for the deserialized key field objects
	transient private final Object[] deserializedKeyFields1;
	transient private final Object[] deserializedKeyFields2;

	/**
	 * General constructor for RowComparator.
	 *
	 * @param arity        the number of fields of the Row
	 * @param keyPositions key positions describe which fields are keys in what order
	 * @param comparators  non-null-aware comparators for the key fields, in the same order as
	 *                     the key fields
	 * @param serializers  serializers to deserialize the first n fields for comparison
	 * @param orders       sorting orders for the fields
	 */
	public RowComparator(
		int arity,
		int[] keyPositions,
		TypeComparator<Object>[] comparators,
		TypeSerializer<Object>[] serializers,
		boolean[] orders) {
		this(arity, keyPositions, makeNullAware(comparators, orders), serializers);
	}


	/**
	 * Intermediate constructor for creating auxiliary fields.
	 */
	private RowComparator(
		int arity,
		int[] keyPositions,
		NullAwareComparator<Object>[] comparators,
		TypeSerializer<Object>[] serializers) {
		this(
			arity,
			keyPositions,
			comparators,
			serializers,
			createAuxiliaryFields(keyPositions, comparators));
	}

	/**
	 * Intermediate constructor for creating auxiliary fields.
	 */
	private RowComparator(
		int arity,
		int[] keyPositions,
		NullAwareComparator<Object>[] comparators,
		TypeSerializer<Object>[] serializers,
		Tuple4<int[], Integer, Integer, Boolean> auxiliaryFields) {
		this(
			arity,
			keyPositions,
			comparators,
			serializers,
			auxiliaryFields.f0,
			auxiliaryFields.f1,
			auxiliaryFields.f2,
			auxiliaryFields.f3);
	}

	/**
	 * Intermediate constructor for creating auxiliary fields.
	 */
	private RowComparator(
		int arity,
		int[] keyPositions,
		NullAwareComparator<Object>[] comparators,
		TypeSerializer<Object>[] serializers,
		int[] normalizedKeyLengths,
		int numLeadingNormalizableKeys, int normalizableKeyPrefixLen, boolean invertNormKey) {
		this.arity = arity;
		this.keyPositions = keyPositions;
		this.comparators = comparators;
		this.serializers = serializers;
		this.normalizedKeyLengths = normalizedKeyLengths;
		this.numLeadingNormalizableKeys = numLeadingNormalizableKeys;
		this.normalizableKeyPrefixLen = normalizableKeyPrefixLen;
		this.invertNormKey = invertNormKey;
		this.nullMask1 = new boolean[arity];
		this.nullMask2 = new boolean[arity];
		deserializedKeyFields1 = instantiateDeserializationFields();
		deserializedKeyFields2 = instantiateDeserializationFields();
	}

	// --------------------------------------------------------------------------------------------
	//  Comparator Methods
	// --------------------------------------------------------------------------------------------

	@Override
	public void getFlatComparator(List<TypeComparator> flatComparators) {
		for (NullAwareComparator<Object> c : comparators) {
			Collections.addAll(flatComparators, c.getFlatComparators());
		}
	}

	@Override
	public int hash(Row record) {
		int code = 0;
		int i = 0;

		try {
			for (; i < keyPositions.length; i++) {
				code *= TupleComparatorBase.HASH_SALT[i & 0x1F];
				Object element = record.getField(keyPositions[i]); // element can be null
				code += comparators[i].hash(element);
			}
		} catch (IndexOutOfBoundsException e) {
			throw new KeyFieldOutOfBoundsException(keyPositions[i]);
		}

		return code;
	}

	@Override
	public void setReference(Row toCompare) {
		int i = 0;
		try {
			for (; i < keyPositions.length; i++) {
				TypeComparator<Object> comparator = comparators[i];
				Object element = toCompare.getField(keyPositions[i]);
				comparator.setReference(element);   // element can be null
			}
		} catch (IndexOutOfBoundsException e) {
			throw new KeyFieldOutOfBoundsException(keyPositions[i]);
		}
	}

	@Override
	public boolean equalToReference(Row candidate) {
		int i = 0;
		try {
			for (; i < keyPositions.length; i++) {
				TypeComparator<Object> comparator = comparators[i];
				Object element = candidate.getField(keyPositions[i]);   // element can be null
				// check if reference is not equal
				if (!comparator.equalToReference(element)) {
					return false;
				}
			}
		} catch (IndexOutOfBoundsException e) {
			throw new KeyFieldOutOfBoundsException(keyPositions[i]);
		}
		return true;
	}

	@Override
	public int compareToReference(TypeComparator<Row> referencedComparator) {
		RowComparator other = (RowComparator) referencedComparator;
		int i = 0;
		try {
			for (; i < keyPositions.length; i++) {
				int cmp = comparators[i].compareToReference(other.comparators[i]);
				if (cmp != 0) {
					return cmp;
				}
			}
		} catch (IndexOutOfBoundsException e) {
			throw new KeyFieldOutOfBoundsException(keyPositions[i]);
		}
		return 0;
	}

	@Override
	public int compare(Row first, Row second) {
		int i = 0;
		try {
			for (; i < keyPositions.length; i++) {
				int keyPos = keyPositions[i];
				TypeComparator<Object> comparator = comparators[i];
				Object firstElement = first.getField(keyPos);   // element can be null
				Object secondElement = second.getField(keyPos); // element can be null

				int cmp = comparator.compare(firstElement, secondElement);
				if (cmp != 0) {
					return cmp;
				}
			}
		} catch (IndexOutOfBoundsException e) {
			throw new KeyFieldOutOfBoundsException(keyPositions[i]);
		}
		return 0;
	}

	@Override
	public int compareSerialized(
		DataInputView firstSource,
		DataInputView secondSource) throws IOException {

		int len = serializers.length;
		int keyLen = keyPositions.length;

		readIntoNullMask(arity, firstSource, nullMask1);
		readIntoNullMask(arity, secondSource, nullMask2);

		// deserialize
		for (int i = 0; i < len; i++) {
			TypeSerializer<Object> serializer = serializers[i];

			// deserialize field 1
			if (!nullMask1[i]) {
				deserializedKeyFields1[i] = serializer.deserialize(
					deserializedKeyFields1[i],
					firstSource);
			}

			// deserialize field 2
			if (!nullMask2[i]) {
				deserializedKeyFields2[i] = serializer.deserialize(
					deserializedKeyFields2[i],
					secondSource);
			}
		}

		// compare
		for (int i = 0; i < keyLen; i++) {
			int keyPos = keyPositions[i];
			TypeComparator<Object> comparator = comparators[i];

			boolean isNull1 = nullMask1[keyPos];
			boolean isNull2 = nullMask2[keyPos];

			int cmp = 0;
			// both values are null -> equality
			if (isNull1 && isNull2) {
				cmp = 0;
			}
			// first value is null -> inequality
			else if (isNull1) {
				cmp = comparator.compare(null, deserializedKeyFields2[keyPos]);
			}
			// second value is null -> inequality
			else if (isNull2) {
				cmp = comparator.compare(deserializedKeyFields1[keyPos], null);
			}
			// no null values
			else {
				cmp = comparator.compare(
					deserializedKeyFields1[keyPos],
					deserializedKeyFields2[keyPos]);
			}

			if (cmp != 0) {
				return cmp;
			}
		}

		return 0;
	}

	@Override
	public boolean supportsNormalizedKey() {
		return numLeadingNormalizableKeys > 0;
	}

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	@Override
	public int getNormalizeKeyLen() {
		return normalizableKeyPrefixLen;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return numLeadingNormalizableKeys < keyPositions.length ||
		       normalizableKeyPrefixLen == Integer.MAX_VALUE ||
		       normalizableKeyPrefixLen > keyBytes;
	}

	@Override
	public void putNormalizedKey(
		Row record, MemorySegment target, int offset, int numBytes) {
		int bytesLeft = numBytes;
		int currentOffset = offset;

		for (int i = 0; i < numLeadingNormalizableKeys && bytesLeft > 0; i++) {
			int len = normalizedKeyLengths[i];
			len = bytesLeft >= len ? len : bytesLeft;

			TypeComparator<Object> comparator = comparators[i];
			Object element = record.getField(keyPositions[i]);  // element can be null
			// write key
			comparator.putNormalizedKey(element, target, currentOffset, len);

			bytesLeft -= len;
			currentOffset += len;
		}

	}

	@Override
	public void writeWithKeyNormalization(Row record, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException(
			"Record serialization with leading normalized keys not supported.");
	}

	@Override
	public Row readWithKeyDenormalization(Row reuse, DataInputView source) throws IOException {
		throw new UnsupportedOperationException(
			"Record deserialization with leading normalized keys not supported.");
	}

	@Override
	public boolean invertNormalizedKey() {
		return invertNormKey;
	}

	@Override
	public TypeComparator<Row> duplicate() {
		NullAwareComparator<?>[] comparatorsCopy = new NullAwareComparator<?>[comparators.length];
		for (int i = 0; i < comparators.length; i++) {
			comparatorsCopy[i] = (NullAwareComparator<?>) comparators[i].duplicate();
		}

		TypeSerializer<?>[] serializersCopy = new TypeSerializer<?>[serializers.length];
		for (int i = 0; i < serializers.length; i++) {
			serializersCopy[i] = serializers[i].duplicate();
		}

		return new RowComparator(
			arity,
			keyPositions,
			(NullAwareComparator<Object>[]) comparatorsCopy,
			(TypeSerializer<Object>[]) serializersCopy,
			normalizedKeyLengths,
			numLeadingNormalizableKeys,
			normalizableKeyPrefixLen,
			invertNormKey);
	}

	@Override
	public int extractKeys(Object record, Object[] target, int index) {
		int len = comparators.length;
		int localIndex = index;
		for (int i = 0; i < len; i++) {
			Object element = ((Row) record).getField(keyPositions[i]);  // element can be null
			localIndex += comparators[i].extractKeys(element, target, localIndex);
		}
		return localIndex - index;
	}


	private Object[] instantiateDeserializationFields() {
		Object[] newFields = new Object[serializers.length];
		for (int i = 0; i < serializers.length; i++) {
			newFields[i] = serializers[i].createInstance();
		}
		return newFields;
	}

	/**
	 * @return creates auxiliary fields for normalized key support
	 */
	private static Tuple4<int[], Integer, Integer, Boolean>
	createAuxiliaryFields(int[] keyPositions, NullAwareComparator<Object>[] comparators) {
		int[] normalizedKeyLengths = new int[keyPositions.length];
		int numLeadingNormalizableKeys = 0;
		int normalizableKeyPrefixLen = 0;
		boolean inverted = false;

		for (int i = 0; i < keyPositions.length; i++) {
			NullAwareComparator<Object> k = comparators[i];
			// as long as the leading keys support normalized keys, we can build up the composite key
			if (k.supportsNormalizedKey()) {
				if (i == 0) {
					// the first comparator decides whether we need to invert the key direction
					inverted = k.invertNormalizedKey();
				} else if (k.invertNormalizedKey() != inverted) {
					// if a successor does not agree on the inversion direction, it cannot be part of the
					// normalized key
					return new Tuple4<>(
						normalizedKeyLengths,
						numLeadingNormalizableKeys,
						normalizableKeyPrefixLen,
						inverted);
				}
				numLeadingNormalizableKeys++;
				int len = k.getNormalizeKeyLen();
				if (len < 0) {
					throw new RuntimeException(
						"Comparator " + k.getClass().getName() +
						" specifies an invalid length for the normalized key: " + len);
				}
				normalizedKeyLengths[i] = len;
				normalizableKeyPrefixLen += len;
				if (normalizableKeyPrefixLen < 0) {
					// overflow, which means we are out of budget for normalized key space anyways
					return new Tuple4<>(
						normalizedKeyLengths,
						numLeadingNormalizableKeys,
						Integer.MAX_VALUE,
						inverted);
				}
			} else {
				return new Tuple4<>(
					normalizedKeyLengths,
					numLeadingNormalizableKeys,
					normalizableKeyPrefixLen,
					inverted);
			}
		}
		return new Tuple4<>(
			normalizedKeyLengths,
			numLeadingNormalizableKeys,
			normalizableKeyPrefixLen,
			inverted);
	}

	private static NullAwareComparator<Object>[] makeNullAware(
		TypeComparator<Object>[] comparators,
		boolean[] orders) {
		checkArgument(comparators.length == orders.length);
		NullAwareComparator<?>[] result = new NullAwareComparator<?>[comparators.length];
		for (int i = 0; i < comparators.length; i++) {
			result[i] = new NullAwareComparator<Object>(comparators[i], orders[i]);
		}
		return (NullAwareComparator<Object>[]) result;
	}


	/**
	 * Null-aware comparator that wraps a comparator which does not support null references.
	 * <p>
	 * NOTE: This class assumes to be used within a composite type comparator (such
	 * as {@link RowComparator}) that handles serialized comparison.
	 */
	public static class NullAwareComparator<T> extends TypeComparator<T> {
		private static final long serialVersionUID = 1L;

		private final TypeComparator<T> wrappedComparator;
		private final boolean order;

		// number of flat fields
		private final int flatFields;

		// stores the null for reference comparison
		private boolean nullReference = false;

		public NullAwareComparator(TypeComparator<T> wrappedComparator, boolean order) {
			this.wrappedComparator = wrappedComparator;
			this.order = order;
			this.flatFields = wrappedComparator.getFlatComparators().length;
		}

		@Override
		public int hash(T record) {
			if (record != null) {
				return wrappedComparator.hash(record);
			} else {
				return 0;
			}
		}

		@Override
		public void setReference(T toCompare) {
			if (toCompare == null) {
				nullReference = true;
			} else {
				nullReference = false;
				wrappedComparator.setReference(toCompare);
			}
		}

		@Override
		public boolean equalToReference(T candidate) {
			// both values are null
			if (candidate == null && nullReference) {
				return true;
			}
			// one value is null
			else if (candidate == null || nullReference) {
				return false;
			}
			// no null value
			else {
				return wrappedComparator.equalToReference(candidate);
			}
		}

		@Override
		public int compareToReference(TypeComparator<T> referencedComparator) {
			NullAwareComparator otherComparator = (NullAwareComparator) referencedComparator;
			boolean otherNullReference = otherComparator.nullReference;
			// both values are null -> equality
			if (nullReference && otherNullReference) {
				return 0;
			}
			// first value is null -> inequality
			// but order is considered
			else if (nullReference) {
				return order ? 1 : -1;
			}
			// second value is null -> inequality
			// but order is considered
			else if (otherNullReference) {
				return order ? -1 : 1;
			}
			// no null values
			else {
				return wrappedComparator.compareToReference(otherComparator.wrappedComparator);
			}
		}

		@Override
		public int compare(T first, T second) {
			// both values are null -> equality
			if (first == null && second == null) {
				return 0;
			}
			// first value is null -> inequality
			// but order is considered
			else if (first == null) {
				return order ? -1 : 1;
			}
			// second value is null -> inequality
			// but order is considered
			else if (second == null) {
				return order ? 1 : -1;
			}
			// no null values
			else {
				return wrappedComparator.compare(first, second);
			}
		}

		@Override
		public int compareSerialized(
			DataInputView firstSource,
			DataInputView secondSource) throws IOException {

			throw new UnsupportedOperationException(
				"Comparator does not support null-aware serialized comparision.");
		}

		@Override
		public boolean supportsNormalizedKey() {
			return wrappedComparator.supportsNormalizedKey();
		}

		@Override
		public boolean supportsSerializationWithKeyNormalization() {
			return false;
		}

		@Override
		public int getNormalizeKeyLen() {
			int len = wrappedComparator.getNormalizeKeyLen();
			if (len == Integer.MAX_VALUE) {
				return Integer.MAX_VALUE;
			} else {
				// add one for a null byte
				return len + 1;
			}
		}

		@Override
		public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
			return wrappedComparator.isNormalizedKeyPrefixOnly(keyBytes - 1);
		}

		@Override
		public void putNormalizedKey(T record, MemorySegment target, int offset, int numBytes) {
			if (numBytes > 0) {
				// write a null byte with padding
				if (record == null) {
					target.putBoolean(offset, false);
					// write padding
					for (int j = 0; j < numBytes - 1; j++) {
						target.put(offset + 1 + j, (byte) 0);
					}
				}
				// write a non-null byte with key
				else {
					target.putBoolean(offset, true);
					// write key
					wrappedComparator.putNormalizedKey(record, target, offset + 1, numBytes - 1);
				}
			}
		}

		@Override
		public void writeWithKeyNormalization(T record, DataOutputView target) throws IOException {
			throw new UnsupportedOperationException(
				"Record serialization with leading normalized keys not supported.");
		}

		@Override
		public T readWithKeyDenormalization(T reuse, DataInputView source) throws IOException {
			throw new UnsupportedOperationException(
				"Record deserialization with leading normalized keys not supported.");
		}

		@Override
		public boolean invertNormalizedKey() {
			return wrappedComparator.invertNormalizedKey();
		}

		@Override
		public TypeComparator<T> duplicate() {
			return new NullAwareComparator<T>(wrappedComparator.duplicate(), order);
		}

		@Override
		public int extractKeys(Object record, Object[] target, int index) {
			if (record == null) {
				for (int i = 0; i < flatFields; i++) {
					target[index + i] = null;
				}
				return flatFields;
			} else {
				return wrappedComparator.extractKeys(record, target, index);
			}
		}

		@Override
		public TypeComparator[] getFlatComparators() {
			// determine the flat comparators and wrap them again in null-aware comparators
			List<TypeComparator<?>> flatComparators = new ArrayList<>();
			if (wrappedComparator instanceof CompositeTypeComparator) {
				((CompositeTypeComparator) wrappedComparator).getFlatComparator(flatComparators);
			} else {
				flatComparators.add(wrappedComparator);
			}

			TypeComparator<?>[] result = new TypeComparator[flatComparators.size()];
			for (int i = 0; i < result.length; i++) {
				result[i] = new NullAwareComparator<>(flatComparators.get(i), order);
			}
			return result;
		}
	}
}
