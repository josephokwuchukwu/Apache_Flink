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


package org.apache.flink.types.parser;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Parses a text field into a {@link Float}.
 */
@PublicEvolving
public class FloatParser extends FieldParser<Float> {

	private float result;
	
	@Override
	public int parseField(byte[] bytes, int startPos, int limit, byte[] delimiter, Float reusable) {
		final int endPos = nextNumericStringEndPos(bytes, startPos, limit, delimiter);
		if (endPos < 0) {
			return -1;
		}

		String str = new String(bytes, startPos, endPos - startPos);
		try {
			this.result = Float.parseFloat(str);
			return (endPos == limit) ? limit : endPos + delimiter.length;
		} catch (NumberFormatException e) {
			setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
			return -1;
		}
	}

	@Override
	public Float createValue() {
		return Float.MIN_VALUE;
	}

	@Override
	public Float getLastResult() {
		return Float.valueOf(this.result);
	}

	/**
	 * Static utility to parse a field of type float from a byte sequence that represents text 
	 * characters
	 * (such as when read from a file stream).
	 *
	 * @param bytes    The bytes containing the text data that should be parsed.
	 * @param startPos The offset to start the parsing.
	 * @param length   The length of the byte sequence (counting from the offset).
	 * @return The parsed value.
	 * @throws NumberFormatException Thrown when the value cannot be parsed because the text 
	 * represents not a correct number.
	 */
	public static final float parseField(byte[] bytes, int startPos, int length) {
		return parseField(bytes, startPos, length, (char) 0xffff);
	}

	/**
	 * Static utility to parse a field of type float from a byte sequence that represents text 
	 * characters
	 * (such as when read from a file stream).
	 *
	 * @param bytes     The bytes containing the text data that should be parsed.
	 * @param startPos  The offset to start the parsing.
	 * @param length    The length of the byte sequence (counting from the offset).
	 * @param delimiter The delimiter that terminates the field.
	 * @return The parsed value.
	 * @throws NumberFormatException Thrown when the value cannot be parsed because the text 
	 * represents not a correct number.
	 */
	public static final float parseField(byte[] bytes, int startPos, int length, char delimiter) {
		final String str = nextNumericString(bytes, startPos, length, delimiter);
		return Float.parseFloat(str);
	}
}
