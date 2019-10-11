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

package org.apache.flink.formats.csv;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvRowDeserializationSchema.RuntimeConverter;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.MappingIterator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import org.apache.commons.compress.utils.BoundedInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.NoSuchElementException;

import static org.apache.flink.formats.csv.CsvRowDeserializationSchema.createFieldRuntimeConverters;
import static org.apache.flink.formats.csv.CsvRowDeserializationSchema.validateArity;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Input format that reads csv into {@link Row}.
 *
 * <p>Different from old csv {@code org.apache.flink.api.java.io.RowCsvInputFormat}:
 * 1.New csv will emit this row (Fill null the remaining fields) when row is too short.
 *   But Old csv will skip this too short row.
 * 2.New csv, escape char will be removed. But old csv will keep the escape char.
 *
 * <p>These can be continuously improved in new csv input format:
 * 1.New csv not support configure comment char. The comment char is "#".
 * 2.New csv not support configure multi chars field delimiter.
 * 3.New csv not support read first N, it will throw exception.
 * 4.Only support configure line delimiter: "\r" or "\n" or "\r\n".
 */
public class RowCsvInputFormat extends FileInputFormat<Row> {

	private static final long serialVersionUID = 1L;

	private final TypeInformation[] fieldTypes;
	private final CsvSchema csvSchema;

	private final String[] selectedFieldNames;
	private final boolean ignoreParseErrors;

	/** Runtime instance that performs the actual work. */
	private transient RuntimeConverter runtimeConverter;

	private transient MappingIterator<JsonNode> iterator;

	private transient boolean end;

	private RowCsvInputFormat(
			Path[] filePaths,
			TypeInformation[] fieldTypes,
			CsvSchema csvSchema,
			int[] selectedFields,
			boolean ignoreParseErrors) {
		setFilePaths(filePaths);

		this.fieldTypes = checkNotNull(fieldTypes);
		this.csvSchema = checkNotNull(csvSchema);
		checkArgument(fieldTypes.length == csvSchema.size());
		this.ignoreParseErrors = ignoreParseErrors;
		this.selectedFieldNames = Arrays.stream(checkNotNull(selectedFields))
				.mapToObj(csvSchema::columnName).toArray(String[]::new);
	}

	@Override
	public boolean supportsMultiPaths() {
		return true;
	}

	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);

		prepareConverter();

		InputStream realStream = stream;

		long realStart = splitStart;
		if (splitStart != 0) {
			realStart = findNextLegalSeparator();
		}

		if (splitLength != READ_WHOLE_SPLIT_FLAG) {
			stream.seek(splitStart + splitLength);
			long firstByteOfNextLine = findNextLegalSeparator();
			stream.seek(realStart);
			realStream = new BoundedInputStream(stream, firstByteOfNextLine - realStart);
		} else {
			stream.seek(realStart);
		}

		this.iterator = new CsvMapper().readerFor(JsonNode.class).with(csvSchema).readValues(realStream);
	}

	private void prepareConverter() {
		RuntimeConverter[] fieldConverters = createFieldRuntimeConverters(ignoreParseErrors, fieldTypes);

		this.runtimeConverter = (node) -> {
			final int nodeSize = node.size();

			validateArity(csvSchema.size(), nodeSize, ignoreParseErrors);

			Row row = new Row(selectedFieldNames.length);
			for (int i = 0; i < Math.min(selectedFieldNames.length, nodeSize); i++) {
				// Jackson only supports mapping by name in the first level
				row.setField(i, fieldConverters[i].convert(node.get(selectedFieldNames[i])));
			}
			return row;
		};
	}

	/**
	 * Find next legal line separator and set next offset (first byte offset of next line) to stream.
	 *
	 * NOTE: Because of the particularity of UTF-8 encoding, we can determine the number of bytes
	 * of this character only by comparing the first byte, so we do not need to traverse M*N in comparison.
	 */
	private long findNextLegalSeparator() throws IOException {
		boolean usesEscapeChar = csvSchema.usesEscapeChar();
		byte[] escapeBytes = Character.toString((char) csvSchema.getEscapeChar())
				.getBytes(StandardCharsets.UTF_8);
		long startPos = stream.getPos();

		byte b;
		while ((b = (byte) stream.read()) != -1) {
			if (b == '\r' || b == '\n') {
				// If there may be escape tags ahead
				if (usesEscapeChar && stream.getPos() - startPos <= escapeBytes.length) {
					long front = stream.getPos() - escapeBytes.length - 1;
					if (front > 0) {
						stream.seek(front);
						byte[] readBytes = new byte[escapeBytes.length];
						stream.read(readBytes); // we have judge front must bigger than zero
						stream.read(); // back to current next one
						if (Arrays.equals(escapeBytes, readBytes)) {
							// equal, we should skip this one line separator
							continue;
						}
					}
				}

				long pos = stream.getPos();

				// deal with "\r\n", next one maybe '\n', so we need skip it.
				if (b == '\r' && (byte) stream.read() == '\n') {
					return stream.getPos();
				} else {
					return pos;
				}
			} else if (usesEscapeChar && b == escapeBytes[0]) {
				boolean equal = true;
				for (int i = 1; i < escapeBytes.length; i++) {
					if ((byte) stream.read() != escapeBytes[i]) {
						equal = false;
						break;
					}
				}
				if (equal) {
					// equal, we should skip next one
					stream.skip(1);
				}
			}
		}
		return stream.getPos();
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return end;
	}

	@Override
	public Row nextRecord(Row record) throws IOException {
		Row returnRecord = null;
		do {
			try {
				JsonNode root = iterator.nextValue();
				returnRecord = (Row) runtimeConverter.convert(root);
			} catch (NoSuchElementException e) {
				end = true;
			} catch (Throwable t) {
				if (!ignoreParseErrors) {
					throw new IOException("Failed to deserialize CSV row.", t);
				}
			}
		} while (returnRecord == null && !reachedEnd());

		return returnRecord;
	}

	/**
	 * Create a builder.
	 */
	public static Builder builder(TypeInformation<Row> typeInfo, Path... filePaths) {
		return new Builder(typeInfo, filePaths);
	}

	/**
	 * A builder for creating a {@link RowCsvInputFormat}.
	 */
	public static class Builder implements Serializable {

		private final Path[] filePaths;
		private final TypeInformation[] fieldTypes;
		private CsvSchema csvSchema;
		private boolean ignoreParseErrors;
		private int[] selectedFields;

		/**
		 * Creates a row CSV input format for the given {@link TypeInformation} and file paths
		 * with optional parameters.
		 */
		private Builder(TypeInformation<Row> typeInfo, Path... filePaths) {
			checkNotNull(filePaths, "File paths must not be null.");
			checkNotNull(typeInfo, "Type information must not be null.");

			if (!(typeInfo instanceof RowTypeInfo)) {
				throw new IllegalArgumentException("Row type information expected.");
			}

			this.filePaths = filePaths;
			this.fieldTypes = ((RowTypeInfo) typeInfo).getFieldTypes();
			this.csvSchema = CsvRowSchemaConverter.convert((RowTypeInfo) typeInfo);
		}

		public Builder setFieldDelimiter(char delimiter) {
			this.csvSchema = this.csvSchema.rebuild().setColumnSeparator(delimiter).build();
			return this;
		}

		public Builder setAllowComments(boolean allowComments) {
			this.csvSchema = this.csvSchema.rebuild().setAllowComments(allowComments).build();
			return this;
		}

		public Builder setArrayElementDelimiter(String delimiter) {
			checkNotNull(delimiter, "Array element delimiter must not be null.");
			this.csvSchema = this.csvSchema.rebuild().setArrayElementSeparator(delimiter).build();
			return this;
		}

		public Builder setQuoteCharacter(char c) {
			this.csvSchema = this.csvSchema.rebuild().setQuoteChar(c).build();
			return this;
		}

		public Builder setEscapeCharacter(char c) {
			this.csvSchema = this.csvSchema.rebuild().setEscapeChar(c).build();
			return this;
		}

		public Builder setNullLiteral(String nullLiteral) {
			checkNotNull(nullLiteral, "Null literal must not be null.");
			this.csvSchema = this.csvSchema.rebuild().setNullValue(nullLiteral).build();
			return this;
		}

		public Builder setIgnoreParseErrors(boolean ignoreParseErrors) {
			this.ignoreParseErrors = ignoreParseErrors;
			return this;
		}

		public Builder setSelectedFields(int[] selectedFields) {
			this.selectedFields = selectedFields;
			return this;
		}

		public RowCsvInputFormat build() {
			if (selectedFields == null) {
				selectedFields = new int[fieldTypes.length];
				for (int i = 0; i < fieldTypes.length; i++) {
					selectedFields[i] = i;
				}
			}

			return new RowCsvInputFormat(
					filePaths,
					fieldTypes,
					csvSchema,
					selectedFields,
					ignoreParseErrors);
		}
	}
}
