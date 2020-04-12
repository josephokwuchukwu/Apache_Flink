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

package org.apache.flink.formats.orc.writers;

import org.apache.flink.formats.orc.data.Record;

import com.google.protobuf25.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

class OrcTestUtil {

	private static final String USER_METADATA_KEY = "userKey";
	private static final ByteBuffer USER_METADATA_VALUE = ByteBuffer.wrap("hello".getBytes());

	static void validate(File files, List<Record> expected) throws IOException {
		final File[] buckets = files.listFiles();
		assertNotNull(buckets);
		assertEquals(1, buckets.length);

		final File[] partFiles = buckets[0].listFiles();
		assertNotNull(partFiles);

		for (File partFile : partFiles) {
			assertTrue(partFile.length() > 0);

			OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(new Configuration());
			Reader reader = OrcFile.createReader(new org.apache.hadoop.fs.Path(partFile.toURI()), readerOptions);

			assertEquals(3, reader.getNumberOfRows());
			assertEquals(2, reader.getSchema().getFieldNames().size());
			assertSame(reader.getCompressionKind(), CompressionKind.LZ4);
			assertTrue(reader.hasMetadataValue(USER_METADATA_KEY));
			assertTrue(reader.getMetadataKeys().contains(USER_METADATA_KEY));

			List<Record> results = getResults(reader);

			assertEquals(3, results.size());
			assertEquals(results, expected);
		}
	}

	static List<OrcProto.UserMetadataItem> getUserMetadataItems() {
		List<OrcProto.UserMetadataItem> userMetadata = new ArrayList<>();

		userMetadata.add(OrcProto.UserMetadataItem.newBuilder()
			.setName(USER_METADATA_KEY)
			.setValue(ByteString.copyFrom(USER_METADATA_VALUE))
			.build());

		return userMetadata;
	}

	private static List<Record> getResults(Reader reader) throws IOException {
		List<Record> results = new ArrayList<>();

		RecordReader recordReader = reader.rows();
		VectorizedRowBatch batch = reader.getSchema().createRowBatch();

		while (recordReader.nextBatch(batch)) {
			BytesColumnVector stringVector = (BytesColumnVector)  batch.cols[0];
			LongColumnVector intVector = (LongColumnVector) batch.cols[1];
			for (int r = 0; r < batch.size; r++) {
				String name = new String(stringVector.vector[r], stringVector.start[r], stringVector.length[r]);
				int age = (int) intVector.vector[r];

				results.add(new Record(name, age));
			}
			recordReader.close();
		}

		return results;
	}
}
