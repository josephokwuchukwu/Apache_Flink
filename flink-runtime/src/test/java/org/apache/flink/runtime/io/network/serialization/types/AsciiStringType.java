/**
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


package org.apache.flink.runtime.io.network.serialization.types;

import java.io.IOException;
import java.util.Random;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public class AsciiStringType implements SerializationTestType {

	private static final int MAX_LEN = 1500;

	public String value;

	public AsciiStringType() {
		this.value = "";
	}

	private AsciiStringType(String value) {
		this.value = value;
	}

	@Override
	public AsciiStringType getRandom(Random rnd) {
		final StringBuilder bld = new StringBuilder();
		final int len = rnd.nextInt(MAX_LEN + 1);

		for (int i = 0; i < len; i++) {
			// 1--127
			bld.append((char) (rnd.nextInt(126) + 1));
		}

		return new AsciiStringType(bld.toString());
	}

	@Override
	public int length() {
		// add 10 because of the fake schema header we write
		return value.getBytes().length + 2 + 10;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		// add some fake seeking to simulate writing of a schema header
		out.lock();
		long start = out.tell();
		out.seek(start + 10);
		out.writeUTF(this.value);
		long end = out.tell();
		out.seek(start);
		for (int i = 0; i < 10; i++) {
			out.writeByte(i);
		}
		out.seek(end);
		out.unlock();
	}

	@Override
	public void read(DataInputView in) throws IOException {
		for (int i = 0; i < 10; i++) {
			byte b = in.readByte();
			if (b != i) {
				throw new RuntimeException("Bytes do not match.");
			}
		}
		this.value = in.readUTF();
	}

	@Override
	public int hashCode() {
		return this.value.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof AsciiStringType) {
			AsciiStringType other = (AsciiStringType) obj;
			return this.value.equals(other.value);
		} else {
			return false;
		}
	}
}
