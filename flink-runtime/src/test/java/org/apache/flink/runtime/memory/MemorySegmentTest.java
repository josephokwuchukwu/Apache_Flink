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


package org.apache.flink.runtime.memory;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.apache.flink.core.memory.DirectMemorySegment;
import org.apache.flink.core.memory.HeapMemorySegment;
import org.apache.flink.runtime.memorymanager.DirectMemoryManager;
import org.apache.flink.runtime.memorymanager.HeapMemoryManager;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.junit.Assert;
import org.apache.flink.core.memory.MemorySegment;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class MemorySegmentTest {

	public static final long RANDOM_SEED = 643196033469871L;

	public static final int MANAGED_MEMORY_SIZE = 1024 * 1024 * 16;

	public static final int PAGE_SIZE = 1024 * 512;

	private MemoryManager manager;

	private MemorySegment segment;

	private Random random;

	public MemorySegmentTest(MemoryManager manager) {
		this.manager = manager;
	}

	@Parameterized.Parameters
	/* Instantiates all of the tests in this class with the HeapMemoryManager and the DirectMemoryManager */
	public static Collection<Object[]> generateParameters() {
		try {
			HeapMemoryManager heapMemoryManager = new HeapMemoryManager(MANAGED_MEMORY_SIZE, 1, PAGE_SIZE);
			DirectMemoryManager directMemoryManager = new DirectMemoryManager(MANAGED_MEMORY_SIZE, 1, PAGE_SIZE);

			return Arrays.asList(new Object[][] {
					{heapMemoryManager}, {directMemoryManager}
			});
		} catch (Exception e) {
			e.printStackTrace();
			fail("Test setup failed.");
		}
		return null;
	}

	@Before
	public void setUp() {
		try {
			this.segment = this.manager.allocatePages(new MemoryManagerTest.DummyInvokable(), 1).get(0);
			this.random = new Random(RANDOM_SEED);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Test setup failed.");
		}
	}

	@After
	public void tearDown() {
		this.manager.release(this.segment);
		this.random = null;
		this.segment = null;

		if (!this.manager.verifyEmpty()) {
			Assert.fail("Not all memory has been properly released.");
		}
		this.manager = null;
	}

	@Test
	public void bulkByteAccess() {

		// test exceptions
		{
			byte[] bytes = new byte[PAGE_SIZE / 4];

			try {
				segment.put(3 * (PAGE_SIZE / 4) + 1, bytes);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.put(7 * (PAGE_SIZE / 8) + 1, bytes, 0, bytes.length / 2);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}
		}

		// test expected correct behavior with default offset / length
		{
			long seed = random.nextLong();

			random.setSeed(seed);
			byte[] src = new byte[PAGE_SIZE / 8];
			for (int i = 0; i < 8; i++) {
				random.nextBytes(src);
				segment.put(i * (PAGE_SIZE / 8), src);
			}

			random.setSeed(seed);
			byte[] expected = new byte[PAGE_SIZE / 8];
			byte[] actual = new byte[PAGE_SIZE / 8];
			for (int i = 0; i < 8; i++) {
				random.nextBytes(expected);
				segment.get(i * (PAGE_SIZE / 8), actual);

				assertArrayEquals(expected, actual);
			}
		}

		// test expected correct behavior with specific offset / length
		{
			byte[] expected = new byte[PAGE_SIZE];
			random.nextBytes(expected);

			for (int i = 0; i < 16; i++) {
				segment.put(i * (PAGE_SIZE / 16), expected, i * (PAGE_SIZE / 16),
					PAGE_SIZE / 16);
			}

			byte[] actual = new byte[PAGE_SIZE];
			for (int i = 0; i < 16; i++) {
				segment.get(i * (PAGE_SIZE / 16), actual, i * (PAGE_SIZE / 16),
					PAGE_SIZE / 16);
			}

			assertArrayEquals(expected, actual);
		}
	}

	@Test
	public void byteAccess() {
		// test exceptions
		{
			try {
				segment.put(-1, (byte) 0);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.put(PAGE_SIZE, (byte) 0);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.get(-1);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.get(PAGE_SIZE);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}
		}

		// test expected correct behavior
		{
			long seed = random.nextLong();

			random.setSeed(seed);
			for (int i = 0; i < PAGE_SIZE; i++) {
				segment.put(i, (byte) random.nextInt());
			}

			random.setSeed(seed);
			for (int i = 0; i < PAGE_SIZE; i++) {
				assertEquals((byte) random.nextInt(), segment.get(i));
			}
		}
	}

	@Test
	public void booleanAccess() {
		// test exceptions
		{
			try {
				segment.putBoolean(-1, false);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.putBoolean(PAGE_SIZE, false);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.getBoolean(-1);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.getBoolean(PAGE_SIZE);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}
		}

		// test expected correct behavior
		{
			long seed = random.nextLong();

			random.setSeed(seed);
			for (int i = 0; i < PAGE_SIZE; i++) {
				segment.putBoolean(i, random.nextBoolean());
			}

			random.setSeed(seed);
			for (int i = 0; i < PAGE_SIZE; i++) {
				assertEquals(random.nextBoolean(), segment.getBoolean(i));
			}
		}
	}

	@Test
	public void charAccess() {
		// test exceptions
		{
			try {
				segment.putChar(-1, 'a');
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.putChar(PAGE_SIZE, 'a');
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.getChar(-1);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.getChar(PAGE_SIZE);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}
		}

		// test expected correct behavior
		{
			long seed = random.nextLong();

			random.setSeed(seed);
			for (int i = 0; i <= PAGE_SIZE - 2; i += 2) {
				segment.putChar(i, (char) ('a' + random.nextInt(26)));
			}

			random.setSeed(seed);
			for (int i = 0; i <= PAGE_SIZE - 2; i += 2) {
				assertEquals((char) ('a' + random.nextInt(26)), segment.getChar(i));
			}
		}
	}

	@Test
	public void doubleAccess() {
		// test exceptions
		{
			try {
				segment.putDouble(-1, 0.0);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.putDouble(PAGE_SIZE, 0.0);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.getDouble(-1);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.getDouble(PAGE_SIZE);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}
		}

		// test expected correct behavior
		{
			long seed = random.nextLong();

			random.setSeed(seed);
			for (int i = 0; i <= PAGE_SIZE - 8; i += 8) {
				segment.putDouble(i, random.nextDouble());
			}

			random.setSeed(seed);
			for (int i = 0; i <= PAGE_SIZE - 8; i += 8) {
				assertEquals(random.nextDouble(), segment.getDouble(i), 0.0);
			}
		}
	}

	// @Test
	public void floatAccess() {
		// test exceptions
		{
			try {
				segment.putFloat(-1, 0.0f);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.putFloat(PAGE_SIZE, 0.0f);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.getFloat(-1);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.getFloat(PAGE_SIZE);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}
		}

		// test expected correct behavior
		{
			long seed = random.nextLong();

			random.setSeed(seed);
			for (int i = 0; i <= PAGE_SIZE - 4; i += 4) {
				segment.putFloat(i, random.nextFloat());
			}

			random.setSeed(seed);
			for (int i = 0; i <= PAGE_SIZE - 4; i += 4) {
				assertEquals(random.nextFloat(), segment.getFloat(i), 0.0);
			}
		}
	}

	@Test
	public void longAccess() {
		// test exceptions
		{
			try {
				segment.putLong(-1, 0L);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.putLong(PAGE_SIZE, 0L);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.getLong(-1);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.getLong(PAGE_SIZE);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}
		}

		// test expected correct behavior
		{
			long seed = random.nextLong();

			random.setSeed(seed);
			for (int i = 0; i <= PAGE_SIZE - 8; i += 8) {
				segment.putLong(i, random.nextLong());
			}

			random.setSeed(seed);
			for (int i = 0; i <= PAGE_SIZE - 8; i += 8) {
				assertEquals(random.nextLong(), segment.getLong(i));
			}
		}
		
		// test unaligned offsets
		{
			final long seed = random.nextLong();

			random.setSeed(seed);
			for (int offset = 0; offset < PAGE_SIZE - 8; offset += random.nextInt(24) + 8) {
				long value = random.nextLong();
				segment.putLong(offset, value);
			}
			
			random.setSeed(seed);
			for (int offset = 0; offset < PAGE_SIZE - 8; offset += random.nextInt(24) + 8) {
				long shouldValue = random.nextLong();
				long isValue = segment.getLong(offset);
				assertEquals(shouldValue, isValue);
			}
		}
	}

	@Test
	public void intAccess() {
		// test exceptions
		{
			try {
				segment.putInt(-1, 0);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.putInt(PAGE_SIZE, 0);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.getInt(-1);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.getInt(PAGE_SIZE);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}
		}

		// test expected correct behavior
		{
			long seed = random.nextLong();

			random.setSeed(seed);
			for (int i = 0; i <= PAGE_SIZE - 4; i += 4) {
				segment.putInt(i, random.nextInt());
			}

			random.setSeed(seed);
			for (int i = 0; i <= PAGE_SIZE - 4; i += 4) {
				assertEquals(random.nextInt(), segment.getInt(i));
			}
		}
	}

	@Test
	public void shortAccess() {
		// test exceptions
		{
			try {
				segment.putShort(-1, (short) 0);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.putShort(PAGE_SIZE, (short) 0);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.getShort(-1);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.getShort(PAGE_SIZE);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}
		}

		// test expected correct behavior
		{
			long seed = random.nextLong();

			random.setSeed(seed);
			for (int i = 0; i <= PAGE_SIZE - 2; i += 2) {
				segment.putShort(i, (short) random.nextInt());
			}

			random.setSeed(seed);
			for (int i = 0; i <= PAGE_SIZE - 2; i += 2) {
				assertEquals((short) random.nextInt(), segment.getShort(i));
			}
		}
	}
	
	@Test
	public void testByteBufferWrapping() {
		int numBytes = 1024;
		MemorySegment seg = null;
		try {
			if (segment instanceof HeapMemorySegment) {
				seg = new HeapMemorySegment(new byte[numBytes]);
			} else if (segment instanceof DirectMemorySegment) {
				seg = new DirectMemorySegment(numBytes);
			} else {
				fail("Unknown MemorySegment implementation was loaded!");
			}

			ByteBuffer buf1 = seg.wrap(13, 47);
			assertEquals(13, buf1.position());
			assertEquals(60, buf1.limit());
			assertEquals(47, buf1.remaining());

			ByteBuffer buf2 = seg.wrap(500, 267);
			assertEquals(500, buf2.position());
			assertEquals(767, buf2.limit());
			assertEquals(267, buf2.remaining());

			ByteBuffer buf3 = seg.wrap(0, 1024);
			assertEquals(0, buf3.position());
			assertEquals(numBytes, buf3.limit());
			assertEquals(numBytes, buf3.remaining());

		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}


	@Test
	public void testBulkOperations() {

		int numBytes = 1024;
		MemorySegment seg = null;
		if (segment instanceof HeapMemorySegment) {
			seg = new HeapMemorySegment(new byte[numBytes]);
		} else if (segment instanceof DirectMemorySegment) {
			seg = new DirectMemorySegment(numBytes);
		} else {
			fail("Unknown MemorySegment implementation was loaded!");
		}

		byte[] dataExpected = new byte[numBytes];
		long seed = random.nextLong();
		random.setSeed(seed);
		random.nextBytes(dataExpected);

		try {
			RandomAccessFile file = new RandomAccessFile("/tmp/mem_segment_test.file", "rw");
			// write expected data to file
			file.write(dataExpected);
			file.seek(0);
			// test bulk put method
			// load file contents into memory segment
			seg.put(file, 0, numBytes);
			file.close();

			RandomAccessFile file2 = new RandomAccessFile("/tmp/mem_segment_test2.file", "rw");
			// test bulk get method
			// write second file with data from memory segment
			seg.get(file2, 0, numBytes);
			file2.seek(0);

			// read file previously written by memory segment
			byte[] data = new byte[numBytes];
			file2.read(data);

			Assert.assertArrayEquals(dataExpected, data);

		} catch (Exception e) {
			e.printStackTrace();
			fail("Could not create/read/write random access file.");
		}
	}

	@Test
	public void testCompare() {
		int numBytes = segment.size();
		MemorySegment seg2 = null;
		if (segment instanceof HeapMemorySegment) {
			seg2 = new HeapMemorySegment(new byte[numBytes]);
		} else if (segment instanceof DirectMemorySegment) {
			seg2 = new DirectMemorySegment(numBytes);
		} else {
			fail("Unknown MemorySegment implementation was loaded!");
		}
		long seed = random.nextLong();
		random.setSeed(seed);

		int cmp = 0;
		byte[] expected = new byte[numBytes];
		random.nextBytes(expected);
		// compare bytes without offset (equal)
		segment.put(0, expected);
		seg2.put(0, expected);
		cmp = segment.compare(seg2, 0, 0, numBytes);
		Assert.assertEquals(0, cmp);
		// compare byte with offset
		segment.put(6, expected, 6, numBytes - 6);
		seg2.put(6, expected, 6, numBytes - 6);
		cmp = segment.compare(seg2, 6, 6, numBytes - 6);
		Assert.assertEquals(0, cmp);

		// not equal check
		segment.put(0, (byte) 1);
		seg2.put(0, (byte) 2);
		cmp = segment.compare(seg2, 0, 0, numBytes);
		Assert.assertEquals(cmp < 0, true);
		// not equal check
		segment.put(0, (byte) 2);
		seg2.put(0, (byte) 1);
		cmp = segment.compare(seg2, 0, 0, numBytes);
		Assert.assertEquals(cmp > 0, true);

		// restore equality
		segment.put(0, (byte) 0);
		seg2.put(0, (byte) 0);

		// not equal check with offset
		segment.put(6, (byte) 1);
		seg2.put(6, (byte) 2);
		cmp = segment.compare(seg2, 0, 0, numBytes);
		Assert.assertEquals(cmp < 0, true);
		// not equal check with offset
		segment.put(6, (byte) 2);
		seg2.put(6, (byte) 1);
		cmp = segment.compare(seg2, 0, 0, numBytes);
		Assert.assertEquals(cmp > 0, true);

	}

	@Test
	public void testSwapBytes() {
		int numBytes = segment.size();
		MemorySegment seg2 = null;
		if (segment instanceof HeapMemorySegment) {
			seg2 = new HeapMemorySegment(new byte[numBytes]);
		} else if (segment instanceof DirectMemorySegment) {
			seg2 = new DirectMemorySegment(numBytes);
		} else {
			fail("Unknown MemorySegment implementation was loaded!");
		}
		long seed = random.nextLong();
		random.setSeed(seed);

		int cmp = 0;
		byte[] b1 = new byte[numBytes];
		byte[] b2 = new byte[numBytes];
		random.nextBytes(b1);
		random.nextBytes(b2);

		/* swap without offset */
		segment.put(0, b1);
		seg2.put(0, b2);
		// swap bytes
		segment.swapBytes(seg2, 0, 0, numBytes);

		byte[] result = new byte[numBytes];
		segment.get(0, result);
		Assert.assertArrayEquals(result, b2);

		result = new byte[numBytes];
		seg2.get(0, result);
		Assert.assertArrayEquals(result, b1);

		/* swap with offset */
		segment.put(0, b1);
		seg2.put(0, b2);

		// swap bytes
		segment.swapBytes(seg2, 6, 6, numBytes - 6);

		for(int i = 0; i < 6; i++) {
			byte tmp = b1[i];
			b1[i] = b2[i];
			b2[i] = tmp;
		}
		result = new byte[numBytes];
		segment.get(0, result, 0, numBytes);
		Assert.assertArrayEquals(result, b2);

		result = new byte[numBytes];
		seg2.get(0, result, 0, numBytes);
		Assert.assertArrayEquals(result, b1);

	}
}
