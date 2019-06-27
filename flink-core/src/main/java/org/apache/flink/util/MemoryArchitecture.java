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

package org.apache.flink.util;

import java.util.Arrays;
import java.util.List;

/**
 * The memory architecture (32 bit / 64 bit) of the current process.
 * Note that this might be different than the actual operating system's architecture, for example
 * when installing a 32 bit JRE in a 64 bit OS.
 */
public enum MemoryArchitecture {

	/**
	 * 32 bit memory address size.
	 */
	SIZE_32,

	/**
	 * 64 bit memory address size.
	 */
	SIZE_64,

	/**
	 * Unknown architecture, could not be determined.
	 */
	UNKNOWN;

	// ------------------------------------------------------------------------

	private static final MemoryArchitecture current = getInternal();

	/**
	 * Gets the processor architecture of this process.
	 */
	public static MemoryArchitecture get() {
		return current;
	}

	private static MemoryArchitecture getInternal() {
		// putting these into the method to avoid having objects on the heap that are not needed
		// any more after initialization
		final List<String> names64bit = Arrays.asList("amd64", "x86_64");
		final List<String> names32bit = Arrays.asList("x86", "i386", "i486", "i586", "i686");
		final String arch = System.getProperty("os.arch");

		if (names64bit.contains(arch)) {
			return SIZE_64;
		}
		else if (names32bit.contains(arch)) {
			return SIZE_32;
		}
		else {
			return UNKNOWN;
		}
	}
}
