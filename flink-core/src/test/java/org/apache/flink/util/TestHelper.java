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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

/**
 * Convenience functions to construct random objects for unit tests.
 */
public class TestHelper
{

	public static int INT_MIN = 0;
	public static int INT_MAX = 1000;
	public static int STRING_LENGTH = 10;

	/**
	 * Return a random integer between {@value INT_MIN} and {@value INT_MAX} (inclusive).
	 */
    public static int uniqueInt() {
        int result = uniqueInt(INT_MIN, INT_MAX);
        return result;
    }

	/**
	 * Return a random integer between {@code min} and {@code max} (inclusive).
	 */
    public static int uniqueInt(int min, int max) {
		int result = RandomUtils.nextInt(min, max + 1);
    	return result;
    }

	/**
	 * Return a random integer between {@value INT_MIN} and {@value INT_MAX} (inclusive) that is not listed in {@code exclude}.
	 */
    public static int uniqueInt(int[] exclude) {
    	int result = uniqueInt(INT_MIN, INT_MAX, exclude);
    	return result;
    }

	/**
	 * Return a random integer between {@code min} and {@code max} (inclusive) that is not listed in {@code exclude}.
	 */
    public static int uniqueInt(int min, int max, int[] exclude) {
        int result = uniqueInt(min, max);
        for (int e : exclude) {
        	if (result == e) {
        		result = uniqueInt(min, max, exclude);
        		break;
        	}
        }
        return result;
    }

	/**
	 * Return a random long between {@value INT_MIN} and {@value INT_MAX} (inclusive).
	 */
    public static long uniqueLong() {
        long result = uniqueLong(INT_MIN, INT_MAX);
        return result;
    }

	/**
	 * Return a random long between {@code min} and {@code max} (inclusive).
	 */
    public static long uniqueLong(long min, long max) {
        long result = RandomUtils.nextLong(min, max + 1);
        return result;
    }

    /**
     * Return a random string consisting of {@value STRING_LENGTH} characters.
     */
    public static String uniqueString() {
    	String result = uniqueString(STRING_LENGTH);
    	return result;
    }
    
    /**
     * Return a random string consisting of {code length} characters.
     */
    public static String uniqueString(int length) {
    	String result = RandomStringUtils.random(length);
    	return result;
    }
    
}