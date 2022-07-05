/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.client.gateway.utils.UserDefinedFunctions.GENERATED_LOWER_UDF_CLASS;
import static org.apache.flink.table.client.gateway.utils.UserDefinedFunctions.GENERATED_LOWER_UDF_CODE;
import static org.apache.flink.table.client.gateway.utils.UserDefinedFunctions.GENERATED_UPPER_UDF_CLASS;
import static org.apache.flink.table.client.gateway.utils.UserDefinedFunctions.GENERATED_UPPER_UDF_CODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for classloading and class loader utilities. */
public class ClientMutableURLClassLoaderTest {

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static File userJar;

    @BeforeClass
    public static void prepare() throws Exception {
        Map<String, String> classNameCodes = new HashMap<>();
        classNameCodes.put(GENERATED_LOWER_UDF_CLASS, GENERATED_LOWER_UDF_CODE);
        classNameCodes.put(GENERATED_UPPER_UDF_CLASS, GENERATED_UPPER_UDF_CODE);
        userJar =
                UserClassLoaderJarTestUtils.createJarFile(
                        temporaryFolder.newFolder("test-jar"),
                        "test-classloader.jar",
                        classNameCodes);
    }

    @Test
    public void testClassLoadingByAddURL() throws Exception {
        Configuration configuration = new Configuration();
        final ClientMutableURLClassLoader classLoader =
                new ClientMutableURLClassLoader(
                        configuration,
                        MutableURLClassLoader.newInstance(
                                new URL[0], getClass().getClassLoader(), configuration));

        // test class loader before add jar url to ClassLoader
        assertClassNotFoundException(GENERATED_LOWER_UDF_CLASS, false, classLoader);

        // add jar url to ClassLoader
        classLoader.addURL(userJar.toURI().toURL());

        assertTrue(classLoader.getURLs().length == 1);

        final Class<?> clazz1 = Class.forName(GENERATED_LOWER_UDF_CLASS, false, classLoader);
        final Class<?> clazz2 = Class.forName(GENERATED_LOWER_UDF_CLASS, false, classLoader);

        assertEquals(clazz1, clazz2);

        classLoader.close();
    }

    @Test
    public void testClassLoadingByRemoveURL() throws Exception {
        URL jarURL = userJar.toURI().toURL();
        Configuration configuration = new Configuration();
        final ClientMutableURLClassLoader classLoader =
                new ClientMutableURLClassLoader(
                        configuration,
                        MutableURLClassLoader.newInstance(
                                new URL[] {jarURL}, getClass().getClassLoader(), configuration));

        final Class<?> clazz1 = Class.forName(GENERATED_LOWER_UDF_CLASS, false, classLoader);
        final Class<?> clazz2 = Class.forName(GENERATED_LOWER_UDF_CLASS, false, classLoader);
        assertEquals(clazz1, clazz2);

        // remove jar url
        classLoader.removeURL(jarURL);

        assertTrue(classLoader.getURLs().length == 0);

        // test class loader after remove jar url from ClassLoader
        assertClassNotFoundException(GENERATED_UPPER_UDF_CLASS, false, classLoader);

        // add jar url to ClassLoader again
        classLoader.addURL(jarURL);

        assertTrue(classLoader.getURLs().length == 1);

        final Class<?> clazz3 = Class.forName(GENERATED_UPPER_UDF_CLASS, false, classLoader);
        final Class<?> clazz4 = Class.forName(GENERATED_UPPER_UDF_CLASS, false, classLoader);
        assertEquals(clazz3, clazz4);

        classLoader.close();
    }

    @Test
    public void testParallelCapable() {
        // It will be true only if all the super classes (except class Object) of the caller are
        // registered as parallel capable.
        assertTrue(TestClientMutableURLClassLoader.isParallelCapable);
    }

    private void assertClassNotFoundException(
            String className, boolean initialize, ClassLoader classLoader) {
        CommonTestUtils.assertThrows(
                className,
                ClassNotFoundException.class,
                () -> Class.forName(className, initialize, classLoader));
    }

    private static class TestClientMutableURLClassLoader extends ClientMutableURLClassLoader {
        public static boolean isParallelCapable;

        static {
            isParallelCapable = ClassLoader.registerAsParallelCapable();
        }

        TestClientMutableURLClassLoader() {
            super(null, null);
        }
    }
}
