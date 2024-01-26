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

package org.apache.flink.configuration;

import org.apache.flink.util.ExceptionUtils;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.snakeyaml.engine.v2.exceptions.YamlEngineException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A test for YamlParserUtils. */
class YamlParserUtilsTest {

    @TempDir private File tmpDir;

    @Test
    void testLoadYamlFile() throws Exception {
        File confFile = new File(tmpDir, "test.yaml");
        try (final PrintWriter pw = new PrintWriter(confFile)) {
            pw.println("key1: value1");
            pw.println("key2: ");
            pw.println("  subKey1: value2");
            pw.println("key3: [a, b, c]");
            pw.println("key4: {k1: v1, k2: v2, k3: v3}");
            pw.println("key5: '*'");
            pw.println("key6: true");
            pw.println("key7: 'true'");
            pw.println(
                    "key8: Yes"); // This value will be interpreted as True in YAML 1.1, whereas in
            // YAML 1.2, it is treated as the string "Yes".
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        Map<String, Object> yamlData = YamlParserUtils.loadYamlFile(confFile);
        assertThat(yamlData).isNotNull();
        assertThat(yamlData.get("key1")).isEqualTo("value1");
        assertThat(((Map<?, ?>) yamlData.get("key2")).get("subKey1")).isEqualTo("value2");
        assertThat(yamlData.get("key3")).isEqualTo(Arrays.asList("a", "b", "c"));

        Map<String, String> map = new HashMap<>();
        map.put("k1", "v1");
        map.put("k2", "v2");
        map.put("k3", "v3");
        assertThat(yamlData.get("key4")).isEqualTo(map);
        assertThat(yamlData.get("key5")).isEqualTo("*");
        assertThat((Boolean) yamlData.get("key6")).isTrue();
        assertThat(yamlData.get("key7")).isEqualTo("true");
        assertThat(yamlData.get("key8")).isEqualTo("Yes");
    }

    @Test
    void testLoadEmptyYamlFile() throws Exception {
        File confFile = new File(tmpDir, "test.yaml");
        confFile.createNewFile();

        assertThat(YamlParserUtils.loadYamlFile(confFile)).isEmpty();
    }

    @Test
    void testLoadYamlFile_InvalidYAMLSyntaxException() {
        File confFile = new File(tmpDir, "invalid.yaml");
        try (final PrintWriter pw = new PrintWriter(confFile)) {
            pw.println("key: value: secret");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        assertThatThrownBy(() -> YamlParserUtils.loadYamlFile(confFile))
                .isInstanceOf(YamlEngineException.class)
                .satisfies(
                        e ->
                                Assertions.assertThat(ExceptionUtils.stringifyException(e))
                                        .doesNotContain("secret"));
    }

    @Test
    void testLoadYamlFile_DuplicateKeyException() {
        File confFile = new File(tmpDir, "invalid.yaml");
        try (final PrintWriter pw = new PrintWriter(confFile)) {
            pw.println("key: secret1");
            pw.println("key: secret2");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        assertThatThrownBy(() -> YamlParserUtils.loadYamlFile(confFile))
                .isInstanceOf(YamlEngineException.class)
                .satisfies(
                        e ->
                                Assertions.assertThat(ExceptionUtils.stringifyException(e))
                                        .doesNotContain("secret1", "secret2"));
    }

    @Test
    void testToYAMLString() {
        assertThat(YamlParserUtils.toYAMLString(TestEnum.ENUM)).isEqualTo(TestEnum.ENUM.toString());

        Object o1 = 123;
        assertThat(YamlParserUtils.toYAMLString(o1)).isEqualTo(String.valueOf(o1));

        Object o2 = true;
        assertThat(YamlParserUtils.toYAMLString(o2)).isEqualTo(String.valueOf(o2));

        // the following value should be escaped
        Object o3 = Arrays.asList("*", "123", "true");
        assertThat(YamlParserUtils.toYAMLString(o3)).isEqualTo("['*', '123', 'true']");
    }

    @Test
    void testConvertToObject() {
        String s1 = "test";
        assertThat(YamlParserUtils.convertToObject(s1, String.class)).isEqualTo(s1);

        String s2 = "true";
        assertThat(YamlParserUtils.convertToObject(s2, Boolean.class)).isTrue();

        String s3 = "[a, b, c]";
        assertThat(YamlParserUtils.convertToObject(s3, List.class))
                .isEqualTo(Arrays.asList("a", "b", "c"));

        String s4 = "{k1: v1, k2: v2}";
        Map<String, String> map = new HashMap<>();
        map.put("k1", "v1");
        map.put("k2", "v2");

        assertThat(YamlParserUtils.convertToObject(s4, Map.class)).isEqualTo(map);
    }

    @Test
    void testDumpNestedYamlFromFlatMap() {
        Map<String, Object> flattenMap = new HashMap<>();
        flattenMap.put("string", "stringValue");
        flattenMap.put("integer", 42);
        flattenMap.put("double", 3.14);
        flattenMap.put("boolean", true);
        flattenMap.put("enum", TestEnum.ENUM);
        flattenMap.put("list1", Arrays.asList("item1", "item2", "item3"));
        flattenMap.put("list2", "{item1, item2, item3}");
        flattenMap.put("map1", Collections.singletonMap("k1", "v1"));
        flattenMap.put("map2", "{k2: v2}");
        flattenMap.put(
                "listMap1",
                Arrays.asList(
                        Collections.singletonMap("k3", "v3"),
                        Collections.singletonMap("k4", "v4")));
        flattenMap.put("listMap2", "[{k5: v5}, {k6: v6}]");
        flattenMap.put("nested.key1.subKey1", "value1");
        flattenMap.put("nested.key2.subKey1", "value2");
        flattenMap.put("nested.key3", "value3");
        flattenMap.put("escaped1", "*");
        flattenMap.put("escaped2", "1");
        flattenMap.put("escaped3", "true");

        List<String> values = YamlParserUtils.convertAndDumpYamlFromFlatMap(flattenMap);

        assertThat(values)
                .containsExactlyInAnyOrder(
                        "string: stringValue",
                        "integer: 42",
                        "double: 3.14",
                        "boolean: true",
                        "enum: ENUM",
                        "list1:",
                        "- item1",
                        "- item2",
                        "- item3",
                        "list2: '{item1, item2, item3}'",
                        "map1:",
                        "  k1: v1",
                        "map2: '{k2: v2}'",
                        "listMap1:",
                        "- k3: v3",
                        "- k4: v4",
                        "listMap2: '[{k5: v5}, {k6: v6}]'",
                        "nested:",
                        "  key1:",
                        "    subKey1: value1",
                        "  key2:",
                        "    subKey1: value2",
                        "  key3: value3",
                        "escaped1: '*'",
                        "escaped2: '1'",
                        "escaped3: 'true'");
    }

    private enum TestEnum {
        ENUM
    }
}
