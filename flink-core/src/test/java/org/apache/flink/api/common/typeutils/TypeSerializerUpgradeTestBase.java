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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.testutils.FlinkVersionBasedTestDataGenerationUtils;

import org.assertj.core.api.HamcrestCondition;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.core.testutils.FlinkVersionBasedTestDataGenerationUtils.mostRecentlyPublishedBaseMinorVersion;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.hamcrest.CoreMatchers.not;

/**
 * A test base for testing {@link TypeSerializer} upgrades.
 *
 * <p>See {@link FlinkVersionBasedTestDataGenerationUtils} for details on how to generate new test
 * data.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TypeSerializerUpgradeTestBase<PreviousElementT, UpgradedElementT> {

    public static final Collection<FlinkVersion> MIGRATION_VERSIONS =
            FlinkVersionBasedTestDataGenerationUtils.rangeFrom(FlinkVersion.v1_11);

    // ------------------------------------------------------------------------------
    //  APIs
    // ------------------------------------------------------------------------------

    /**
     * Creates a collection of {@link TestSpecification} which will be used as input for
     * parametrized tests.
     */
    public abstract Collection<TestSpecification<?, ?>> createTestSpecifications() throws Exception;

    /**
     * Setup code for a {@link TestSpecification}. This creates the serializer before upgrade and
     * test data, that will be written by the created pre-upgrade {@link TypeSerializer}.
     */
    public interface PreUpgradeSetup<PreviousElementT> {

        /** Creates a pre-upgrade {@link TypeSerializer}. */
        TypeSerializer<PreviousElementT> createPriorSerializer();

        /** Creates test data that will be written using the pre-upgrade {@link TypeSerializer}. */
        PreviousElementT createTestData();
    }

    /**
     * Verification code for a {@link TestSpecification}. This creates the "upgraded" {@link
     * TypeSerializer} and provides matchers for comparing the deserialized test data and for the
     * {@link TypeSerializerSchemaCompatibility}.
     */
    public interface UpgradeVerifier<UpgradedElementT> {

        /** Creates a post-upgrade {@link TypeSerializer}. */
        TypeSerializer<UpgradedElementT> createUpgradedSerializer();

        /** Returns a {@link Matcher} for asserting the deserialized test data. */
        Matcher<UpgradedElementT> testDataMatcher();

        /**
         * Returns a {@link Matcher} for comparing the {@link TypeSerializerSchemaCompatibility}
         * that the serializer upgrade produced with an expected {@link
         * TypeSerializerSchemaCompatibility}.
         */
        Matcher<TypeSerializerSchemaCompatibility<UpgradedElementT>> schemaCompatibilityMatcher(
                FlinkVersion version);
    }

    private static class ClassLoaderSafePreUpgradeSetup<PreviousElementT>
            implements PreUpgradeSetup<PreviousElementT> {

        private final PreUpgradeSetup<PreviousElementT> delegateSetup;
        private final ClassLoader setupClassloader;

        ClassLoaderSafePreUpgradeSetup(
                Class<? extends PreUpgradeSetup<PreviousElementT>> delegateSetupClass)
                throws Exception {
            checkNotNull(delegateSetupClass);
            Class<? extends PreUpgradeSetup<PreviousElementT>> relocatedDelegateSetupClass =
                    ClassRelocator.relocate(delegateSetupClass);

            this.setupClassloader = relocatedDelegateSetupClass.getClassLoader();
            try (ThreadContextClassLoader ignored =
                    new ThreadContextClassLoader(setupClassloader)) {
                this.delegateSetup = relocatedDelegateSetupClass.newInstance();
            }
        }

        @Override
        public TypeSerializer<PreviousElementT> createPriorSerializer() {
            try (ThreadContextClassLoader ignored =
                    new ThreadContextClassLoader(setupClassloader)) {
                return delegateSetup.createPriorSerializer();
            } catch (IOException e) {
                throw new RuntimeException(
                        "Error creating prior serializer via ClassLoaderSafePreUpgradeSetup.", e);
            }
        }

        @Override
        public PreviousElementT createTestData() {
            try (ThreadContextClassLoader ignored =
                    new ThreadContextClassLoader(setupClassloader)) {
                return delegateSetup.createTestData();
            } catch (IOException e) {
                throw new RuntimeException(
                        "Error creating test data via ThreadContextClassLoader.", e);
            }
        }
    }

    private static class ClassLoaderSafeUpgradeVerifier<UpgradedElementT>
            implements UpgradeVerifier<UpgradedElementT> {

        private final UpgradeVerifier<UpgradedElementT> delegateVerifier;
        private final ClassLoader verifierClassloader;

        ClassLoaderSafeUpgradeVerifier(
                Class<? extends UpgradeVerifier<UpgradedElementT>> delegateVerifierClass)
                throws Exception {
            checkNotNull(delegateVerifierClass);
            Class<? extends UpgradeVerifier<UpgradedElementT>> relocatedDelegateVerifierClass =
                    ClassRelocator.relocate(delegateVerifierClass);

            this.verifierClassloader = relocatedDelegateVerifierClass.getClassLoader();
            try (ThreadContextClassLoader ignored =
                    new ThreadContextClassLoader(verifierClassloader)) {
                this.delegateVerifier = relocatedDelegateVerifierClass.newInstance();
            }
        }

        @Override
        public TypeSerializer<UpgradedElementT> createUpgradedSerializer() {
            try (ThreadContextClassLoader ignored =
                    new ThreadContextClassLoader(verifierClassloader)) {
                return delegateVerifier.createUpgradedSerializer();
            } catch (IOException e) {
                throw new RuntimeException(
                        "Error creating upgraded serializer via ClassLoaderSafeUpgradeVerifier.",
                        e);
            }
        }

        @Override
        public Matcher<UpgradedElementT> testDataMatcher() {
            try (ThreadContextClassLoader ignored =
                    new ThreadContextClassLoader(verifierClassloader)) {
                return delegateVerifier.testDataMatcher();
            } catch (IOException e) {
                throw new RuntimeException(
                        "Error creating expected test data via ClassLoaderSafeUpgradeVerifier.", e);
            }
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<UpgradedElementT>>
                schemaCompatibilityMatcher(FlinkVersion version) {
            try (ThreadContextClassLoader ignored =
                    new ThreadContextClassLoader(verifierClassloader)) {
                return delegateVerifier.schemaCompatibilityMatcher(version);
            } catch (IOException e) {
                throw new RuntimeException(
                        "Error creating schema compatibility matcher via ClassLoaderSafeUpgradeVerifier.",
                        e);
            }
        }
    }

    /**
     * Specification of one test scenario. This mainly needs a {@link PreUpgradeSetup} and {@link
     * UpgradeVerifier}.
     */
    public static class TestSpecification<PreviousElementT, UpgradedElementT> {
        private final String name;
        private final FlinkVersion flinkVersion;
        private final ClassLoaderSafePreUpgradeSetup<PreviousElementT> setup;
        private final ClassLoaderSafeUpgradeVerifier<UpgradedElementT> verifier;

        public TestSpecification(
                String name,
                FlinkVersion flinkVersion,
                Class<? extends PreUpgradeSetup<PreviousElementT>> setupClass,
                Class<? extends UpgradeVerifier<UpgradedElementT>> verifierClass)
                throws Exception {
            this.name = checkNotNull(name);
            this.flinkVersion = checkNotNull(flinkVersion);
            this.setup = new ClassLoaderSafePreUpgradeSetup<>(setupClass);
            this.verifier = new ClassLoaderSafeUpgradeVerifier<>(verifierClass);
        }

        @Override
        public String toString() {
            return name + " / " + flinkVersion;
        }
    }

    // ------------------------------------------------------------------------------
    //  Test file generation
    // ------------------------------------------------------------------------------

    private static final int INITIAL_OUTPUT_BUFFER_SIZE = 64;

    @Test
    public void generateTestSetupFiles(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification)
            throws Exception {
        FlinkVersionBasedTestDataGenerationUtils.assumeFlinkVersionWithDescriptiveMessage(
                testSpecification.flinkVersion);
        Files.createDirectories(getSerializerSnapshotFilePath(testSpecification).getParent());

        try (ThreadContextClassLoader ignored =
                new ThreadContextClassLoader(testSpecification.setup.setupClassloader)) {
            TypeSerializer<PreviousElementT> priorSerializer =
                    testSpecification.setup.createPriorSerializer();

            // first, use the serializer to write test data
            // NOTE: it is important that we write test data first, because some serializers'
            // configuration mutates only after being used for serialization (e.g. dynamic type
            // registrations for Pojo / Kryo)
            DataOutputSerializer testDataOut = new DataOutputSerializer(INITIAL_OUTPUT_BUFFER_SIZE);
            priorSerializer.serialize(testSpecification.setup.createTestData(), testDataOut);
            writeContentsTo(
                    getGenerateDataFilePath(testSpecification), testDataOut.getCopyOfBuffer());

            // ... then write the serializer snapshot
            DataOutputSerializer serializerSnapshotOut =
                    new DataOutputSerializer(INITIAL_OUTPUT_BUFFER_SIZE);
            writeSerializerSnapshot(
                    serializerSnapshotOut,
                    priorSerializer,
                    mostRecentlyPublishedBaseMinorVersion());
            writeContentsTo(
                    getGenerateSerializerSnapshotFilePath(testSpecification),
                    serializerSnapshotOut.getCopyOfBuffer());
        }
    }

    // ------------------------------------------------------------------------------
    //  Tests
    // ------------------------------------------------------------------------------

    @ParameterizedTest(name = "Test Specification = {0}")
    @MethodSource("createTestSpecifications")
    void restoreSerializerIsValid(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification)
            throws Exception {
        try (ThreadContextClassLoader ignored =
                new ThreadContextClassLoader(testSpecification.verifier.verifierClassloader)) {
            assumeThat(TypeSerializerSchemaCompatibility.incompatible())
                    .as(
                            "This test only applies for test specifications that verify an upgraded serializer that is not incompatible.")
                    .is(
                            HamcrestCondition.matching(
                                    not(
                                            testSpecification.verifier.schemaCompatibilityMatcher(
                                                    testSpecification.flinkVersion))));

            TypeSerializerSnapshot<UpgradedElementT> restoredSerializerSnapshot =
                    snapshotUnderTest(testSpecification);

            TypeSerializer<UpgradedElementT> restoredSerializer =
                    restoredSerializerSnapshot.restoreSerializer();
            assertSerializerIsValid(
                    restoredSerializer,
                    dataUnderTest(testSpecification),
                    testSpecification.verifier.testDataMatcher());
        }
    }

    @ParameterizedTest(name = "Test Specification = {0}")
    @MethodSource("createTestSpecifications")
    void upgradedSerializerHasExpectedSchemaCompatibility(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification)
            throws Exception {
        try (ThreadContextClassLoader ignored =
                new ThreadContextClassLoader(testSpecification.verifier.verifierClassloader)) {
            TypeSerializerSnapshot<UpgradedElementT> restoredSerializerSnapshot =
                    snapshotUnderTest(testSpecification);
            TypeSerializer<UpgradedElementT> upgradedSerializer =
                    testSpecification.verifier.createUpgradedSerializer();

            TypeSerializerSchemaCompatibility<UpgradedElementT> upgradeCompatibility =
                    restoredSerializerSnapshot.resolveSchemaCompatibility(upgradedSerializer);

            assertThat(upgradeCompatibility)
                    .is(
                            HamcrestCondition.matching(
                                    testSpecification.verifier.schemaCompatibilityMatcher(
                                            testSpecification.flinkVersion)));
        }
    }

    @ParameterizedTest(name = "Test Specification = {0}")
    @MethodSource("createTestSpecifications")
    void upgradedSerializerIsValidAfterMigration(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification)
            throws Exception {
        try (ThreadContextClassLoader ignored =
                new ThreadContextClassLoader(testSpecification.verifier.verifierClassloader)) {
            TypeSerializerSnapshot<UpgradedElementT> restoredSerializerSnapshot =
                    snapshotUnderTest(testSpecification);

            TypeSerializer<UpgradedElementT> upgradedSerializer =
                    testSpecification.verifier.createUpgradedSerializer();

            TypeSerializerSchemaCompatibility<UpgradedElementT> upgradeCompatibility =
                    restoredSerializerSnapshot.resolveSchemaCompatibility(upgradedSerializer);
            assumeThat(upgradeCompatibility)
                    .as(
                            "This test only applies for test specifications that verify an upgraded serializer that requires migration to be compatible.")
                    .is(
                            HamcrestCondition.matching(
                                    TypeSerializerMatchers.isCompatibleAfterMigration()));

            // migrate the previous data schema,
            TypeSerializer<UpgradedElementT> restoreSerializer =
                    restoredSerializerSnapshot.restoreSerializer();
            DataInputView migratedData =
                    readAndThenWriteData(
                            dataUnderTest(testSpecification),
                            restoreSerializer,
                            upgradedSerializer,
                            testSpecification.verifier.testDataMatcher());

            // .. and then assert that the upgraded serializer is valid with the migrated data
            assertSerializerIsValid(
                    upgradedSerializer, migratedData, testSpecification.verifier.testDataMatcher());
        }
    }

    @ParameterizedTest(name = "Test Specification = {0}")
    @MethodSource("createTestSpecifications")
    void upgradedSerializerIsValidAfterReconfiguration(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification)
            throws Exception {
        try (ThreadContextClassLoader ignored =
                new ThreadContextClassLoader(testSpecification.verifier.verifierClassloader)) {
            TypeSerializerSnapshot<UpgradedElementT> restoredSerializerSnapshot =
                    snapshotUnderTest(testSpecification);
            TypeSerializer<UpgradedElementT> upgradedSerializer =
                    testSpecification.verifier.createUpgradedSerializer();

            TypeSerializerSchemaCompatibility<UpgradedElementT> upgradeCompatibility =
                    restoredSerializerSnapshot.resolveSchemaCompatibility(upgradedSerializer);
            assumeThat(upgradeCompatibility)
                    .as(
                            "This test only applies for test specifications that verify an upgraded serializer that requires reconfiguration to be compatible.")
                    .is(
                            HamcrestCondition.matching(
                                    TypeSerializerMatchers
                                            .isCompatibleWithReconfiguredSerializer()));

            TypeSerializer<UpgradedElementT> reconfiguredUpgradedSerializer =
                    upgradeCompatibility.getReconfiguredSerializer();
            assertSerializerIsValid(
                    reconfiguredUpgradedSerializer,
                    dataUnderTest(testSpecification),
                    testSpecification.verifier.testDataMatcher());
        }
    }

    @ParameterizedTest(name = "Test Specification = {0}")
    @MethodSource("createTestSpecifications")
    void upgradedSerializerIsValidWhenCompatibleAsIs(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification)
            throws Exception {
        try (ThreadContextClassLoader ignored =
                new ThreadContextClassLoader(testSpecification.verifier.verifierClassloader)) {
            TypeSerializerSnapshot<UpgradedElementT> restoredSerializerSnapshot =
                    snapshotUnderTest(testSpecification);
            TypeSerializer<UpgradedElementT> upgradedSerializer =
                    testSpecification.verifier.createUpgradedSerializer();

            TypeSerializerSchemaCompatibility<UpgradedElementT> upgradeCompatibility =
                    restoredSerializerSnapshot.resolveSchemaCompatibility(upgradedSerializer);
            assumeThat(upgradeCompatibility)
                    .as(
                            "This test only applies for test specifications that verify an upgraded serializer that is compatible as is.")
                    .is(HamcrestCondition.matching(TypeSerializerMatchers.isCompatibleAsIs()));

            assertSerializerIsValid(
                    upgradedSerializer,
                    dataUnderTest(testSpecification),
                    testSpecification.verifier.testDataMatcher());
        }
    }

    /**
     * Asserts that a given {@link TypeSerializer} is valid, given a {@link DataInputView} of
     * serialized data.
     *
     * <p>A serializer is valid, iff:
     *
     * <ul>
     *   <li>1. The serializer can read and then write again the given serialized data.
     *   <li>2. The serializer can produce a serializer snapshot which can be written and then read
     *       back again.
     *   <li>3. The serializer's produced snapshot is capable of creating a restore serializer.
     *   <li>4. The restore serializer created from the serializer snapshot can read and then write
     *       again data written by step 1. Given that the serializer is not a restore serializer
     *       already.
     * </ul>
     */
    private static <T> void assertSerializerIsValid(
            TypeSerializer<T> serializer, DataInputView dataInput, Matcher<T> testDataMatcher)
            throws Exception {

        DataInputView serializedData =
                readAndThenWriteData(dataInput, serializer, serializer, testDataMatcher);
        TypeSerializerSnapshot<T> snapshot = writeAndThenReadSerializerSnapshot(serializer);
        TypeSerializer<T> restoreSerializer = snapshot.restoreSerializer();
        serializedData =
                readAndThenWriteData(
                        serializedData, restoreSerializer, restoreSerializer, testDataMatcher);

        TypeSerializer<T> duplicateSerializer = snapshot.restoreSerializer().duplicate();
        readAndThenWriteData(
                serializedData, duplicateSerializer, duplicateSerializer, testDataMatcher);
    }

    // ------------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------------

    private Path getGenerateSerializerSnapshotFilePath(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification) {
        return Paths.get(getGenerateResourceDirectory(testSpecification) + "/serializer-snapshot");
    }

    private Path getGenerateDataFilePath(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification) {
        return Paths.get(getGenerateResourceDirectory(testSpecification) + "/test-data");
    }

    private String getGenerateResourceDirectory(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification) {
        return System.getProperty("user.dir")
                + "/src/test/resources/"
                + testSpecification.name
                + "-"
                + mostRecentlyPublishedBaseMinorVersion();
    }

    private Path getSerializerSnapshotFilePath(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification) {
        return Paths.get(getTestResourceDirectory(testSpecification) + "/serializer-snapshot");
    }

    private Path getTestDataFilePath(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification) {
        return Paths.get(getTestResourceDirectory(testSpecification) + "/test-data");
    }

    private String getTestResourceDirectory(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification) {
        return System.getProperty("user.dir")
                + "/src/test/resources/"
                + testSpecification.name
                + "-"
                + testSpecification.flinkVersion;
    }

    private TypeSerializerSnapshot<UpgradedElementT> snapshotUnderTest(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification)
            throws Exception {
        return readSerializerSnapshot(
                contentsOf(getSerializerSnapshotFilePath(testSpecification)),
                testSpecification.flinkVersion);
    }

    private DataInputView dataUnderTest(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification) {
        return contentsOf(getTestDataFilePath(testSpecification));
    }

    private static void writeContentsTo(Path path, byte[] bytes) {
        try {
            Files.write(path, bytes);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write to " + path, e);
        }
    }

    private static DataInputView contentsOf(Path path) {
        try {
            byte[] bytes = Files.readAllBytes(path);
            return new DataInputDeserializer(bytes);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read contents of " + path, e);
        }
    }

    private static <T> void writeSerializerSnapshot(
            DataOutputView out, TypeSerializer<T> serializer, FlinkVersion flinkVersion)
            throws IOException {

        if (flinkVersion.isNewerVersionThan(FlinkVersion.v1_6)) {
            writeSerializerSnapshotCurrentFormat(out, serializer);
        } else {
            writeSerializerSnapshotPre17Format(out, serializer);
        }
    }

    private static <T> void writeSerializerSnapshotCurrentFormat(
            DataOutputView out, TypeSerializer<T> serializer) throws IOException {

        TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
                out, serializer.snapshotConfiguration(), serializer);
    }

    @SuppressWarnings("deprecation")
    private static <T> void writeSerializerSnapshotPre17Format(
            DataOutputView out, TypeSerializer<T> serializer) throws IOException {

        TypeSerializerSerializationUtil.writeSerializersAndConfigsWithResilience(
                out,
                Collections.singletonList(
                        Tuple2.of(serializer, serializer.snapshotConfiguration())));
    }

    private static <T> TypeSerializerSnapshot<T> readSerializerSnapshot(
            DataInputView in, FlinkVersion flinkVersion) throws IOException {

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (flinkVersion.isNewerVersionThan(FlinkVersion.v1_6)) {
            return readSerializerSnapshotCurrentFormat(in, classLoader);
        } else {
            return readSerializerSnapshotPre17Format(in, classLoader);
        }
    }

    private static <T> TypeSerializerSnapshot<T> readSerializerSnapshotCurrentFormat(
            DataInputView in, ClassLoader userCodeClassLoader) throws IOException {

        return TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                in, userCodeClassLoader, null);
    }

    @SuppressWarnings({"unchecked", "deprecation"})
    private static <T> TypeSerializerSnapshot<T> readSerializerSnapshotPre17Format(
            DataInputView in, ClassLoader userCodeClassLoader) throws IOException {

        List<Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> serializerSnapshotPair =
                TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(
                        in, userCodeClassLoader);
        return (TypeSerializerSnapshot<T>) serializerSnapshotPair.get(0).f1;
    }

    private static <T> DataInputView readAndThenWriteData(
            DataInputView originalDataInput,
            TypeSerializer<T> readSerializer,
            TypeSerializer<T> writeSerializer,
            Matcher<T> testDataMatcher)
            throws IOException {

        T data = readSerializer.deserialize(originalDataInput);
        assertThat(data).is(HamcrestCondition.matching(testDataMatcher));

        DataOutputSerializer out = new DataOutputSerializer(INITIAL_OUTPUT_BUFFER_SIZE);
        writeSerializer.serialize(data, out);
        return new DataInputDeserializer(out.wrapAsByteBuffer());
    }

    private static <T> TypeSerializerSnapshot<T> writeAndThenReadSerializerSnapshot(
            TypeSerializer<T> serializer) throws IOException {

        DataOutputSerializer out = new DataOutputSerializer(INITIAL_OUTPUT_BUFFER_SIZE);
        writeSerializerSnapshotCurrentFormat(out, serializer);

        DataInputDeserializer in = new DataInputDeserializer(out.wrapAsByteBuffer());
        return readSerializerSnapshotCurrentFormat(
                in, Thread.currentThread().getContextClassLoader());
    }
}
