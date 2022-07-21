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

package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.runtime.functions.aggregate.FirstValueWithRetractAggFunction;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.testutils.serialization.types.DoubleType;
import org.apache.flink.testutils.serialization.types.FloatType;
import org.apache.flink.testutils.serialization.types.IntType;
import org.apache.flink.testutils.serialization.types.ShortType;

import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

/**
 * Test case for built-in FirstValue with retract aggregate function. This class tests `accumulate`
 * method without order but without argument about respect or ignore nulls, which will respect nulls
 * by default.
 */
@RunWith(Enclosed.class)
public class FirstValueRespectNullsWithRetractAggFunctionWithoutOrderTest {

    // --------------------------------------------------------------------------------------------
    // Test sets for a particular type being aggregated
    //
    // Actual tests are implemented in:
    //  - AggFunctionTestBase
    // --------------------------------------------------------------------------------------------

    /** Test for {@link TinyIntType}. */
    public static final class ByteFirstValueNotWithRetractAggFunctionWithoutOrderWithOrderTest
            extends NumberFirstValueRespectNullsWithRetractAggFunctionWithoutOrderWithOrderTestBase<
                    Byte> {

        @Override
        protected Byte getValue(String v) {
            return Byte.valueOf(v);
        }

        @Override
        protected AggregateFunction<
                        Byte,
                        FirstValueWithRetractAggFunction.FirstValueWithRetractAccumulator<Byte>>
                getAggregator() {
            return new FirstValueWithRetractAggFunction<>(DataTypes.TINYINT().getLogicalType());
        }
    }

    /** Test for {@link ShortType}. */
    public static final class ShortFirstValueNotWithRetractAggFunctionWithoutOrderWithOrderTest
            extends NumberFirstValueRespectNullsWithRetractAggFunctionWithoutOrderWithOrderTestBase<
                    Short> {

        @Override
        protected Short getValue(String v) {
            return Short.valueOf(v);
        }

        @Override
        protected AggregateFunction<
                        Short,
                        FirstValueWithRetractAggFunction.FirstValueWithRetractAccumulator<Short>>
                getAggregator() {
            return new FirstValueWithRetractAggFunction<>(DataTypes.SMALLINT().getLogicalType());
        }
    }

    /** Test for {@link IntType}. */
    public static final class IntFirstValueNotWithRetractAggFunctionWithoutOrderWithOrderTest
            extends NumberFirstValueRespectNullsWithRetractAggFunctionWithoutOrderWithOrderTestBase<
                    Integer> {

        @Override
        protected Integer getValue(String v) {
            return Integer.valueOf(v);
        }

        @Override
        protected AggregateFunction<
                        Integer,
                        FirstValueWithRetractAggFunction.FirstValueWithRetractAccumulator<Integer>>
                getAggregator() {
            return new FirstValueWithRetractAggFunction<>(DataTypes.INT().getLogicalType());
        }
    }

    /** Test for {@link BigIntType}. */
    public static final class LongFirstValueNotWithRetractAggFunctionWithoutOrderWithOrderTest
            extends NumberFirstValueRespectNullsWithRetractAggFunctionWithoutOrderWithOrderTestBase<
                    Long> {

        @Override
        protected Long getValue(String v) {
            return Long.valueOf(v);
        }

        @Override
        protected AggregateFunction<
                        Long,
                        FirstValueWithRetractAggFunction.FirstValueWithRetractAccumulator<Long>>
                getAggregator() {
            return new FirstValueWithRetractAggFunction<>(DataTypes.BIGINT().getLogicalType());
        }
    }

    /** Test for {@link FloatType}. */
    public static final class FloatFirstValueNotWithRetractAggFunctionWithoutOrderWithOrderTest
            extends NumberFirstValueRespectNullsWithRetractAggFunctionWithoutOrderWithOrderTestBase<
                    Float> {

        @Override
        protected Float getValue(String v) {
            return Float.valueOf(v);
        }

        @Override
        protected AggregateFunction<
                        Float,
                        FirstValueWithRetractAggFunction.FirstValueWithRetractAccumulator<Float>>
                getAggregator() {
            return new FirstValueWithRetractAggFunction<>(DataTypes.FLOAT().getLogicalType());
        }
    }

    /** Test for {@link DoubleType}. */
    public static final class DoubleFirstValueNotWithRetractAggFunctionWithoutOrderWithOrderTest
            extends NumberFirstValueRespectNullsWithRetractAggFunctionWithoutOrderWithOrderTestBase<
                    Double> {

        @Override
        protected Double getValue(String v) {
            return Double.valueOf(v);
        }

        @Override
        protected AggregateFunction<
                        Double,
                        FirstValueWithRetractAggFunction.FirstValueWithRetractAccumulator<Double>>
                getAggregator() {
            return new FirstValueWithRetractAggFunction<>(DataTypes.DOUBLE().getLogicalType());
        }
    }

    /** Test for {@link BooleanType}. */
    public static final class BooleanFirstValueNotWithRetractAggFunctionWithoutOrderWithOrderTest
            extends FirstValueRespectNullsWithRetractAggFunctionWithoutOrderWithOrderTestBase<
                    Boolean> {

        @Override
        protected List<List<Boolean>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(false, false, false),
                    Arrays.asList(true, true, true),
                    Arrays.asList(true, false, null, true, false, true, null),
                    Arrays.asList(null, null, null),
                    Arrays.asList(null, true));
        }

        @Override
        protected List<Boolean> getExpectedResults() {
            return Arrays.asList(false, true, true, null, null);
        }

        @Override
        protected AggregateFunction<
                        Boolean,
                        FirstValueWithRetractAggFunction.FirstValueWithRetractAccumulator<Boolean>>
                getAggregator() {
            return new FirstValueWithRetractAggFunction<>(DataTypes.BOOLEAN().getLogicalType());
        }
    }

    /** Test for {@link DecimalType}. */
    public static final class DecimalFirstValueNotWithRetractAggFunctionWithoutOrderWithOrderTest
            extends FirstValueRespectNullsWithRetractAggFunctionWithoutOrderWithOrderTestBase<
                    DecimalData> {

        private final int precision = 20;
        private final int scale = 6;

        @Override
        protected List<List<DecimalData>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(
                            DecimalDataUtils.castFrom("1", precision, scale),
                            DecimalDataUtils.castFrom("1000.000001", precision, scale),
                            DecimalDataUtils.castFrom("-1", precision, scale),
                            DecimalDataUtils.castFrom("-999.998999", precision, scale),
                            null,
                            DecimalDataUtils.castFrom("0", precision, scale),
                            DecimalDataUtils.castFrom("-999.999", precision, scale),
                            null,
                            DecimalDataUtils.castFrom("999.999", precision, scale)),
                    Arrays.asList(null, null, null, null, null),
                    Arrays.asList(null, DecimalDataUtils.castFrom("0", precision, scale)));
        }

        @Override
        protected List<DecimalData> getExpectedResults() {
            return Arrays.asList(DecimalDataUtils.castFrom("1", precision, scale), null, null);
        }

        @Override
        protected AggregateFunction<
                        DecimalData,
                        FirstValueWithRetractAggFunction.FirstValueWithRetractAccumulator<
                                DecimalData>>
                getAggregator() {
            return new FirstValueWithRetractAggFunction<>(
                    DataTypes.DECIMAL(precision, scale).getLogicalType());
        }
    }

    /** Test for {@link VarCharType}. */
    public static final class StringFirstValueNotWithRetractAggFunctionWithoutOrderWithOrderTest
            extends FirstValueRespectNullsWithRetractAggFunctionWithoutOrderWithOrderTestBase<
                    StringData> {

        @Override
        protected List<List<StringData>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(
                            StringData.fromString("abc"),
                            StringData.fromString("def"),
                            StringData.fromString("ghi"),
                            null,
                            StringData.fromString("jkl"),
                            null,
                            StringData.fromString("zzz")),
                    Arrays.asList(null, null),
                    Arrays.asList(null, StringData.fromString("a")),
                    Arrays.asList(StringData.fromString("x"), null, StringData.fromString("e")));
        }

        @Override
        protected List<StringData> getExpectedResults() {
            return Arrays.asList(
                    StringData.fromString("abc"), null, null, StringData.fromString("x"));
        }

        @Override
        protected AggregateFunction<
                        StringData,
                        FirstValueWithRetractAggFunction.FirstValueWithRetractAccumulator<
                                StringData>>
                getAggregator() {
            return new FirstValueWithRetractAggFunction<>(DataTypes.STRING().getLogicalType());
        }
    }

    // --------------------------------------------------------------------------------------------
    // This section contain base classes that provide common:
    //  - inputs
    //  - accumulator class
    //  - accessor for retract function
    //  for tests declared above.
    // --------------------------------------------------------------------------------------------

    /** Test base for {@link FirstValueWithRetractAggFunction} without order. */
    public abstract static
    class FirstValueRespectNullsWithRetractAggFunctionWithoutOrderWithOrderTestBase<T>
            extends FirstLastValueRespectNullsAggFunctionWithoutOrderTestBase<
                    T, FirstValueWithRetractAggFunction.FirstValueWithRetractAccumulator<T>> {

        @Override
        protected Class<?> getAccClass() {
            return FirstValueWithRetractAggFunction.FirstValueWithRetractAccumulator.class;
        }
    }

    /** Test base for {@link FirstValueWithRetractAggFunction} with number types. */
    public abstract static
    class NumberFirstValueRespectNullsWithRetractAggFunctionWithoutOrderWithOrderTestBase<T>
            extends FirstValueRespectNullsWithRetractAggFunctionWithoutOrderWithOrderTestBase<T> {

        protected abstract T getValue(String v);

        @Override
        protected List<List<T>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(getValue("1"), null, getValue("-99"), getValue("3"), null),
                    Arrays.asList(null, null, null, null),
                    Arrays.asList(null, getValue("10"), null, getValue("3")));
        }

        @Override
        protected List<T> getExpectedResults() {
            return Arrays.asList(getValue("1"), null, null);
        }
    }
}
