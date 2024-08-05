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

package org.apache.flink.api.common.eventtime;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.clock.RelativeClock;
import org.apache.flink.util.clock.SystemClock;

import org.junit.Test;

import java.io.Serializable;
import java.time.Duration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Test for the {@link WatermarkStrategy} class. */
public class WatermarkStrategyTest {

    @Test
    public void testDefaultTimeStampAssigner() {
        WatermarkStrategy<Object> wmStrategy = WatermarkStrategy.forMonotonousTimestamps();

        // ensure that the closure can be cleaned through the WatermarkStategies
        ClosureCleaner.clean(wmStrategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

        assertThat(
                wmStrategy.createTimestampAssigner(assignerContext()),
                instanceOf(RecordTimestampAssigner.class));
    }

    @Test
    public void testLambdaTimestampAssigner() {
        WatermarkStrategy<Object> wmStrategy =
                WatermarkStrategy.forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> 42L);

        // ensure that the closure can be cleaned through the WatermarkStategies
        ClosureCleaner.clean(wmStrategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

        TimestampAssigner<Object> timestampAssigner =
                wmStrategy.createTimestampAssigner(assignerContext());

        assertThat(timestampAssigner.extractTimestamp(null, 13L), is(42L));
    }

    @Test
    public void testLambdaTimestampAssignerSupplier() {
        WatermarkStrategy<Object> wmStrategy =
                WatermarkStrategy.forMonotonousTimestamps()
                        .withTimestampAssigner(
                                TimestampAssignerSupplier.of((event, timestamp) -> 42L));
        // ensure that the closure can be cleaned through the WatermarkStategies
        ClosureCleaner.clean(wmStrategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

        TimestampAssigner<Object> timestampAssigner =
                wmStrategy.createTimestampAssigner(assignerContext());

        assertThat(timestampAssigner.extractTimestamp(null, 13L), is(42L));
    }

    @Test
    public void testAnonymousInnerTimestampAssigner() {
        WatermarkStrategy<Object> wmStrategy =
                WatermarkStrategy.forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Object>() {
                                    @Override
                                    public long extractTimestamp(
                                            Object element, long recordTimestamp) {
                                        return 42;
                                    }
                                });
        // ensure that the closure can be cleaned through the WatermarkStategies
        ClosureCleaner.clean(wmStrategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

        TimestampAssigner<Object> timestampAssigner =
                wmStrategy.createTimestampAssigner(assignerContext());

        assertThat(timestampAssigner.extractTimestamp(null, 13L), is(42L));
    }

    @Test
    public void testClassTimestampAssigner() {
        WatermarkStrategy<Object> wmStrategy =
                WatermarkStrategy.forMonotonousTimestamps()
                        .withTimestampAssigner((ctx) -> new TestTimestampAssigner());
        // ensure that the closure can be cleaned through the WatermarkStategies
        ClosureCleaner.clean(wmStrategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

        TimestampAssigner<Object> timestampAssigner =
                wmStrategy.createTimestampAssigner(assignerContext());

        assertThat(timestampAssigner.extractTimestamp(null, 13L), is(42L));
    }

    @Test
    public void testClassTimestampAssignerUsingSupplier() {
        WatermarkStrategy<Object> wmStrategy =
                WatermarkStrategy.forMonotonousTimestamps()
                        .withTimestampAssigner((context) -> new TestTimestampAssigner());
        // ensure that the closure can be cleaned through the WatermarkStategies
        ClosureCleaner.clean(wmStrategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

        TimestampAssigner<Object> timestampAssigner =
                wmStrategy.createTimestampAssigner(assignerContext());

        assertThat(timestampAssigner.extractTimestamp(null, 13L), is(42L));
    }

    @Test
    public void testWithIdlenessHelper() {
        WatermarkStrategy<String> wmStrategy =
                WatermarkStrategy.<String>forMonotonousTimestamps()
                        .withIdleness(Duration.ofDays(7));

        // ensure that the closure can be cleaned
        ClosureCleaner.clean(wmStrategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

        assertThat(
                wmStrategy.createTimestampAssigner(assignerContext()),
                instanceOf(RecordTimestampAssigner.class));
        assertThat(
                wmStrategy.createWatermarkGenerator(generatorContext()),
                instanceOf(WatermarksWithIdleness.class));
    }

    @Test
    public void testWithWatermarkAlignment() {
        final String watermarkGroup = "group-1";
        final Duration maxAllowedWatermarkDrift = Duration.ofMillis(200);
        final WatermarkStrategy<String> strategy =
                WatermarkStrategy.<String>forMonotonousTimestamps()
                        .withWatermarkAlignment(watermarkGroup, maxAllowedWatermarkDrift)
                        // we call a different builder method on top of watermark alignment
                        // to make sure it can be properly mixed
                        .withIdleness(Duration.ofMillis(200));

        // ensure that the closure can be cleaned
        ClosureCleaner.clean(strategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

        final WatermarkAlignmentParams alignmentParameters = strategy.getAlignmentParameters();
        assertThat(alignmentParameters.getWatermarkGroup(), equalTo(watermarkGroup));
        assertThat(
                alignmentParameters.getMaxAllowedWatermarkDrift(),
                equalTo(maxAllowedWatermarkDrift.toMillis()));
        assertThat(
                alignmentParameters.getUpdateInterval(),
                equalTo(WatermarksWithWatermarkAlignment.DEFAULT_UPDATE_INTERVAL.toMillis()));
        assertThat(
                strategy.createTimestampAssigner(assignerContext()),
                instanceOf(RecordTimestampAssigner.class));
        assertThat(
                strategy.createWatermarkGenerator(generatorContext()),
                instanceOf(WatermarksWithIdleness.class));
    }

    static class TestTimestampAssigner implements TimestampAssigner<Object>, Serializable {

        @Override
        public long extractTimestamp(Object element, long recordTimestamp) {
            return 42L;
        }
    }

    static TimestampAssignerSupplier.Context assignerContext() {
        return UnregisteredMetricsGroup::new;
    }

    static WatermarkGeneratorSupplier.Context generatorContext() {
        return new WatermarkGeneratorSupplier.Context() {
            @Override
            public MetricGroup getMetricGroup() {
                return new UnregisteredMetricsGroup();
            }

            @Override
            public RelativeClock getInputActivityClock() {
                return SystemClock.getInstance();
            }
        };
    }
}
