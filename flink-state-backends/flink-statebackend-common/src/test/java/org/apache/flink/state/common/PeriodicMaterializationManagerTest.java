/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.common;

import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.state.common.PeriodicMaterializationManager.MaterializationTarget;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.flink.shaded.guava30.com.google.common.collect.Iterators.getOnlyElement;
import static org.apache.flink.util.concurrent.Executors.newDirectExecutorService;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/** {@link PeriodicMaterializationManager} test. */
public class PeriodicMaterializationManagerTest extends TestLogger {

    @Test
    public void testInitialDelay() {
        ManuallyTriggeredScheduledExecutorService scheduledExecutorService =
                new ManuallyTriggeredScheduledExecutorService();
        long periodicMaterializeDelay = 10_000L;

        try (PeriodicMaterializationManager test =
                new PeriodicMaterializationManager(
                        new SyncMailboxExecutor(),
                        newDirectExecutorService(),
                        "test",
                        (message, exception) -> {},
                        MaterializationTarget.NO_OP,
                        new ChangelogMaterializationMetricGroup(
                                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup()),
                        periodicMaterializeDelay,
                        0,
                        "subtask-0",
                        scheduledExecutorService)) {
            test.start();

            assertThat(
                    String.format(
                            "task for initial materialization should be scheduled with a 0..%d delay",
                            periodicMaterializeDelay),
                    getOnlyElement(scheduledExecutorService.getAllScheduledTasks().iterator())
                            .getDelay(MILLISECONDS),
                    lessThanOrEqualTo(periodicMaterializeDelay));
        }
    }
}
