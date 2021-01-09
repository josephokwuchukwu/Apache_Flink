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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.util.TestLogger;
import org.apache.flink.util.clock.ManualClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for {@link FailureRateRestartBackoffTimeStrategy}. */
public class FailureRateRestartBackoffTimeStrategyTest extends TestLogger {

    private final Exception failure = new Exception();

    @Test
    public void testManyFailuresWithinRate() {
        final int numFailures = 3;
        final long intervalMS = 1L;

        ManualClock clock = new ManualClock();

        final FailureRateRestartBackoffTimeStrategy restartStrategy =
                new FailureRateRestartBackoffTimeStrategy(clock, 1, intervalMS, 0);

        for (int failuresLeft = numFailures; failuresLeft > 0; failuresLeft--) {
            restartStrategy.notifyFailure(failure);
            assertTrue(restartStrategy.canRestart());
            clock.advanceTime(intervalMS + 1, TimeUnit.MILLISECONDS);
        }

        assertTrue(restartStrategy.canRestart());
    }

    @Test
    public void testFailuresExceedingRate() {
        final int numFailures = 3;
        final long intervalMS = 10_000L;

        final FailureRateRestartBackoffTimeStrategy restartStrategy =
                new FailureRateRestartBackoffTimeStrategy(
                        new ManualClock(), numFailures, intervalMS, 0);

        for (int failuresLeft = numFailures; failuresLeft > 0; failuresLeft--) {
            restartStrategy.notifyFailure(failure);
            assertTrue(restartStrategy.canRestart());
        }

        restartStrategy.notifyFailure(failure);
        assertFalse(restartStrategy.canRestart());
    }

    @Test
    public void testBackoffTime() {
        final long backoffTimeMS = 10_000L;

        final FailureRateRestartBackoffTimeStrategy restartStrategy =
                new FailureRateRestartBackoffTimeStrategy(new ManualClock(), 1, 1, backoffTimeMS);

        assertEquals(backoffTimeMS, restartStrategy.getBackoffTime());
    }
}
