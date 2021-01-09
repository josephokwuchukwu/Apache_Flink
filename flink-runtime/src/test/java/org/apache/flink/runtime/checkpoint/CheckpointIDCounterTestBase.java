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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;

/** Test base class with common tests for the {@link CheckpointIDCounter} implementations. */
public abstract class CheckpointIDCounterTestBase extends TestLogger {

    protected abstract CheckpointIDCounter createCheckpointIdCounter() throws Exception;

    // ---------------------------------------------------------------------------------------------

    /**
     * This test guards an assumption made in the notifications in the {@link
     * org.apache.flink.runtime.operators.coordination.OperatorCoordinator}. The coordinator is
     * notified of a reset/restore and if no checkpoint yet exists (failure was before the first
     * checkpoint), a negative ID is passed.
     */
    @Test
    public void testCounterIsNeverNegative() throws Exception {
        final CheckpointIDCounter counter = createCheckpointIdCounter();

        try {
            counter.start();
            assertThat(counter.get(), greaterThanOrEqualTo(0L));
        } finally {
            counter.shutdown(JobStatus.FINISHED);
        }
    }

    /** Tests serial increment and get calls. */
    @Test
    public void testSerialIncrementAndGet() throws Exception {
        final CheckpointIDCounter counter = createCheckpointIdCounter();

        try {
            counter.start();

            assertEquals(1, counter.getAndIncrement());
            assertEquals(2, counter.get());
            assertEquals(2, counter.getAndIncrement());
            assertEquals(3, counter.get());
            assertEquals(3, counter.getAndIncrement());
            assertEquals(4, counter.get());
            assertEquals(4, counter.getAndIncrement());
        } finally {
            counter.shutdown(JobStatus.FINISHED);
        }
    }

    /**
     * Tests concurrent increment and get calls from multiple Threads and verifies that the numbers
     * counts strictly increasing.
     */
    @Test
    public void testConcurrentGetAndIncrement() throws Exception {
        // Config
        final int numThreads = 8;

        // Setup
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CheckpointIDCounter counter = createCheckpointIdCounter();
        counter.start();

        ExecutorService executor = null;
        try {
            executor = Executors.newFixedThreadPool(numThreads);

            List<Future<List<Long>>> resultFutures = new ArrayList<>(numThreads);

            for (int i = 0; i < numThreads; i++) {
                resultFutures.add(executor.submit(new Incrementer(startLatch, counter)));
            }

            // Kick off the incrementing
            startLatch.countDown();

            final int expectedTotal = numThreads * Incrementer.NumIncrements;

            List<Long> all = new ArrayList<>(expectedTotal);

            // Get the counts
            for (Future<List<Long>> result : resultFutures) {
                List<Long> counts = result.get();
                all.addAll(counts);
            }

            // Verify
            Collections.sort(all);

            assertEquals(expectedTotal, all.size());

            long current = 0;
            for (long val : all) {
                // Incrementing counts
                assertEquals(++current, val);
            }

            // The final count
            assertEquals(expectedTotal + 1, counter.get());
            assertEquals(expectedTotal + 1, counter.getAndIncrement());
        } finally {
            if (executor != null) {
                executor.shutdown();
            }

            counter.shutdown(JobStatus.FINISHED);
        }
    }

    /** Tests a simple {@link CheckpointIDCounter#setCount(long)} operation. */
    @Test
    public void testSetCount() throws Exception {
        final CheckpointIDCounter counter = createCheckpointIdCounter();
        counter.start();

        // Test setCount
        counter.setCount(1337);
        assertEquals(1337, counter.get());
        assertEquals(1337, counter.getAndIncrement());
        assertEquals(1338, counter.get());
        assertEquals(1338, counter.getAndIncrement());

        counter.shutdown(JobStatus.FINISHED);
    }

    /** Task repeatedly incrementing the {@link CheckpointIDCounter}. */
    private static class Incrementer implements Callable<List<Long>> {

        /** Total number of {@link CheckpointIDCounter#getAndIncrement()} calls. */
        private static final int NumIncrements = 128;

        private final CountDownLatch startLatch;

        private final CheckpointIDCounter counter;

        public Incrementer(CountDownLatch startLatch, CheckpointIDCounter counter) {
            this.startLatch = startLatch;
            this.counter = counter;
        }

        @Override
        public List<Long> call() throws Exception {
            final Random rand = new Random();
            final List<Long> counts = new ArrayList<>();

            // Wait for the main thread to kick off execution
            this.startLatch.await();

            for (int i = 0; i < NumIncrements; i++) {
                counts.add(counter.getAndIncrement());

                // To get some "random" interleaving ;)
                Thread.sleep(rand.nextInt(20));
            }

            return counts;
        }
    }
}
