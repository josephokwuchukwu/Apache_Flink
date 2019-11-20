/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.clusterframework.types;

import org.apache.flink.runtime.jobmaster.slotpool.PreviousAllocationSlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.SlotSelectionStrategy;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;

/**
 * Tests for {@link PreviousAllocationSlotSelectionStrategy}.
 */
public class PreviousAllocationSlotSelectionStrategyTest extends LocationPreferenceSlotSelectionStrategyTest {

	public PreviousAllocationSlotSelectionStrategyTest() {
		super(PreviousAllocationSlotSelectionStrategy.create());
	}

	@Test
	public void matchPreviousAllocationOverridesPreferredLocation() {

		SlotProfile slotProfile = SlotProfile.priorAllocation(
			resourceProfile,
			Collections.singletonList(tml2),
			Collections.singleton(aid3),
			Collections.emptySet());
		Optional<SlotSelectionStrategy.SlotInfoAndLocality> match = runMatching(slotProfile);

		Assert.assertEquals(slotInfo3, match.get().getSlotInfo());

		slotProfile = SlotProfile.priorAllocation(
			resourceProfile,
			Arrays.asList(tmlX, tml1),
			new HashSet<>(Arrays.asList(aidX, aid2)),
			Collections.emptySet());
		match = runMatching(slotProfile);

		Assert.assertEquals(slotInfo2, match.get().getSlotInfo());
	}

	@Test
	public void matchPreviousLocationNotAvailableButByLocality() {

		SlotProfile slotProfile = SlotProfile.priorAllocation(
			resourceProfile,
			Collections.singletonList(tml4),
			Collections.singleton(aidX),
			Collections.emptySet());
		Optional<SlotSelectionStrategy.SlotInfoAndLocality> match = runMatching(slotProfile);

		Assert.assertEquals(slotInfo4, match.get().getSlotInfo());
	}

	@Test
	public void matchPreviousLocationNotAvailableAndAllOthersBlacklisted() {
		HashSet<AllocationID> blacklisted = new HashSet<>(4);
		blacklisted.add(aid1);
		blacklisted.add(aid2);
		blacklisted.add(aid3);
		blacklisted.add(aid4);
		SlotProfile slotProfile = SlotProfile.priorAllocation(
			resourceProfile,
			Collections.singletonList(tml4),
			Collections.singletonList(aidX),
			blacklisted);
		Optional<SlotSelectionStrategy.SlotInfoAndLocality> match = runMatching(slotProfile);

		// there should be no valid option left and we expect null as return
		Assert.assertFalse(match.isPresent());
	}

	@Test
	public void matchPreviousLocationNotAvailableAndSomeOthersBlacklisted() {
		HashSet<AllocationID> blacklisted = new HashSet<>(3);
		blacklisted.add(aid1);
		blacklisted.add(aid3);
		blacklisted.add(aid4);
		SlotProfile slotProfile = SlotProfile.priorAllocation(
			resourceProfile,
			Collections.singletonList(tml4),
			Collections.singletonList(aidX),
			blacklisted);
		Optional<SlotSelectionStrategy.SlotInfoAndLocality> match = runMatching(slotProfile);

		// we expect that the candidate that is not blacklisted is returned
		Assert.assertEquals(slotInfo2, match.get().getSlotInfo());
	}
}
