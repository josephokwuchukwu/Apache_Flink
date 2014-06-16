/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.task.chaining;

import eu.stratosphere.api.common.functions.Function;
import eu.stratosphere.api.common.operators.base.BulkIterationBase;
import eu.stratosphere.api.common.operators.base.BulkIterationBase.TerminationCriterionAccumulator;
import eu.stratosphere.nephele.template.AbstractInvokable;

public class ChainedTerminationCriterionDriver<IT, OT> extends ChainedDriver<IT, OT> {
	
	private TerminationCriterionAccumulator acc;

	// --------------------------------------------------------------------------------------------

	@Override
	public void setup(AbstractInvokable parent) {
		acc = (TerminationCriterionAccumulator) getUdfRuntimeContext().<Long, Long>getAccumulator(BulkIterationBase.TERMINATION_CRITERION_ACCUMULATOR_NAME);
	}

	@Override
	public void openTask() {}

	@Override
	public void closeTask() {}

	@Override
	public void cancelTask() {}

	// --------------------------------------------------------------------------------------------

	public Function getStub() {
		return null;
	}

	public String getTaskName() {
		return "";
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void collect(IT record) {
		acc.add(1L);
	}

	@Override
	public void close() {}
}
