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

package org.apache.flink.runtime.metrics;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.commons.math3.stat.ranking.NaNStrategy;

/**
 * The {@link DescriptiveStatisticsHistogram} use a DescriptiveStatistics {@link DescriptiveStatistics} as a Flink {@link Histogram}.
 */
public class DescriptiveStatisticsHistogram implements org.apache.flink.metrics.Histogram {

	private final DescriptiveStatistics descriptiveStatistics;

	private long elementsSeen = 0L;

	public DescriptiveStatisticsHistogram(int windowSize) {
		this.descriptiveStatistics = new DescriptiveStatistics(windowSize);
		// since we are storing Long values, we won't have NaN values
		Percentile percentileImpl = new Percentile().withNaNStrategy(NaNStrategy.FIXED);
		descriptiveStatistics.setPercentileImpl(percentileImpl);
	}

	@Override
	public void update(long value) {
		elementsSeen += 1L;
		this.descriptiveStatistics.addValue(value);
	}

	@Override
	public long getCount() {
		return this.elementsSeen;
	}

	@Override
	public HistogramStatistics getStatistics() {
		return new DescriptiveStatisticsHistogramStatistics(this.descriptiveStatistics);
	}
}
