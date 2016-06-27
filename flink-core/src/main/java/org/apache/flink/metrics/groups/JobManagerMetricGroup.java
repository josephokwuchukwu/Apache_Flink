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
package org.apache.flink.metrics.groups;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.metrics.MetricRegistry;
import org.apache.flink.metrics.groups.scope.ScopeFormat.JobManagerScopeFormat;

import java.util.HashMap;
import java.util.Map;

/**
 * Special {@link org.apache.flink.metrics.MetricGroup} representing a JobManager.
 *
 * <p>Contains extra logic for adding jobs with tasks, and removing jobs when they do
 * not contain tasks any more
 */
@Internal
public class JobManagerMetricGroup extends ComponentMetricGroup {

	private final Map<JobID, JobManagerJobMetricGroup> jobs = new HashMap<>();

	private final String hostname;

	public JobManagerMetricGroup(MetricRegistry registry, String hostname) {
		this(registry, registry.getScopeFormats().getJobManagerFormat(), hostname);
	}

	public JobManagerMetricGroup(
		MetricRegistry registry,
		JobManagerScopeFormat scopeFormat,
		String hostname) {

		super(registry, scopeFormat.formatScope(hostname));
		this.hostname = hostname;
	}

	public String hostname() {
		return hostname;
	}

	// ------------------------------------------------------------------------
	//  job groups
	// ------------------------------------------------------------------------

	public JobManagerJobMetricGroup addJob(
		JobID jobId,
		String jobName) {
		// get or create a jobs metric group
		JobManagerJobMetricGroup currentJobGroup;
		synchronized (this) {
			if (isClosed()) {
				currentJobGroup = jobs.get(jobId);

				if (currentJobGroup == null || currentJobGroup.isClosed()) {
					currentJobGroup = new JobManagerJobMetricGroup(registry, this, jobId, jobName);
					jobs.put(jobId, currentJobGroup);
				}
				return currentJobGroup;
			} else {
				return null;
			}
		}
	}

	public void removeJob(JobID jobId) {
		if (jobId == null) {
			return;
		}

		synchronized (this) {
			JobManagerJobMetricGroup containedGroup = jobs.remove(jobId);
			if (containedGroup != null) {
				containedGroup.close();
			}
		}
	}

	public int numRegisteredJobMetricGroups() {
		return jobs.size();
	}

	@Override
	protected Iterable<? extends ComponentMetricGroup> subComponents() {
		return jobs.values();
	}
}

