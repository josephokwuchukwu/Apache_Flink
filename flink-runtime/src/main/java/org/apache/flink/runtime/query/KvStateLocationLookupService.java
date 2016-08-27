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

package org.apache.flink.runtime.query;

import org.apache.flink.api.common.JobID;
import scala.concurrent.Future;

/**
 * {@link KvStateLocation} lookup service.
 */
public interface KvStateLocationLookupService {

	/**
	 * Starts the lookup service.
	 */
	void start();

	/**
	 * Shuts down the lookup service.
	 */
	void shutDown();

	/**
	 * Returns a future holding the {@link KvStateLocation} for the given job
	 * and KvState registration name.
	 *
	 * @param jobId            JobID the KvState instance belongs to
	 * @param registrationName Name under which the KvState has been registered
	 * @return Future holding the {@link KvStateLocation}
	 */
	Future<KvStateLocation> getKvStateLookupInfo(JobID jobId, String registrationName);

}
