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

package org.apache.flink.yarn;

import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

import java.util.function.BiFunction;

/** A {@link YarnResourceManagerClientFactory} implementation for testing. */
public class TestingYarnResourceManagerClientFactory implements YarnResourceManagerClientFactory {
    private final BiFunction<
                    Integer,
                    AMRMClientAsync.AbstractCallbackHandler,
                    AMRMClientAsync<AMRMClient.ContainerRequest>>
            createResourceManagerClientFunction;

    TestingYarnResourceManagerClientFactory(
            BiFunction<
                            Integer,
                            AMRMClientAsync.AbstractCallbackHandler,
                            AMRMClientAsync<AMRMClient.ContainerRequest>>
                    createResourceManagerClientFunction) {
        this.createResourceManagerClientFunction = createResourceManagerClientFunction;
    }

    @Override
    public AMRMClientAsync<AMRMClient.ContainerRequest> createResourceManagerClient(
            int yarnHeartbeatIntervalMillis,
            AMRMClientAsync.AbstractCallbackHandler callbackHandler) {
        return createResourceManagerClientFunction.apply(
                yarnHeartbeatIntervalMillis, callbackHandler);
    }
}
