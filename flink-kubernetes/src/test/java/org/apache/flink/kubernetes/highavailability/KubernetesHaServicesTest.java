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

package org.apache.flink.kubernetes.highavailability;

import org.apache.flink.runtime.blob.VoidBlobStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;
import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_KEY;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/** Tests for the {@link KubernetesHaServices}. */
public class KubernetesHaServicesTest extends KubernetesHighAvailabilityTestBase {

    @Test
    public void testInternalCloseShouldCloseFlinkKubeClient() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            final KubernetesHaServices kubernetesHaServices =
                                    new KubernetesHaServices(
                                            flinkKubeClient,
                                            executorService,
                                            configuration,
                                            new VoidBlobStore());
                            kubernetesHaServices.internalClose();
                            assertThat(closeKubeClientFuture.isDone(), is(true));
                        });
            }
        };
    }

    @Test
    public void testInternalCleanupShouldCleanupConfigMaps() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            final KubernetesHaServices kubernetesHaServices =
                                    new KubernetesHaServices(
                                            flinkKubeClient,
                                            executorService,
                                            configuration,
                                            new VoidBlobStore());
                            kubernetesHaServices.internalCleanup();
                            final Map<String, String> labels =
                                    deleteConfigMapByLabelsFuture.get(
                                            TIMEOUT, TimeUnit.MILLISECONDS);
                            assertThat(labels.size(), is(3));
                            assertThat(
                                    labels.get(LABEL_CONFIGMAP_TYPE_KEY),
                                    is(LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY));
                        });
            }
        };
    }
}
