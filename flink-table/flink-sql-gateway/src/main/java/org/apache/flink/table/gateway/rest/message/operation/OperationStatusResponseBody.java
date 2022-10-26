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

package org.apache.flink.table.gateway.rest.message.operation;

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/** {@link ResponseBody} for getting the status of operation. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OperationStatusResponseBody implements ResponseBody {

    private static final String FIELD_NAME_STATUS = "status";

    @JsonProperty(FIELD_NAME_STATUS)
    private final String status;

    public OperationStatusResponseBody(@JsonProperty(FIELD_NAME_STATUS) String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }
}
