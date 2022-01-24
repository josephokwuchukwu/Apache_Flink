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

package org.apache.flink.streaming.connectors.dynamodb.util;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.streaming.connectors.dynamodb.sink.DynamoDbWriteRequest;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.util.Map;

/** DynamoDB element converter test implementation. */
public class DynamoDbElementConverter
        implements ElementConverter<Map<String, AttributeValue>, DynamoDbWriteRequest> {
    private final String tableName;

    public DynamoDbElementConverter(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public DynamoDbWriteRequest apply(
            Map<String, AttributeValue> elements, SinkWriter.Context context) {
        return new DynamoDbWriteRequest(
                tableName,
                WriteRequest.builder()
                        .putRequest(PutRequest.builder().item(elements).build())
                        .build());
    }
}
