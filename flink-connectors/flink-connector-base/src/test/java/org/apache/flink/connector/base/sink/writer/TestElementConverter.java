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

package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

/** A test implementation of {@link ElementConverter} used to verify the open() method. */
public class TestElementConverter implements ElementConverter<String, Integer> {

    private static final long serialVersionUID = 1L;
    private int openCallCount;

    @Override
    public void open(WriterInitContext context) {
        openCallCount++;
    }

    @Override
    public Integer apply(String element, SinkWriter.Context context) {
        return openCallCount;
    }

    public int getOpenCallCount() {
        return openCallCount;
    }
}
