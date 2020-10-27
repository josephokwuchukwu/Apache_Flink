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

package org.apache.flink.table.factories;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.table.data.RowData;

/**
 * Base interface for configuring a {@link Encoder} for file system connector.
 *
 * @see FactoryUtil#createTableFactoryHelper(DynamicTableFactory, DynamicTableFactory.Context)
 */
@Internal
public interface EncoderFactory extends EncodingFormatFactory<Encoder<RowData>> {
	// interface is used for discovery but is already fully specified by the generics
}
