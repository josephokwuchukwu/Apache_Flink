/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.fs;

import java.io.Closeable;

/**
 * This interface extends the {@link CloseableRegistry} interface by a close() method that should only be visible to
 * the object owning the registry. This establishes a separation of concerns between objects that we can pass a plain
 * {@link CloseableRegistry}, so that they can register their closeable objects and the owner that is finally
 * responsible for closing a registry. This prevents accidental closing by objects that are not allowed to do so.
 *
 */
public interface OwnedCloseableRegistry extends CloseableRegistry, Closeable {}