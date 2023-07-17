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

package org.apache.flink.table.catalog;

import org.apache.flink.util.Preconditions;

/** The AbstractCatalogStore class is an abstract base class for implementing a catalog store. */
public abstract class AbstractCatalogStore implements CatalogStore {

    private boolean isOpen;

    /** Opens the catalog store. */
    @Override
    public void open() {
        isOpen = true;
    }

    /** Closes the catalog store. */
    @Override
    public void close() {
        isOpen = false;
    }

    /**
     * Returns whether the catalog store is currently open.
     *
     * @return true if the store is open, false otherwise
     */
    public boolean isOpen() {
        return isOpen;
    }

    /**
     * Checks whether the catalog store is currently open.
     *
     * @throws IllegalStateException if the store is closed
     */
    public void checkOpenState() {
        Preconditions.checkState(isOpen(), "Catalog store is not open.");
    }
}
