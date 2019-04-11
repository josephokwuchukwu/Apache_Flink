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

import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import java.util.List;

/**
 * This interface is responsible for reading database/table/views/UDFs from a registered catalog.
 * It connects a registered catalog and Flink's Table API.
 */
public interface ReadableCatalog {

	/**
	 * Open the catalog. Used for any required preparation in initialization phase.
	 */
	void open();

	/**
	 * Close the catalog when it is no longer needed and release any resource that it might be holding.
	 */
	void close();

	// ------ databases ------

	/**
	 * Get the name of the current database of this type of catalog. This is used when users refers an object in the catalog
	 * without specifying a database. For example, the current db in a Hive Metastore is 'default' by default.
	 *
	 * @return the name of the current database
	 */
	String getCurrentDatabase();

	/**
	 * Set the database with the given name as the current database. A current database is used when users refers an object
	 * in the catalog without specifying a database.
	 *
	 * @param databaseName	the name of the database
	 */
	void setCurrentDatabase(String databaseName) throws DatabaseNotExistException;

	/**
	 * Get the names of all databases in this catalog.
	 *
	 * @return The list of the names of all databases
	 */
	List<String> listDatabases();

	/**
	 * Get a database from this catalog.
	 *
	 * @param databaseName	Name of the database
	 * @return The requested database
	 * @throws DatabaseNotExistException if the database does not exist
	 */
	CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException;

	/**
	 * Check if a database exists in this catalog.
	 *
	 * @param databaseName		Name of the database
	 */
	boolean databaseExists(String databaseName);

	/**
	 * Get names of all tables and views under this database. An empty list is returned if none exists.
	 *
	 * @return A list of the names of all tables and views in this database
	 * @throws DatabaseNotExistException if the database does not exist
	 */
	List<String> listTables(String databaseName) throws DatabaseNotExistException;

	/**
	 * Get names of all views under this database. An empty list is returned if none exists.
	 *
	 * @param databaseName the name of the given database
	 * @return the list of the names of all views in the given database
	 * @throws DatabaseNotExistException if the database does not exist
	 */
	List<String> listViews(String databaseName) throws DatabaseNotExistException;

	/**
	 * Get a CatalogTable or CatalogView identified by objectPath.
	 *
	 * @param objectPath		Path of the table or view
	 * @throws TableNotExistException if the target does not exist
	 * @return The requested table or view
	 */
	CommonTable getTable(ObjectPath objectPath) throws TableNotExistException;

	/**
	 * Check if a table or view exists in this catalog.
	 *
	 * @param objectPath    Path of the table or view
	 */
	boolean tableExists(ObjectPath objectPath);

}
