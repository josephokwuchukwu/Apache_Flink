/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.batch.connectors.cassandra.example;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.batch.connectors.cassandra.CassandraPojoInputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraTupleOutputFormat;
import org.apache.flink.batch.connectors.cassandra.CustomCassandraAnnotatedPojo;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.mapping.Mapper;

import java.util.ArrayList;

/**
 * This is an example showing the to use the {@link CassandraPojoInputFormat}/{@link CassandraTupleOutputFormat} in the Batch API.
 *
 * <p>The example assumes that a table exists in a local cassandra database, according to the following queries:
 * CREATE KEYSPACE IF NOT EXISTS flink WITH replication = {'class': 'SimpleStrategy', 'replication_factor': ‘1’};
 * CREATE TABLE IF NOT EXISTS flink.batches (id text, counter int, batch_id int, PRIMARY KEY(id, counter, batchId));
 */
public class BatchPojoExample {
	private static final String INSERT_QUERY = "INSERT INTO flink.batches (id, counter, batch_id) VALUES (?,?,?);";
	private static final String SELECT_QUERY = "SELECT id, counter, batch_id FROM flink.batches;";

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		ArrayList<Tuple3<String, Integer, Integer>> collection = new ArrayList<>(20);
		for (int i = 0; i < 20; i++) {
			collection.add(new Tuple3<>("string " + i, i, i));
		}

		DataSet<Tuple3<String, Integer, Integer>> dataSet = env.fromCollection(collection);

		ClusterBuilder clusterBuilder = new ClusterBuilder() {
			private static final long serialVersionUID = -1754532803757154795L;

			@Override
			protected Cluster buildCluster(Cluster.Builder builder) {
				return builder.addContactPoints("127.0.0.1").build();
			}
		};

		dataSet.output(new CassandraTupleOutputFormat<>(INSERT_QUERY, clusterBuilder));

		env.execute("Write");

		/*
		 *	This is for the purpose of showing an example of creating a DataSet using CassandraPojoInputFormat.
		 */
		DataSet<CustomCassandraAnnotatedPojo> inputDS = env
			.createInput(new CassandraPojoInputFormat<>(
				SELECT_QUERY,
				clusterBuilder,
				CustomCassandraAnnotatedPojo.class,
				() -> new Mapper.Option[]{Mapper.Option.consistencyLevel(ConsistencyLevel.ANY)}
			));

		inputDS.print();
	}
}
