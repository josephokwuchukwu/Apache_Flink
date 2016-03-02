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

package org.apache.flink.streaming.connectors.cassandra.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import java.util.ArrayList;

public class CassandraPojoAtLeastOnceSinkExample {
	private static final ArrayList<Message> messages = new ArrayList<>(20);

	static {
		for (long i = 0; i < 20; i++) {
			messages.add(new Message("cassandra-" + i));
		}
	}

	/*
	 *	create table: "CREATE TABLE IF NOT EXISTS test.message(body txt PRIMARY KEY);"
	 */
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Message> source = env.fromCollection(messages);

		CassandraSink.addSink(source)
			.setClusterBuilder(new ClusterBuilder() {
				@Override
				protected Cluster buildCluster(Builder builder) {
					return builder.addContactPoint("127.0.0.1").build();
				}
			})
			.build();

		env.execute("Cassandra Sink example");
	}
}
