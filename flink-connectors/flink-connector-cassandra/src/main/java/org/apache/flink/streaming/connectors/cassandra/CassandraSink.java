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
package org.apache.flink.streaming.connectors.cassandra;

import com.datastax.driver.core.Cluster;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;

/**
 * This class wraps different Cassandra sink implementations to provide a common interface for all of them.
 *
 * @param <IN> input type
 */
public class CassandraSink<IN> {
	private final boolean useDataStreamSink;
	private DataStreamSink<IN> sink1;
	private SingleOutputStreamOperator<IN> sink2;

	private CassandraSink(DataStreamSink<IN> sink) {
		sink1 = sink;
		useDataStreamSink = true;
	}

	private CassandraSink(SingleOutputStreamOperator<IN> sink) {
		sink2 = sink;
		useDataStreamSink = false;
	}

	private SinkTransformation<IN> getSinkTransformation() {
		return sink1.getTransformation();
	}

	private StreamTransformation<IN> getStreamTransformation() {
		return sink2.getTransformation();
	}

	/**
	 * Sets the name of this sink. This name is
	 * used by the visualization and logging during runtime.
	 *
	 * @return The named sink.
	 */
	public CassandraSink<IN> name(String name) {
		if (useDataStreamSink) {
			getSinkTransformation().setName(name);
		} else {
			getStreamTransformation().setName(name);
		}
		return this;
	}

	/**
	 * Sets an ID for this operator.
	 * <p/>
	 * <p>The specified ID is used to assign the same operator ID across job
	 * submissions (for example when starting a job from a savepoint).
	 * <p/>
	 * <p><strong>Important</strong>: this ID needs to be unique per
	 * transformation and job. Otherwise, job submission will fail.
	 *
	 * @param uid The unique user-specified ID of this transformation.
	 * @return The operator with the specified ID.
	 */
	@PublicEvolving
	public CassandraSink<IN> uid(String uid) {
		if (useDataStreamSink) {
			getSinkTransformation().setUid(uid);
		} else {
			getStreamTransformation().setUid(uid);
		}
		return this;
	}

	/**
	 * Sets an additional, user provided hash for this operator.
	 * <p/>
	 * <p>The user provided hash is an alternative to the generated hashes, that is considered when identifying an
	 * operator through the default hash mechanics fails (e.g. because of changes between Flink versions).
	 * <p/>
	 * <p><strong>Important</strong>: this hash needs to be unique per transformation and job. Otherwise, job
	 * submission will fail. Furthermore, you cannot assign user-specified hash to intermediate nodes in an operator
	 * chain and trying so will let your job fail.
	 *
	 * @param hash the user provided hash for this operator.
	 * @return The operator with the user provided hash.
	 */
	@PublicEvolving
	public CassandraSink<IN> setAdditionalNodeHash(String hash) {
		if (useDataStreamSink) {
			getSinkTransformation().provideAdditionalNodeHash(hash);
		} else {
			getStreamTransformation().provideAdditionalNodeHash(hash);
		}
		return this;
	}

	/**
	 * Sets the parallelism for this sink. The degree must be higher than zero.
	 *
	 * @param parallelism The parallelism for this sink.
	 * @return The sink with set parallelism.
	 */
	public CassandraSink<IN> setParallelism(int parallelism) {
		if (useDataStreamSink) {
			getSinkTransformation().setParallelism(parallelism);
		} else {
			getStreamTransformation().setParallelism(parallelism);
		}
		return this;
	}

	/**
	 * Turns off chaining for this operator so thread co-location will not be
	 * used as an optimization.
	 * <p/>
	 * <p/>
	 * Chaining can be turned off for the whole
	 * job by {@link org.apache.flink.streaming.api.environment.StreamExecutionEnvironment#disableOperatorChaining()}
	 * however it is not advised for performance considerations.
	 *
	 * @return The sink with chaining disabled
	 */
	public CassandraSink<IN> disableChaining() {
		if (useDataStreamSink) {
			getSinkTransformation().setChainingStrategy(ChainingStrategy.NEVER);
		} else {
			getStreamTransformation().setChainingStrategy(ChainingStrategy.NEVER);
		}
		return this;
	}

	/**
	 * Sets the slot sharing group of this operation. Parallel instances of
	 * operations that are in the same slot sharing group will be co-located in the same
	 * TaskManager slot, if possible.
	 * <p/>
	 * <p>Operations inherit the slot sharing group of input operations if all input operations
	 * are in the same slot sharing group and no slot sharing group was explicitly specified.
	 * <p/>
	 * <p>Initially an operation is in the default slot sharing group. An operation can be put into
	 * the default group explicitly by setting the slot sharing group to {@code "default"}.
	 *
	 * @param slotSharingGroup The slot sharing group name.
	 */
	public CassandraSink<IN> slotSharingGroup(String slotSharingGroup) {
		if (useDataStreamSink) {
			getSinkTransformation().setSlotSharingGroup(slotSharingGroup);
		} else {
			getStreamTransformation().setSlotSharingGroup(slotSharingGroup);
		}
		return this;
	}

	/**
	 * Writes a DataStream into a Cassandra database.
	 *
	 * @param input input DataStream
	 * @param <IN>  input type
	 * @return CassandraSinkBuilder, to further configure the sink
	 */
	public static <IN, T extends Tuple> CassandraSinkBuilder<IN> addSink(DataStream<IN> input) {
		if (input.getType() instanceof TupleTypeInfo) {
			DataStream<T> tupleInput = (DataStream<T>) input;
			return (CassandraSinkBuilder<IN>) new CassandraTupleSinkBuilder<>(tupleInput, tupleInput.getType(), tupleInput.getType().createSerializer(tupleInput.getExecutionEnvironment().getConfig()));
		} else {
			return new CassandraPojoSinkBuilder<>(input, input.getType(), input.getType().createSerializer(input.getExecutionEnvironment().getConfig()));
		}
	}

	public abstract static class CassandraSinkBuilder<IN> {
		protected final DataStream<IN> input;
		protected final TypeSerializer<IN> serializer;
		protected final TypeInformation<IN> typeInfo;
		protected ClusterBuilder builder;
		protected String query;
		protected CheckpointCommitter committer;
		protected boolean isWriteAheadLogEnabled;

		public CassandraSinkBuilder(DataStream<IN> input, TypeInformation<IN> typeInfo, TypeSerializer<IN> serializer) {
			this.input = input;
			this.typeInfo = typeInfo;
			this.serializer = serializer;
		}

		/**
		 * Sets the query that is to be executed for every record.
		 *
		 * @param query query to use
		 * @return this builder
		 */
		public CassandraSinkBuilder<IN> setQuery(String query) {
			this.query = query;
			return this;
		}

		/**
		 * Sets the cassandra host to connect to.
		 *
		 * @param host host to connect to
		 * @return this builder
		 */
		public CassandraSinkBuilder<IN> setHost(String host) {
			return setHost(host, 9042);
		}

		/**
		 * Sets the cassandra host/port to connect to.
		 *
		 * @param host host to connect to
		 * @param port port to connect to
		 * @return this builder
		 */
		public CassandraSinkBuilder<IN> setHost(final String host, final int port) {
			if (this.builder != null) {
				throw new IllegalArgumentException("Builder was already set. You must use either setHost() or setClusterBuilder().");
			}
			this.builder = new ClusterBuilder() {
				@Override
				protected Cluster buildCluster(Cluster.Builder builder) {
					return builder.addContactPoint(host).withPort(port).build();
				}
			};
			return this;
		}

		/**
		 * Sets the ClusterBuilder for this sink. A ClusterBuilder is used to configure the connection to cassandra.
		 *
		 * @param builder ClusterBuilder to configure the connection to cassandra
		 * @return this builder
		 */
		public CassandraSinkBuilder<IN> setClusterBuilder(ClusterBuilder builder) {
			if (this.builder != null) {
				throw new IllegalArgumentException("Builder was already set. You must use either setHost() or setClusterBuilder().");
			}
			this.builder = builder;
			return this;
		}

		/**
		 * Enables the write-ahead log, which allows exactly-once processing for non-deterministic algorithms that use
		 * idempotent updates.
		 *
		 * @return this builder
		 */
		public CassandraSinkBuilder<IN> enableWriteAheadLog() {
			this.isWriteAheadLogEnabled = true;
			return this;
		}

		/**
		 * Enables the write-ahead log, which allows exactly-once processing for non-deterministic algorithms that use
		 * idempotent updates.
		 *
		 * @param committer CheckpointCommitter, that stores informationa bout completed checkpoints in an external
		 *                  resource. By default this information is stored within a separate table within Cassandra.
		 * @return this builder
		 */
		public CassandraSinkBuilder<IN> enableWriteAheadLog(CheckpointCommitter committer) {
			this.isWriteAheadLogEnabled = true;
			this.committer = committer;
			return this;
		}

		/**
		 * Finalizes the configuration of this sink.
		 *
		 * @return finalized sink
		 * @throws Exception
		 */
		public abstract CassandraSink<IN> build() throws Exception;

		protected void sanityCheck() {
			if (builder == null) {
				throw new IllegalArgumentException("Cassandra host information must be supplied using either setHost() or setClusterBuilder().");
			}
		}
	}

	public static class CassandraTupleSinkBuilder<IN extends Tuple> extends CassandraSinkBuilder<IN> {
		public CassandraTupleSinkBuilder(DataStream<IN> input, TypeInformation<IN> typeInfo, TypeSerializer<IN> serializer) {
			super(input, typeInfo, serializer);
		}

		@Override
		protected void sanityCheck() {
			super.sanityCheck();
			if (query == null || query.length() == 0) {
				throw new IllegalArgumentException("Query must not be null or empty.");
			}
		}

		@Override
		public CassandraSink<IN> build() throws Exception {
			sanityCheck();
			if (isWriteAheadLogEnabled) {
				return committer == null
					? new CassandraSink<>(input.transform("Cassandra Sink", null, new CassandraTupleWriteAheadSink<>(query, serializer, builder, new CassandraCommitter(builder))))
					: new CassandraSink<>(input.transform("Cassandra Sink", null, new CassandraTupleWriteAheadSink<>(query, serializer, builder, committer)));
			} else {
				return new CassandraSink<>(input.addSink(new CassandraTupleSink<IN>(query, builder)).name("Cassandra Sink"));
			}
		}
	}

	public static class CassandraPojoSinkBuilder<IN> extends CassandraSinkBuilder<IN> {
		public CassandraPojoSinkBuilder(DataStream<IN> input, TypeInformation<IN> typeInfo, TypeSerializer<IN> serializer) {
			super(input, typeInfo, serializer);
		}

		@Override
		protected void sanityCheck() {
			super.sanityCheck();
			if (query != null) {
				throw new IllegalArgumentException("Specifying a query is not allowed when using a Pojo-Stream as input.");
			}
		}

		@Override
		public CassandraSink<IN> build() throws Exception {
			sanityCheck();
			if (isWriteAheadLogEnabled) {
				throw new IllegalArgumentException("Exactly-once guarantees can only be provided for tuple types.");
			} else {
				return new CassandraSink<>(input.addSink(new CassandraPojoSink<>(typeInfo.getTypeClass(), builder)).name("Cassandra Sink"));
			}
		}
	}
}
