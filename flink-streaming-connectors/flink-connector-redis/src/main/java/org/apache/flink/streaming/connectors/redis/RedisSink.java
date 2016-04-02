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

package org.apache.flink.streaming.connectors.redis;

import com.google.common.base.Preconditions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.common.config.JedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.JedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.JedisSentinelConfig;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataTypeDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A sink that delivers data to a Redis channel using the Jedis client.
 * <p>When creating the sink using first constructor {@link #RedisSink(JedisPoolConfig, RedisMapper)}
 * the sink will create connection using {@link redis.clients.jedis.JedisPool}.
 * <p>When using second constructor {@link #RedisSink(JedisSentinelConfig, RedisMapper)} the sink will create connection
 * using {@link redis.clients.jedis.JedisSentinelPool} to Redis cluster. Use this if redis is
 * configured using sentinels else use the third constructor {@link #RedisSink(JedisClusterConfig, RedisMapper)}
 * which use {@link redis.clients.jedis.JedisCluster} to connect to Redis cluster.
 *
 * <p>Example:
 *
 * <pre>
 *{@code
 *public static class RedisExampleDataMapper implements RedisMapper<Tuple2<String, String>> {
 *    public RedisDataTypeDescription getDataTypeDescription() {
 *        return new RedisDataTypeDescription(dataType, REDIS_ADDITIONAL_KEY);
 *    }
 *    public String getKeyFromData(Tuple2 data) {
 *        return String.valueOf(data.f0);
 *    }
 *    public String getValueFromData(Tuple2 data) {
 *        return String.valueOf(data.f1);
 *    }
 *}
 *JedisPoolConfig jedisPoolConfig = new JedisPoolConfig.Builder()
 *    .setHost(REDIS_HOST).setPort(REDIS_PORT).build();
 *new RedisSink<String>(jedisPoolConfig, new RedisExampleDataMapper());
 *}</pre>
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public class RedisSink<IN> extends RichSinkFunction<IN> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);

	/**
	 * This additional key needed for {@link RedisDataType#HASH} and {@link RedisDataType#SORTED_SET}.
	 * Other {@link RedisDataType} works only with two variable i.e. name of the list and value to be added.
	 * But for {@link RedisDataType#HASH} and {@link RedisDataType#SORTED_SET} we need three variables.
	 * <p>For {@link RedisDataType#HASH} we need hash name, hash key and element.
	 * additionalKey used as hash name for {@link RedisDataType#HASH}
	 * <p>For {@link RedisDataType#SORTED_SET} we need set name, the element and it's score.
	 * additionalKey used as set name for {@link RedisDataType#SORTED_SET}
	 */
	private String additionalKey;
	private RedisMapper<IN> redisSinkMapper;
	private RedisDataType redisDataType;

	private JedisPoolConfig jedisPoolConfig;
	private JedisSentinelConfig jedisSentinelConfig;
	private JedisClusterConfig jedisClusterConfig;

	private RedisCommandsContainer redisCommandsContainer;

	/**
	 * Creates a new RedisSink that connects to the Redis Server.
	 *
	 * @param jedisPoolConfig The configuration of {@link org.apache.flink.streaming.connectors.redis.common.config.JedisPoolConfig}
	 * @param redisSinkMapper This used for generate redis command and key value from incoming elements
	 */
	public RedisSink(JedisPoolConfig jedisPoolConfig, RedisMapper<IN> redisSinkMapper) {
		Preconditions.checkNotNull(jedisPoolConfig, "Redis connection pool config should not be Null");
		Preconditions.checkNotNull(redisSinkMapper, "Redis Mapper can not be null");

		this.redisSinkMapper = redisSinkMapper;
		this.jedisPoolConfig = jedisPoolConfig;
		RedisDataTypeDescription dataTypeDescription = redisSinkMapper.getDataTypeDescription();
		this.redisDataType = dataTypeDescription.getDataType();
		this.additionalKey = dataTypeDescription.getAdditionalKey();

	}

	/**
	 * Creates a new RedisSink that connects to the Redis Sentinels.
	 *
	 * @param jedisSentinelConfig The configuration of {@link org.apache.flink.streaming.connectors.redis.common.config.JedisSentinelConfig}
	 * @param redisSinkMapper This used for generate redis command and key value from incoming elements
	 */
	public RedisSink(JedisSentinelConfig jedisSentinelConfig, RedisMapper<IN> redisSinkMapper) {
		Preconditions.checkNotNull(jedisSentinelConfig, "Redis Sentinel connection pool config should not be Null");
		Preconditions.checkNotNull(redisSinkMapper, "Redis Mapper can not be null");

		this.redisSinkMapper = redisSinkMapper;
		this.jedisSentinelConfig = jedisSentinelConfig;
		RedisDataTypeDescription dataTypeDescription = redisSinkMapper.getDataTypeDescription();
		this.redisDataType = dataTypeDescription.getDataType();
		this.additionalKey = dataTypeDescription.getAdditionalKey();

	}

	/**
	 * Creates a new RedisSink that connects to the Redis Cluster.
	 *
	 * @param jedisClusterConfig The configuration of {@link org.apache.flink.streaming.connectors.redis.common.config.JedisClusterConfig}
	 * @param redisSinkMapper This used for generate redis command and key value from incoming elements
	 */
	public RedisSink(JedisClusterConfig jedisClusterConfig, RedisMapper<IN> redisSinkMapper) {
		Preconditions.checkNotNull(jedisClusterConfig, "Redis cluster config should not be Null");
		Preconditions.checkNotNull(redisSinkMapper, "Redis Mapper can not be null");

		this.redisSinkMapper = redisSinkMapper;
		this.jedisClusterConfig = jedisClusterConfig;
		RedisDataTypeDescription dataTypeDescription = redisSinkMapper.getDataTypeDescription();
		this.redisDataType = dataTypeDescription.getDataType();
		this.additionalKey = dataTypeDescription.getAdditionalKey();

	}

	/**
	 * Called when new data arrives to the sink, and forwards it to Redis channel.
	 * <p> If redis data type is {@link RedisDataType#HASH} Redis HSET command will be applied.
	 * <p> If redis data type is {@link RedisDataType#LIST} Redis RPUSH command will be applied.
	 * <p> If redis data type is {@link RedisDataType#SET} Redis SADD command will be applied.
	 * <p> If redis data type is {@link RedisDataType#PUBSUB} Redis PUBLISH command will be applied.
	 * <p> If redis data type is {@link RedisDataType#STRING} Redis SET command will be applied.
	 * <p> If redis data type is {@link RedisDataType#HYPER_LOG_LOG} Redis PFADD command will be applied.
	 * <p> If redis data type is {@link RedisDataType#SORTED_SET} Redis ZADD command will be applied.
	 *
	 * @param input The incoming data
	 */
	@Override
	public void invoke(IN input) throws Exception {
		String key = redisSinkMapper.getKeyFromData(input);
		String value = redisSinkMapper.getValueFromData(input);

		switch (redisDataType) {
			case HASH:
				this.redisCommandsContainer.hset(this.additionalKey, key, value);
				break;

			case LIST:
				this.redisCommandsContainer.rpush(key, value);
				break;

			case SET:
				this.redisCommandsContainer.sadd(key, value);
				break;

			case PUBSUB:
				this.redisCommandsContainer.publish(key, value);
				break;

			case STRING:
				this.redisCommandsContainer.set(key, value);
				break;

			case HYPER_LOG_LOG:
				this.redisCommandsContainer.pfadd(key, value);
				break;

			case SORTED_SET:
				this.redisCommandsContainer.zadd(this.additionalKey, value, key);
				break;

			default:
				throw new IllegalArgumentException("Cannot process such data type: " + redisDataType);
		}

	}

	/**
	 * Initializes the connection to Redis by either cluster or sentinels or single server
	 *
	 * @throws IllegalArgumentException if jedisPoolConfig, jedisClusterConfig and jedisSentinelConfig all are null
     */
	@Override
	public void open(Configuration parameters) throws Exception {
		if (jedisPoolConfig != null) {
			this.redisCommandsContainer = RedisCommandsContainerBuilder.build(jedisPoolConfig);
		} else if (jedisClusterConfig != null) {
			this.redisCommandsContainer = RedisCommandsContainerBuilder.build(jedisClusterConfig);
		} else if (jedisSentinelConfig != null) {
			this.redisCommandsContainer = RedisCommandsContainerBuilder.build(jedisSentinelConfig);
		} else {
			throw new IllegalArgumentException("Jedis configuration not found");
		}
	}

	/**
	 * Closes commands container
	 * @throws IOException if command container is unable to close.
	 */
	@Override
	public void close() throws IOException {
		if (redisCommandsContainer != null) {
			try {
				redisCommandsContainer.close();
			} catch (IOException e) {
				if (LOG.isErrorEnabled()) {
					LOG.error("failed to close Redis Commands Container {}", e);
				}
				throw e;
			}
		}
	}
}
