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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.redis.common.config.JedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.JedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataTypeDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RedisSinkTest extends RedisTestBase {

	private JedisPoolConfig jedisPoolConfig;
	private static final Long NUM_ELEMENTS = 20L;
	private static final String REDIS_KEY = "TEST_KEY";
	private static final String REDIS_ADDITIONAL_KEY = "TEST_ADDITIONAL_KEY";

	StreamExecutionEnvironment env;


	private Jedis jedis;

	@Before
	public void setUp(){
		jedisPoolConfig = new JedisPoolConfig.Builder()
			.setHost(REDIS_HOST)
			.setPort(REDIS_PORT).build();
		jedis = new Jedis(REDIS_HOST, REDIS_PORT);
		env = StreamExecutionEnvironment.getExecutionEnvironment();
	}

	@Test
	public void testRedisListDataType() throws Exception {
		DataStreamSource<Tuple2<String, String>> source = env.addSource(new TestSourceFunction());
		RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(jedisPoolConfig,
			new RedisDataMapper(RedisDataType.LIST));

		source.addSink(redisSink);
		env.execute("Test Redis List Data Type");

		assertEquals(NUM_ELEMENTS, jedis.llen(REDIS_KEY));

		jedis.del(REDIS_KEY);
	}

	@Test
	public void testRedisSetDataType() throws Exception {
		DataStreamSource<Tuple2<String, String>> source = env.addSource(new TestSourceFunction());
		RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(jedisPoolConfig,
			new RedisDataMapper(RedisDataType.SET));

		source.addSink(redisSink);
		env.execute("Test Redis Set Data Type");

		assertEquals(NUM_ELEMENTS, jedis.scard(REDIS_KEY));

		jedis.del(REDIS_KEY);
	}

	@Test
	public void testRedisHyperLogLogDataType() throws Exception {
		DataStreamSource<Tuple2<String, String>> source = env.addSource(new TestSourceFunction());
		RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(jedisPoolConfig,
			new RedisDataMapper(RedisDataType.HYPER_LOG_LOG));

		source.addSink(redisSink);
		env.execute("Test Redis Hyper Log Log Data Type");

		assertEquals(NUM_ELEMENTS, Long.valueOf(jedis.pfcount(REDIS_KEY)));

		jedis.del(REDIS_KEY);
	}

	@Test
	public void testRedisSortedSetDataType() throws Exception {
		DataStreamSource<Tuple2<String, String>> source = env.addSource(new TestSourceFunctionAdditionalKey());
		RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(jedisPoolConfig,
			new RedisAdditionalDataMapper(RedisDataType.SORTED_SET));

		source.addSink(redisSink);
		env.execute("Test Redis Sorted Set Data Type");

		assertEquals(NUM_ELEMENTS, jedis.zcard(REDIS_ADDITIONAL_KEY));

		jedis.del(REDIS_ADDITIONAL_KEY);
	}

	@Test
	public void testRedisHashDataType() throws Exception {
		DataStreamSource<Tuple2<String, String>> source = env.addSource(new TestSourceFunctionAdditionalKey());
		RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(jedisPoolConfig,
			new RedisAdditionalDataMapper(RedisDataType.HASH));

		source.addSink(redisSink);
		env.execute("Test Redis Hash Data Type");

		assertEquals(NUM_ELEMENTS, jedis.hlen(REDIS_ADDITIONAL_KEY));

		jedis.del(REDIS_ADDITIONAL_KEY);
	}

	@Test(expected=NullPointerException.class)
	public void shouldThrowNullPointExceptionIfDataMapperIsNull(){
		new RedisSink(new JedisClusterConfig.Builder().build(), null);
	}

	@After
	public void tearDown(){
		if(jedis != null){
			jedis.close();
		}
	}

	private static class TestSourceFunction implements SourceFunction<Tuple2<String, String>> {
		private static final long serialVersionUID = 1L;

		private volatile boolean running = true;

		@Override
		public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
			for (int i = 0; i < NUM_ELEMENTS && running; i++) {
				ctx.collect(new Tuple2<>(REDIS_KEY, "message #" + i));
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	private static class TestSourceFunctionAdditionalKey implements SourceFunction<Tuple2<String, String>> {
		private static final long serialVersionUID = 1L;

		private volatile boolean running = true;

		@Override
		public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
			for (int i = 0; i < NUM_ELEMENTS && running; i++) {
				ctx.collect(new Tuple2<>("" + i, "message #" + i));
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	public static class RedisDataMapper implements RedisMapper<Tuple2<String, String>>{

		private RedisDataType dataType;

		public RedisDataMapper(RedisDataType dataType){
			this.dataType = dataType;
		}
		@Override
		public RedisDataTypeDescription getDataTypeDescription() {
			return new RedisDataTypeDescription(dataType);
		}

		@Override
		public String getKeyFromData(Tuple2<String, String> data) {
			return data.f0;
		}

		@Override
		public String getValueFromData(Tuple2<String, String> data) {
			return data.f1;
		}
	}

	public static class RedisAdditionalDataMapper implements RedisMapper<Tuple2<String, String>>{

		private RedisDataType dataType;

		public RedisAdditionalDataMapper(RedisDataType dataType){
			this.dataType = dataType;
		}

		@Override
		public RedisDataTypeDescription getDataTypeDescription() {
			return new RedisDataTypeDescription(dataType, REDIS_ADDITIONAL_KEY);
		}

		@Override
		public String getKeyFromData(Tuple2<String, String> data) {
			return data.f0;
		}

		@Override
		public String getValueFromData(Tuple2<String, String> data) {
			return data.f1;
		}
	}
}
