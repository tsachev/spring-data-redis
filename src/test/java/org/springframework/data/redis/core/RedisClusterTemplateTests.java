/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.core;

import static org.hamcrest.core.Is.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.DoubleObjectFactory;
import org.springframework.data.redis.LongObjectFactory;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.PersonObjectFactory;
import org.springframework.data.redis.RawObjectFactory;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConverters;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.OxmSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.oxm.xstream.XStreamMarshaller;

import redis.clients.jedis.Jedis;

/**
 * @author Christoph Strobl
 */
public class RedisClusterTemplateTests<K, V> extends RedisTemplateTests<K, V> {

	static final List<String> CLUSTER_NODES = Arrays.asList("127.0.0.1:7379", "127.0.0.1:7380", "127.0.0.1:7381");

	public RedisClusterTemplateTests(RedisTemplate<K, V> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<V> valueFactory) {
		super(redisTemplate, keyFactory, valueFactory);
	}

	@BeforeClass
	public static void before() {

		Jedis jedis = new Jedis("127.0.0.1", 7379);
		String mode = JedisConverters.toProperties(jedis.info()).getProperty("redis_mode");
		jedis.close();

		Assume.assumeThat(mode, is("cluster"));
	}

	@Test
	@Ignore("not supported in cluster mode")
	public void testGetExpireMillisNotSupported() {
		super.testGetExpireMillisNotSupported();
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	@Ignore("Pipeline not supported in cluster mode")
	public void testExecutePipelinedNonNullRedisCallback() {
		super.testExecutePipelinedNonNullRedisCallback();
	}

	@Test
	@Ignore("Pipeline not supported in cluster mode")
	public void testExecutePipelinedTx() {
		super.testExecutePipelinedTx();
	}

	@Test
	@Ignore("Watch only supported on same connection...")
	public void testWatch() {
		super.testWatch();
	}

	@Test
	@Ignore("Watch only supported on same connection...")
	public void testUnwatch() {
		super.testUnwatch();
	}

	@Test
	@Ignore("EXEC only supported on same connection...")
	public void testExec() {
		super.testExec();
	}

	@Test
	@Ignore("Rename not supported in cluster mode")
	public void testRenameIfAbsent() {
		super.testRenameIfAbsent();
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	@Ignore("Pipeline not supported in cluster mode")
	public void testExecutePipelinedNonNullSessionCallback() {
		super.testExecutePipelinedNonNullSessionCallback();
	}

	@Test
	@Ignore("PubSub not supported in cluster mode")
	public void testConvertAndSend() {
		super.testConvertAndSend();
	}

	@Test
	@Ignore("Watch only supported on same connection...")
	public void testExecConversionDisabled() {
		super.testExecConversionDisabled();
	}

	@Test
	@Ignore("Discard only supported on same connection...")
	public void testDiscard() {
		super.testDiscard();
	}

	@Test
	@Ignore("Pipleline not supported in cluster mode")
	public void testExecutePipelined() {
		super.testExecutePipelined();
	}

	@Test
	@Ignore("Rename not supported in cluster mode")
	public void testRename() {
		super.testRename();
	}

	@Test
	@Ignore("Watch only supported on same connection...")
	public void testWatchMultipleKeys() {
		super.testWatchMultipleKeys();
	}

	@Test
	@Ignore("This one fails when using GET options on numbers")
	public void testSortBulkMapper() {
		super.testSortBulkMapper();
	}

	@Test
	@Ignore("stort store not supported in cluster mode")
	public void testSortStore() {
		super.testSortStore();
	}

	@Parameters
	public static Collection<Object[]> testParams() {

		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<Long> longFactory = new LongObjectFactory();
		ObjectFactory<Double> doubleFactory = new DoubleObjectFactory();
		ObjectFactory<byte[]> rawFactory = new RawObjectFactory();
		ObjectFactory<Person> personFactory = new PersonObjectFactory();

		// XStream serializer
		XStreamMarshaller xstream = new XStreamMarshaller();
		try {
			xstream.afterPropertiesSet();
		} catch (Exception ex) {
			throw new RuntimeException("Cannot init XStream", ex);
		}

		OxmSerializer serializer = new OxmSerializer(xstream, xstream);
		Jackson2JsonRedisSerializer<Person> jackson2JsonSerializer = new Jackson2JsonRedisSerializer<Person>(Person.class);

		// JEDIS
		JedisConnectionFactory jedisConnectionFactory = new JedisConnectionFactory(new RedisClusterConfiguration(
				CLUSTER_NODES));

		jedisConnectionFactory.afterPropertiesSet();

		RedisClusterTemplate<String, String> jedisStringTemplate = new RedisClusterTemplate<String, String>();
		jedisStringTemplate.setDefaultSerializer(new StringRedisSerializer());
		jedisStringTemplate.setConnectionFactory(jedisConnectionFactory);
		jedisStringTemplate.afterPropertiesSet();

		RedisClusterTemplate<String, Long> jedisLongTemplate = new RedisClusterTemplate<String, Long>();
		jedisLongTemplate.setKeySerializer(new StringRedisSerializer());
		jedisLongTemplate.setValueSerializer(new GenericToStringSerializer<Long>(Long.class));
		jedisLongTemplate.setConnectionFactory(jedisConnectionFactory);
		jedisLongTemplate.afterPropertiesSet();

		RedisClusterTemplate<byte[], byte[]> jedisRawTemplate = new RedisClusterTemplate<byte[], byte[]>();
		jedisRawTemplate.setEnableDefaultSerializer(false);
		jedisRawTemplate.setConnectionFactory(jedisConnectionFactory);
		jedisRawTemplate.afterPropertiesSet();

		RedisClusterTemplate<String, Person> jedisPersonTemplate = new RedisClusterTemplate<String, Person>();
		jedisPersonTemplate.setConnectionFactory(jedisConnectionFactory);
		jedisPersonTemplate.afterPropertiesSet();

		RedisClusterTemplate<String, String> jedisXstreamStringTemplate = new RedisClusterTemplate<String, String>();
		jedisXstreamStringTemplate.setConnectionFactory(jedisConnectionFactory);
		jedisXstreamStringTemplate.setDefaultSerializer(serializer);
		jedisXstreamStringTemplate.afterPropertiesSet();

		RedisClusterTemplate<String, Person> jedisJackson2JsonPersonTemplate = new RedisClusterTemplate<String, Person>();
		jedisJackson2JsonPersonTemplate.setConnectionFactory(jedisConnectionFactory);
		jedisJackson2JsonPersonTemplate.setValueSerializer(jackson2JsonSerializer);
		jedisJackson2JsonPersonTemplate.afterPropertiesSet();

		// LETTUCE

		LettuceConnectionFactory lettuceConnectionFactory = new LettuceConnectionFactory(new RedisClusterConfiguration(
				CLUSTER_NODES));

		lettuceConnectionFactory.afterPropertiesSet();

		RedisClusterTemplate<String, String> lettuceStringTemplate = new RedisClusterTemplate<String, String>();
		lettuceStringTemplate.setDefaultSerializer(new StringRedisSerializer());
		lettuceStringTemplate.setConnectionFactory(lettuceConnectionFactory);
		lettuceStringTemplate.afterPropertiesSet();

		RedisClusterTemplate<String, Long> lettuceLongTemplate = new RedisClusterTemplate<String, Long>();
		lettuceLongTemplate.setKeySerializer(new StringRedisSerializer());
		lettuceLongTemplate.setValueSerializer(new GenericToStringSerializer<Long>(Long.class));
		lettuceLongTemplate.setConnectionFactory(lettuceConnectionFactory);
		lettuceLongTemplate.afterPropertiesSet();

		RedisClusterTemplate<byte[], byte[]> lettuceRawTemplate = new RedisClusterTemplate<byte[], byte[]>();
		lettuceRawTemplate.setEnableDefaultSerializer(false);
		lettuceRawTemplate.setConnectionFactory(lettuceConnectionFactory);
		lettuceRawTemplate.afterPropertiesSet();

		RedisClusterTemplate<String, Person> lettucePersonTemplate = new RedisClusterTemplate<String, Person>();
		lettucePersonTemplate.setConnectionFactory(lettuceConnectionFactory);
		lettucePersonTemplate.afterPropertiesSet();

		RedisClusterTemplate<String, String> lettuceXstreamStringTemplate = new RedisClusterTemplate<String, String>();
		lettuceXstreamStringTemplate.setConnectionFactory(lettuceConnectionFactory);
		lettuceXstreamStringTemplate.setDefaultSerializer(serializer);
		lettuceXstreamStringTemplate.afterPropertiesSet();

		RedisClusterTemplate<String, Person> lettuceJackson2JsonPersonTemplate = new RedisClusterTemplate<String, Person>();
		lettuceJackson2JsonPersonTemplate.setConnectionFactory(lettuceConnectionFactory);
		lettuceJackson2JsonPersonTemplate.setValueSerializer(jackson2JsonSerializer);
		lettuceJackson2JsonPersonTemplate.afterPropertiesSet();

		return Arrays.asList(new Object[][] { //

						// JEDIS
						{ jedisStringTemplate, stringFactory, stringFactory }, //
						{ jedisLongTemplate, stringFactory, longFactory }, //
						{ jedisRawTemplate, rawFactory, rawFactory }, //
						{ jedisPersonTemplate, stringFactory, personFactory }, //
						{ jedisXstreamStringTemplate, stringFactory, stringFactory }, //
						{ jedisJackson2JsonPersonTemplate, stringFactory, personFactory }, //

						// LETTUCE
						{ lettuceStringTemplate, stringFactory, stringFactory }, //
						{ lettuceLongTemplate, stringFactory, longFactory }, //
						{ lettuceRawTemplate, rawFactory, rawFactory }, //
						{ lettucePersonTemplate, stringFactory, personFactory }, //
						{ lettuceXstreamStringTemplate, stringFactory, stringFactory }, //
						{ lettuceJackson2JsonPersonTemplate, stringFactory, personFactory } //
				});
	}

}
