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
import static org.junit.Assert.*;
import static org.junit.Assume.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConverters;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.OxmSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.oxm.xstream.XStreamMarshaller;

import com.lambdaworks.redis.RedisClient;

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

		RedisClient client = new RedisClient("127.0.0.1", 7379);
		String mode = LettuceConverters.toProperties(client.connect().info()).getProperty("redis_mode");
		client.shutdown(0, 0, TimeUnit.MILLISECONDS);

		Assume.assumeThat(mode, is("cluster"));
	}

	@Test
	@Override
	public void testGetExpireMillisNotSupported() {

		final K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		assumeTrue(key1 instanceof String && value1 instanceof String);

		LettuceConnectionFactory lettuceConnectionFactory = new LettuceConnectionFactory(new RedisClusterConfiguration(
				CLUSTER_NODES));
		lettuceConnectionFactory.afterPropertiesSet();

		try {

			final StringRedisTemplate template2 = new StringRedisTemplate(lettuceConnectionFactory);
			template2.boundValueOps((String) key1).set((String) value1);
			template2.expire((String) key1, 5, TimeUnit.SECONDS);
			long expire = template2.getExpire((String) key1, TimeUnit.MILLISECONDS);
			// we should still get expire in milliseconds if requested
			assertTrue(expire > 1000 && expire <= 5000);
		} finally {
			lettuceConnectionFactory.destroy();
		}
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

		LettuceConnectionFactory lettuceConnectionFactory = new LettuceConnectionFactory(new RedisClusterConfiguration(
				CLUSTER_NODES));

		lettuceConnectionFactory.afterPropertiesSet();

		RedisClusterTemplate<String, String> stringTemplate = new RedisClusterTemplate<String, String>();
		stringTemplate.setDefaultSerializer(new StringRedisSerializer());
		stringTemplate.setConnectionFactory(lettuceConnectionFactory);
		stringTemplate.afterPropertiesSet();

		RedisClusterTemplate<String, Long> longTemplate = new RedisClusterTemplate<String, Long>();
		longTemplate.setKeySerializer(new StringRedisSerializer());
		longTemplate.setValueSerializer(new GenericToStringSerializer<Long>(Long.class));
		longTemplate.setConnectionFactory(lettuceConnectionFactory);
		longTemplate.afterPropertiesSet();

		RedisClusterTemplate<String, Double> doubleTemplate = new RedisClusterTemplate<String, Double>();
		doubleTemplate.setKeySerializer(new StringRedisSerializer());
		doubleTemplate.setValueSerializer(new GenericToStringSerializer<Double>(Double.class));
		doubleTemplate.setConnectionFactory(lettuceConnectionFactory);
		doubleTemplate.afterPropertiesSet();

		RedisClusterTemplate<byte[], byte[]> rawTemplate = new RedisClusterTemplate<byte[], byte[]>();
		rawTemplate.setEnableDefaultSerializer(false);
		rawTemplate.setConnectionFactory(lettuceConnectionFactory);
		rawTemplate.afterPropertiesSet();

		RedisClusterTemplate<String, Person> personTemplate = new RedisClusterTemplate<String, Person>();
		personTemplate.setConnectionFactory(lettuceConnectionFactory);
		personTemplate.afterPropertiesSet();

		OxmSerializer serializer = new OxmSerializer(xstream, xstream);
		RedisClusterTemplate<String, String> xstreamStringTemplate = new RedisClusterTemplate<String, String>();
		xstreamStringTemplate.setConnectionFactory(lettuceConnectionFactory);
		xstreamStringTemplate.setDefaultSerializer(serializer);
		xstreamStringTemplate.afterPropertiesSet();

		RedisClusterTemplate<String, Person> xstreamPersonTemplate = new RedisClusterTemplate<String, Person>();
		xstreamPersonTemplate.setConnectionFactory(lettuceConnectionFactory);
		xstreamPersonTemplate.setValueSerializer(serializer);
		xstreamPersonTemplate.afterPropertiesSet();

		JacksonJsonRedisSerializer<Person> jacksonJsonSerializer = new JacksonJsonRedisSerializer<Person>(Person.class);
		RedisClusterTemplate<String, Person> jsonPersonTemplate = new RedisClusterTemplate<String, Person>();
		jsonPersonTemplate.setConnectionFactory(lettuceConnectionFactory);
		jsonPersonTemplate.setValueSerializer(jacksonJsonSerializer);
		jsonPersonTemplate.afterPropertiesSet();

		Jackson2JsonRedisSerializer<Person> jackson2JsonSerializer = new Jackson2JsonRedisSerializer<Person>(Person.class);
		RedisClusterTemplate<String, Person> jackson2JsonPersonTemplate = new RedisClusterTemplate<String, Person>();
		jackson2JsonPersonTemplate.setConnectionFactory(lettuceConnectionFactory);
		jackson2JsonPersonTemplate.setValueSerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplate.afterPropertiesSet();

		return Arrays.asList(new Object[][] { //
				{ stringTemplate, stringFactory, stringFactory }, //
						{ longTemplate, stringFactory, longFactory }, //
						{ doubleTemplate, stringFactory, doubleFactory }, //
						{ rawTemplate, rawFactory, rawFactory }, //
						{ personTemplate, stringFactory, personFactory }, //
						{ xstreamStringTemplate, stringFactory, stringFactory }, //
						{ xstreamPersonTemplate, stringFactory, personFactory }, //
						{ jsonPersonTemplate, stringFactory, personFactory }, //
						{ jackson2JsonPersonTemplate, stringFactory, personFactory } //
				});
	}

}
