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
import static org.hamcrest.core.IsCollectionContaining.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisClusterCommands.AddSlots;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterNode.SlotRange;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisServerCommands.MigrateOption;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultClusterOperationsUnitTests {

	static final RedisClusterNode NODE_1 = new RedisClusterNode("127.0.0.1", 6379, null)
			.withId("d1861060fe6a534d42d8a19aeb36600e18785e04");

	static final RedisClusterNode NODE_2 = new RedisClusterNode("127.0.0.1", 6380, null)
			.withId("0f2ee5df45d18c50aca07228cc18b1da96fd5e84");

	@Mock RedisConnectionFactory connectionFactory;
	@Mock RedisClusterConnection connection;

	RedisSerializer<String> serializer;

	DefaultClusterOperations<String, String> clusterOps;

	@Before
	public void setUp() {

		when(connectionFactory.getConnection()).thenReturn(connection);

		serializer = new StringRedisSerializer();

		RedisTemplate<String, String> template = new RedisTemplate<String, String>();
		template.setConnectionFactory(connectionFactory);
		template.setValueSerializer(serializer);
		template.setKeySerializer(serializer);
		template.afterPropertiesSet();

		this.clusterOps = new DefaultClusterOperations<String, String>(template);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void keysShouldDelegateToConnectionCorrectly() {

		Set<byte[]> keys = new HashSet<byte[]>(Arrays.asList(serializer.serialize("key-1"), serializer.serialize("key-2")));
		when(connection.keys(any(RedisClusterNode.class), any(byte[].class))).thenReturn(keys);

		assertThat(clusterOps.keys(NODE_1, "*"), hasItems("key-1", "key-2"));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test(expected = IllegalArgumentException.class)
	public void keysShouldThrowExceptionWhenNodeIsNull() {
		clusterOps.keys(null, "*");
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void keysShouldReturnEmptySetWhenNoKeysAvailable() {

		when(connection.keys(any(RedisClusterNode.class), any(byte[].class))).thenReturn(null);

		assertThat(clusterOps.keys(NODE_1, "*"), notNullValue());
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void randomKeyShouldDelegateToConnection() {

		when(connection.randomKey(any(RedisClusterNode.class))).thenReturn(serializer.serialize("key-1"));

		assertThat(clusterOps.randomKey(NODE_1), is("key-1"));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test(expected = IllegalArgumentException.class)
	public void randomKeyShouldThrowExceptionWhenNodeIsNull() {
		clusterOps.randomKey(null);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void randomKeyShouldReturnNullWhenNoKeyAvailable() {

		when(connection.randomKey(any(RedisClusterNode.class))).thenReturn(null);

		assertThat(clusterOps.randomKey(NODE_1), nullValue());
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void pingShouldDelegateToConnection() {

		when(connection.ping(any(RedisClusterNode.class))).thenReturn("PONG");

		assertThat(clusterOps.ping(NODE_1), is("PONG"));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test(expected = IllegalArgumentException.class)
	public void pingShouldThrowExceptionWhenNodeIsNull() {
		clusterOps.ping(null);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void addSlotsShouldDelegateToConnection() {

		clusterOps.addSlots(NODE_1, 1, 2, 3);

		verify(connection, times(1)).clusterAddSlots(eq(NODE_1), Mockito.<int[]> anyVararg());
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test(expected = IllegalArgumentException.class)
	public void addSlotsShouldThrowExceptionWhenNodeIsNull() {
		clusterOps.addSlots(null);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void addSlotsWithRangeShouldDelegateToConnection() {

		clusterOps.addSlots(NODE_1, new SlotRange(1, 3));

		verify(connection, times(1)).clusterAddSlots(eq(NODE_1), Mockito.<int[]> anyVararg());
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test(expected = IllegalArgumentException.class)
	public void addSlotsWithRangeShouldThrowExceptionWhenRangeIsNull() {
		clusterOps.addSlots(NODE_1, (SlotRange) null);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void bgSaveShouldDelegateToConnection() {

		clusterOps.bgSave(NODE_1);

		verify(connection, times(1)).bgSave(eq(NODE_1));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test(expected = IllegalArgumentException.class)
	public void bgSaveShouldThrowExceptionWhenNodeIsNull() {
		clusterOps.bgSave(null);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void meetShouldDelegateToConnection() {

		clusterOps.meet(NODE_1);

		verify(connection, times(1)).clusterMeet(eq(NODE_1));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test(expected = IllegalArgumentException.class)
	public void meetShouldThrowExceptionWhenNodeIsNull() {
		clusterOps.meet(null);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void forgetShouldDelegateToConnection() {

		clusterOps.forget(NODE_1);

		verify(connection, times(1)).clusterForget(eq(NODE_1));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test(expected = IllegalArgumentException.class)
	public void forgetShouldThrowExceptionWhenNodeIsNull() {
		clusterOps.forget(null);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void flushDbShouldDelegateToConnection() {

		clusterOps.flushDb(NODE_1);

		verify(connection, times(1)).flushDb(eq(NODE_1));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test(expected = IllegalArgumentException.class)
	public void flushDbShouldThrowExceptionWhenNodeIsNull() {
		clusterOps.flushDb(null);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void getSlavesShouldDelegateToConnection() {

		clusterOps.getSlaves(NODE_1);

		verify(connection, times(1)).clusterGetSlaves(eq(NODE_1));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test(expected = IllegalArgumentException.class)
	public void getSlavesShouldThrowExceptionWhenNodeIsNull() {
		clusterOps.getSlaves(null);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void saveShouldDelegateToConnection() {

		clusterOps.save(NODE_1);

		verify(connection, times(1)).save(eq(NODE_1));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test(expected = IllegalArgumentException.class)
	public void saveShouldThrowExceptionWhenNodeIsNull() {
		clusterOps.save(null);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void shutdownShouldDelegateToConnection() {

		clusterOps.shutdown(NODE_1);

		verify(connection, times(1)).shutdown(eq(NODE_1));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test(expected = IllegalArgumentException.class)
	public void shutdownShouldThrowExceptionWhenNodeIsNull() {
		clusterOps.shutdown(null);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void executeShouldDelegateToConnection() {

		final byte[] key = serializer.serialize("foo");
		clusterOps.execute(new RedisClusterCallback<String>() {

			@Override
			public String doInRedis(RedisClusterConnection connection) throws DataAccessException {
				return serializer.deserialize(connection.get(key));
			}
		});

		verify(connection, times(1)).get(eq(key));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test(expected = IllegalArgumentException.class)
	public void executeShouldThrowExceptionWhenCallbackIsNull() {
		clusterOps.execute(null);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void reshardShouldExecuteCommandsCorrectly() {

		byte[] key = "foo".getBytes();
		when(connection.clusterGetKeysInSlot(eq(100), anyInt())).thenReturn(Collections.singletonList(key));
		clusterOps.reshard(NODE_1, 100, NODE_2);

		verify(connection, times(1)).clusterSetSlot(eq(NODE_2), eq(100), eq(AddSlots.IMPORTING));
		verify(connection, times(1)).clusterSetSlot(eq(NODE_1), eq(100), eq(AddSlots.MIGRATING));
		verify(connection, times(1)).clusterGetKeysInSlot(eq(100), anyInt());
		verify(connection, times(1)).migrate(any(byte[].class), eq(NODE_1), eq(0), eq(MigrateOption.COPY));
		verify(connection, times(1)).clusterSetSlot(eq(NODE_2), eq(100), eq(AddSlots.NODE));

	}
}
