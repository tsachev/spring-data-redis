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
package org.springframework.data.redis.connection.lettuce;

import static org.hamcrest.core.AnyOf.*;
import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.redis.connection.RedisClusterCommands.AddSlots;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisNode.NodeType;
import org.springframework.util.ObjectUtils;

import com.lambdaworks.redis.RedisAsyncConnection;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.cluster.RedisClusterClient;
import com.lambdaworks.redis.cluster.models.partitions.Partitions;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class LettuceClusterConnectionUnitTests {

	static final String KEY_1 = "key-1";
	static final byte[] KEY_1_BYTES = KEY_1.getBytes();

	static final String VALUE_1 = "value-1";
	static final byte[] VALUE_1_BYTES = VALUE_1.getBytes();

	static final String KEY_2 = "key-2";
	static final byte[] KEY_2_BYTES = KEY_2.getBytes();

	static final String KEY_3 = "key-3";
	static final byte[] KEY_3_BYTES = KEY_3.getBytes();

	static final String CLUSTER_NODE_1_HOST = "127.0.0.1";
	static final String CLUSTER_NODE_2_HOST = "127.0.0.1";
	static final String CLUSTER_NODE_3_HOST = "127.0.0.1";

	static final int CLUSTER_NODE_1_PORT = 6379;
	static final int CLUSTER_NODE_2_PORT = 6380;
	static final int CLUSTER_NODE_3_PORT = 6381;

	static final RedisClusterNode CLUSTER_NODE_1 = new RedisClusterNode(CLUSTER_NODE_1_HOST, CLUSTER_NODE_1_PORT, null)
			.withId("ef570f86c7b1a953846668debc177a3a16733420").withType(NodeType.MASTER);
	static final RedisClusterNode CLUSTER_NODE_2 = new RedisClusterNode(CLUSTER_NODE_2_HOST, CLUSTER_NODE_2_PORT, null)
			.withId("0f2ee5df45d18c50aca07228cc18b1da96fd5e84").withType(NodeType.MASTER);
	static final RedisClusterNode CLUSTER_NODE_3 = new RedisClusterNode(CLUSTER_NODE_3_HOST, CLUSTER_NODE_3_PORT, null)
			.withId("3b9b8192a874fa8f1f09dbc0ee20afab5738eee7").withType(NodeType.MASTER);

	static final RedisClusterNode UNKNOWN_CLUSTER_NODE = new RedisClusterNode("8.8.8.8", 6379, null);

	@Mock RedisClusterClient clusterMock;

	@Mock RedisAsyncConnection<byte[], byte[]> dedicatedConnectionMock;
	@Mock RedisConnection<byte[], byte[]> clusterConnection1Mock;
	@Mock RedisConnection<byte[], byte[]> clusterConnection2Mock;
	@Mock RedisConnection<byte[], byte[]> clusterConnection3Mock;

	LettuceClusterConnection connection;

	@Before
	public void setUp() {

		Partitions partitions = new Partitions();

		com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode partition1 = new com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode();
		partition1.setNodeId(CLUSTER_NODE_1.getId());
		partition1.setConnected(true);
		partition1.setUri(RedisURI.create("redis://" + CLUSTER_NODE_1_HOST + ":" + CLUSTER_NODE_1_PORT));

		com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode partition2 = new com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode();
		partition2.setNodeId(CLUSTER_NODE_2.getId());
		partition2.setConnected(true);
		partition2.setUri(RedisURI.create("redis://" + CLUSTER_NODE_2_HOST + ":" + CLUSTER_NODE_2_PORT));

		com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode partition3 = new com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode();
		partition3.setNodeId(CLUSTER_NODE_3.getId());
		partition3.setConnected(true);
		partition3.setUri(RedisURI.create("redis://" + CLUSTER_NODE_3_HOST + ":" + CLUSTER_NODE_3_PORT));

		partitions.addPartition(partition1);
		partitions.addPartition(partition2);
		partitions.addPartition(partition3);

		when(clusterMock.getPartitions()).thenReturn(partitions);

		connection = new LettuceClusterConnection(clusterMock) {

			@Override
			@SuppressWarnings("unchecked")
			public RedisConnection<byte[], byte[]> getResourceForSpecificNode(RedisClusterNode node) {

				if (ObjectUtils.nullSafeEquals(node, CLUSTER_NODE_1)) {
					return clusterConnection1Mock;
				}

				if (ObjectUtils.nullSafeEquals(node, CLUSTER_NODE_2)) {
					return clusterConnection2Mock;
				}

				if (ObjectUtils.nullSafeEquals(node, CLUSTER_NODE_3)) {
					return clusterConnection3Mock;
				}

				return null;
			}

			@Override
			protected RedisAsyncConnection<byte[], byte[]> getAsyncDedicatedConnection() {
				return dedicatedConnectionMock;
			}

			@Override
			public List<RedisClusterNode> clusterGetClusterNodes() {
				return Arrays.asList(CLUSTER_NODE_1, CLUSTER_NODE_2, CLUSTER_NODE_3);
			}
		};
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterMeetShouldSendCommandsToExistingNodesCorrectly() {

		connection.clusterMeet(UNKNOWN_CLUSTER_NODE);

		verify(clusterConnection1Mock, times(1))
				.clusterMeet(UNKNOWN_CLUSTER_NODE.getHost(), UNKNOWN_CLUSTER_NODE.getPort());
		verify(clusterConnection2Mock, times(1))
				.clusterMeet(UNKNOWN_CLUSTER_NODE.getHost(), UNKNOWN_CLUSTER_NODE.getPort());
		verify(clusterConnection3Mock, times(1))
				.clusterMeet(UNKNOWN_CLUSTER_NODE.getHost(), UNKNOWN_CLUSTER_NODE.getPort());
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test(expected = IllegalArgumentException.class)
	public void clusterMeetShouldThrowExceptionWhenNodeIsNull() {
		connection.clusterMeet(null);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterForgetShouldSendCommandsToRemainingNodesCorrectly() {

		connection.clusterForget(CLUSTER_NODE_2);

		verify(clusterConnection1Mock, times(1)).clusterForget(CLUSTER_NODE_2.getId());
		verifyZeroInteractions(clusterConnection2Mock);
		verify(clusterConnection3Mock, times(1)).clusterForget(CLUSTER_NODE_2.getId());
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterReplicateShouldSendCommandsCorrectly() {

		connection.clusterReplicate(CLUSTER_NODE_1, CLUSTER_NODE_2);

		verify(clusterConnection2Mock, times(1)).clusterReplicate(CLUSTER_NODE_1.getId());
		verifyZeroInteractions(clusterConnection1Mock);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void closeShouldNotCloseUnderlyingClusterPool() throws IOException {

		connection.close();

		verify(clusterMock, never()).shutdown();
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void isClosedShouldReturnConnectionStateCorrectly() {

		assertThat(connection.isClosed(), is(false));

		connection.close();

		assertThat(connection.isClosed(), is(true));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void keysShouldBeRunOnAllClusterNodes() {

		when(clusterConnection1Mock.keys(any(byte[].class))).thenReturn(Collections.<byte[]> emptyList());
		when(clusterConnection2Mock.keys(any(byte[].class))).thenReturn(Collections.<byte[]> emptyList());
		when(clusterConnection3Mock.keys(any(byte[].class))).thenReturn(Collections.<byte[]> emptyList());

		byte[] pattern = LettuceConverters.toBytes("*");

		connection.keys(pattern);

		verify(clusterConnection1Mock, times(1)).keys(pattern);
		verify(clusterConnection2Mock, times(1)).keys(pattern);
		verify(clusterConnection3Mock, times(1)).keys(pattern);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void keysShouldOnlyBeRunOnDedicatedNodeWhenPinned() {

		when(clusterConnection2Mock.keys(any(byte[].class))).thenReturn(Collections.<byte[]> emptyList());

		byte[] pattern = LettuceConverters.toBytes("*");

		connection.keys(CLUSTER_NODE_2, pattern);

		verify(clusterConnection1Mock, never()).keys(pattern);
		verify(clusterConnection2Mock, times(1)).keys(pattern);
		verify(clusterConnection3Mock, never()).keys(pattern);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void randomKeyShouldReturnAnyKeyFromRandomNode() {

		when(clusterConnection1Mock.randomkey()).thenReturn(KEY_1_BYTES);
		when(clusterConnection2Mock.randomkey()).thenReturn(KEY_2_BYTES);
		when(clusterConnection3Mock.randomkey()).thenReturn(KEY_3_BYTES);

		assertThat(connection.randomKey(), anyOf(is(KEY_1_BYTES), is(KEY_2_BYTES), is(KEY_3_BYTES)));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void randomKeyShouldReturnKeyWhenAvailableOnAnyNode() {

		when(clusterConnection3Mock.randomkey()).thenReturn(KEY_3_BYTES);

		for (int i = 0; i < 100; i++) {
			assertThat(connection.randomKey(), is(KEY_3_BYTES));
		}
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void randomKeyShouldReturnNullWhenNoKeysPresentOnAllNodes() {

		when(clusterConnection1Mock.randomkey()).thenReturn(null);
		when(clusterConnection2Mock.randomkey()).thenReturn(null);
		when(clusterConnection3Mock.randomkey()).thenReturn(null);

		assertThat(connection.randomKey(), nullValue());
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterSetSlotImportingShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.IMPORTING);

		verify(clusterConnection1Mock, times(1)).clusterSetSlotImporting(eq(100), eq(CLUSTER_NODE_1.getId()));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterSetSlotMigratingShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.MIGRATING);

		verify(clusterConnection1Mock, times(1)).clusterSetSlotMigrating(eq(100), eq(CLUSTER_NODE_1.getId()));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	@Ignore("Stable not available for lettuce")
	public void clusterSetSlotStableShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.STABLE);

		// verify(clusterConnection1Mock, times(1)).clusterSetSlotStable(eq(100));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterSetSlotNodeShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.NODE);

		verify(clusterConnection1Mock, times(1)).clusterSetSlotNode(eq(100), eq(CLUSTER_NODE_1.getId()));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterSetSlotShouldBeExecutedOnTargetNodeWhenNodeIdNotSet() {

		connection.clusterSetSlot(new RedisClusterNode(CLUSTER_NODE_1_HOST, CLUSTER_NODE_2_PORT), 100, AddSlots.IMPORTING);

		verify(clusterConnection2Mock, times(1)).clusterSetSlotImporting(eq(100), eq(CLUSTER_NODE_2.getId()));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test(expected = IllegalArgumentException.class)
	public void clusterSetSlotShouldThrowExceptionWhenModeIsNull() {
		connection.clusterSetSlot(CLUSTER_NODE_1, 100, null);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterDeleteSlotsShouldBeExecutedCorrectly() {

		int[] slots = new int[] { 9000, 10000 };
		connection.clusterDeleteSlots(CLUSTER_NODE_2, slots);

		verify(clusterConnection2Mock, times(1)).clusterDelSlots((int[]) anyVararg());
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test(expected = IllegalArgumentException.class)
	public void clusterDeleteSlotShouldThrowExceptionWhenNodeIsNull() {
		connection.clusterDeleteSlots(null, new int[] { 1 });
	}

}
