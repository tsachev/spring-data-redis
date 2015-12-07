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
package org.springframework.data.redis.connection.jedis;

import static org.hamcrest.core.Is.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.redis.connection.ClusterInfo;
import org.springframework.data.redis.connection.RedisClusterCommands.AddSlots;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisNode.NodeType;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class JedisClusterConnectionUnitTests {

	static final String CLUSTER_NODE_HOST = "127.0.0.1";

	static final int CLUSTER_NODE_1_PORT = 7379;
	static final int CLUSTER_NODE_2_PORT = 7380;
	static final int CLUSTER_NODE_3_PORT = 7381;

	static final RedisClusterNode CLUSTER_NODE_1 = new RedisClusterNode(CLUSTER_NODE_HOST, CLUSTER_NODE_1_PORT, null)
			.withId("ef570f86c7b1a953846668debc177a3a16733420").withType(NodeType.MASTER);
	static final RedisClusterNode CLUSTER_NODE_2 = new RedisClusterNode(CLUSTER_NODE_HOST, CLUSTER_NODE_2_PORT, null)
			.withId("0f2ee5df45d18c50aca07228cc18b1da96fd5e84").withType(NodeType.MASTER);
	static final RedisClusterNode CLUSTER_NODE_3 = new RedisClusterNode(CLUSTER_NODE_HOST, CLUSTER_NODE_3_PORT, null)
			.withId("3b9b8192a874fa8f1f09dbc0ee20afab5738eee7").withType(NodeType.MASTER);

	static final RedisClusterNode UNKNOWN_CLUSTER_NODE = new RedisClusterNode("8.8.8.8", 6379, null);

	private static final String CLUSTER_NODES_RESPONSE = "" //
			+ "ef570f86c7b1a953846668debc177a3a16733420 " + CLUSTER_NODE_HOST + ":"
			+ CLUSTER_NODE_1_PORT
			+ " myself,master - 0 0 1 connected 0-5460"
			+ System.getProperty("line.separator")
			+ "0f2ee5df45d18c50aca07228cc18b1da96fd5e84 " + CLUSTER_NODE_HOST + ":"
			+ CLUSTER_NODE_2_PORT
			+ " master - 0 1427718161587 2 connected 5461-10922"
			+ System.getProperty("line.separator")
			+ "3b9b8192a874fa8f1f09dbc0ee20afab5738eee7 " + CLUSTER_NODE_HOST + ":"
			+ CLUSTER_NODE_3_PORT
			+ " master - 0 1427718161587 3 connected 10923-16383";

	static final String CLUSTER_INFO_RESPONSE = "cluster_state:ok" + System.getProperty("line.separator")
			+ "cluster_slots_assigned:16384" + System.getProperty("line.separator") + "cluster_slots_ok:16384"
			+ System.getProperty("line.separator") + "cluster_slots_pfail:0" + System.getProperty("line.separator")
			+ "cluster_slots_fail:0" + System.getProperty("line.separator") + "cluster_known_nodes:4"
			+ System.getProperty("line.separator") + "cluster_size:3" + System.getProperty("line.separator")
			+ "cluster_current_epoch:30" + System.getProperty("line.separator") + "cluster_my_epoch:2"
			+ System.getProperty("line.separator") + "cluster_stats_messages_sent:2560260"
			+ System.getProperty("line.separator") + "cluster_stats_messages_received:2560086";

	JedisClusterConnection connection;

	@Mock JedisCluster clusterMock;

	@Mock JedisPool node1PoolMock;
	@Mock JedisPool node2PoolMock;
	@Mock JedisPool node3PoolMock;

	@Mock Jedis con1Mock;
	@Mock Jedis con2Mock;
	@Mock Jedis con3Mock;

	@Before
	public void setUp() {

		Map<String, JedisPool> nodes = new LinkedHashMap<String, JedisPool>(3);
		nodes.put(CLUSTER_NODE_HOST + ":" + CLUSTER_NODE_1_PORT, node1PoolMock);
		nodes.put(CLUSTER_NODE_HOST + ":" + CLUSTER_NODE_2_PORT, node2PoolMock);
		nodes.put(CLUSTER_NODE_HOST + ":" + CLUSTER_NODE_3_PORT, node3PoolMock);

		when(clusterMock.getClusterNodes()).thenReturn(nodes);
		when(node1PoolMock.getResource()).thenReturn(con1Mock);
		when(node2PoolMock.getResource()).thenReturn(con2Mock);
		when(node3PoolMock.getResource()).thenReturn(con3Mock);

		when(con1Mock.clusterNodes()).thenReturn(CLUSTER_NODES_RESPONSE);

		connection = new JedisClusterConnection(clusterMock);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterMeetShouldSendCommandsToExistingNodesCorrectly() {

		connection.clusterMeet(UNKNOWN_CLUSTER_NODE);

		verify(con1Mock, times(1)).clusterMeet(UNKNOWN_CLUSTER_NODE.getHost(), UNKNOWN_CLUSTER_NODE.getPort());
		verify(con2Mock, times(1)).clusterMeet(UNKNOWN_CLUSTER_NODE.getHost(), UNKNOWN_CLUSTER_NODE.getPort());
		verify(con2Mock, times(1)).clusterMeet(UNKNOWN_CLUSTER_NODE.getHost(), UNKNOWN_CLUSTER_NODE.getPort());
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

		verify(con1Mock, times(1)).clusterForget(CLUSTER_NODE_2.getId());
		verifyZeroInteractions(con2Mock);
		verify(con3Mock, times(1)).clusterForget(CLUSTER_NODE_2.getId());
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterReplicateShouldSendCommandsCorrectly() {

		connection.clusterReplicate(CLUSTER_NODE_1, CLUSTER_NODE_2);

		verify(con2Mock, times(1)).clusterReplicate(CLUSTER_NODE_1.getId());
		verifyZeroInteractions(con1Mock);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void closeShouldNotCloseUnderlyingClusterPool() throws IOException {

		connection.close();

		verify(clusterMock, never()).close();
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
	public void clusterInfoShouldBeReturnedCorrectly() {

		when(con1Mock.clusterInfo()).thenReturn(CLUSTER_INFO_RESPONSE);
		when(con2Mock.clusterInfo()).thenReturn(CLUSTER_INFO_RESPONSE);
		when(con3Mock.clusterInfo()).thenReturn(CLUSTER_INFO_RESPONSE);

		ClusterInfo p = connection.clusterGetClusterInfo();
		assertThat(p.getSlotsAssigned(), is(16384L));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterSetSlotImportingShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.IMPORTING);

		verify(con1Mock, times(1)).clusterSetSlotImporting(eq(100), eq(CLUSTER_NODE_1.getId()));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterSetSlotMigratingShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.MIGRATING);

		verify(con1Mock, times(1)).clusterSetSlotMigrating(eq(100), eq(CLUSTER_NODE_1.getId()));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterSetSlotStableShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.STABLE);

		verify(con1Mock, times(1)).clusterSetSlotStable(eq(100));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterSetSlotNodeShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.NODE);

		verify(con1Mock, times(1)).clusterSetSlotNode(eq(100), eq(CLUSTER_NODE_1.getId()));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterSetSlotShouldBeExecutedOnTargetNodeWhenNodeIdNotSet() {

		connection.clusterSetSlot(new RedisClusterNode(CLUSTER_NODE_HOST, CLUSTER_NODE_2_PORT), 100, AddSlots.IMPORTING);

		verify(con2Mock, times(1)).clusterSetSlotImporting(eq(100), eq(CLUSTER_NODE_2.getId()));
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

		verify(con2Mock, times(1)).clusterDelSlots((int[]) anyVararg());
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test(expected = IllegalArgumentException.class)
	public void clusterDeleteSlotShouldThrowExceptionWhenNodeIsNull() {
		connection.clusterDeleteSlots(null, new int[] { 1 });
	}

}
