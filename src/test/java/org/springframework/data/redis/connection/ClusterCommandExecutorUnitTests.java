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
package org.springframework.data.redis.connection;

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsCollectionContaining.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;

import org.hamcrest.core.IsInstanceOf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.PassThroughExceptionTranslationStrategy;
import org.springframework.data.redis.connection.ClusterCommandExecutor.ClusterCommandCallback;
import org.springframework.data.redis.connection.RedisClusterNode.SlotRange;
import org.springframework.data.redis.connection.RedisNode.NodeType;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class ClusterCommandExecutorUnitTests {

	static final String CLUSTER_NODE_1_HOST = "127.0.0.1";
	static final String CLUSTER_NODE_2_HOST = "127.0.0.1";
	static final String CLUSTER_NODE_3_HOST = "127.0.0.1";

	static final int CLUSTER_NODE_1_PORT = 7379;
	static final int CLUSTER_NODE_2_PORT = 7380;
	static final int CLUSTER_NODE_3_PORT = 7381;

	static final RedisClusterNode CLUSTER_NODE_1 = new RedisClusterNode(CLUSTER_NODE_1_HOST, CLUSTER_NODE_1_PORT,
			new SlotRange(0, 5460)).withId("ef570f86c7b1a953846668debc177a3a16733420").withType(NodeType.MASTER);
	static final RedisClusterNode CLUSTER_NODE_2 = new RedisClusterNode(CLUSTER_NODE_2_HOST, CLUSTER_NODE_2_PORT,
			new SlotRange(5461, 10922)).withId("0f2ee5df45d18c50aca07228cc18b1da96fd5e84").withType(NodeType.MASTER);;
	static final RedisClusterNode CLUSTER_NODE_3 = new RedisClusterNode(CLUSTER_NODE_3_HOST, CLUSTER_NODE_3_PORT,
			new SlotRange(10923, 16383)).withId("3b9b8192a874fa8f1f09dbc0ee20afab5738eee7").withType(NodeType.MASTER);;

	static final RedisClusterNode UNKNOWN_CLUSTER_NODE = new RedisClusterNode("8.8.8.8", 7379, null);

	private ClusterCommandExecutor executor;

	private static final ConnectionCommandCallback<String> COMMAND_CALLBACK = new ConnectionCommandCallback<String>() {

		@Override
		public String doInCluster(Connection connection) {
			return connection.theWheelWeavesAsTheWheelWills();
		}
	};

	private static final Converter<Exception, DataAccessException> exceptionConverter = new Converter<Exception, DataAccessException>() {

		@Override
		public DataAccessException convert(Exception source) {
			return new InvalidDataAccessApiUsageException(source.getMessage(), source);
		}

	};

	@Mock Connection con1;
	@Mock Connection con2;
	@Mock Connection con3;

	@Before
	public void setUp() {
		this.executor = new ClusterCommandExecutor(new MockClusterNodeProvider(), new MockClusterResourceProvider(),
				new PassThroughExceptionTranslationStrategy(exceptionConverter));
	}

	@After
	public void tearDown() throws Exception {
		this.executor.destroy();
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void runCommandOnSingleNodeShouldBeExecutedCorrectly() {

		executor.runCommandOnSingleNode(COMMAND_CALLBACK, CLUSTER_NODE_2);

		verify(con2, times(1)).theWheelWeavesAsTheWheelWills();
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test(expected = IllegalArgumentException.class)
	public void runCommandOnSingleNodeShouldThrowExceptionWhenNodeIsNull() {
		executor.runCommandOnSingleNode(COMMAND_CALLBACK, null);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test(expected = IllegalArgumentException.class)
	public void runCommandOnSingleNodeShouldThrowExceptionWhenCommandCallbackIsNull() {
		executor.runCommandOnSingleNode(null, CLUSTER_NODE_1);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test(expected = IllegalArgumentException.class)
	public void runCommandOnSingleNodeShouldThrowExceptionWhenNodeIsUnknown() {
		executor.runCommandOnSingleNode(COMMAND_CALLBACK, UNKNOWN_CLUSTER_NODE);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void runCommandAsyncOnNodesShouldExecuteCommandOnGivenNodes() {

		ClusterCommandExecutor executor = new ClusterCommandExecutor(new MockClusterNodeProvider(),
				new MockClusterResourceProvider(), new PassThroughExceptionTranslationStrategy(exceptionConverter),
				new ConcurrentTaskExecutor(new SyncTaskExecutor()));

		executor.runCommandAsyncOnNodes(COMMAND_CALLBACK, Arrays.asList(CLUSTER_NODE_1, CLUSTER_NODE_2));

		verify(con1, times(1)).theWheelWeavesAsTheWheelWills();
		verify(con2, times(1)).theWheelWeavesAsTheWheelWills();
		verify(con3, never()).theWheelWeavesAsTheWheelWills();
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void runCommandOnAllNodesShouldExecuteCommandOnEveryKnwonClusterNode() {

		ClusterCommandExecutor executor = new ClusterCommandExecutor(new MockClusterNodeProvider(),
				new MockClusterResourceProvider(), new PassThroughExceptionTranslationStrategy(exceptionConverter),
				new ConcurrentTaskExecutor(new SyncTaskExecutor()));

		executor.runCommandOnAllNodes(COMMAND_CALLBACK);

		verify(con1, times(1)).theWheelWeavesAsTheWheelWills();
		verify(con2, times(1)).theWheelWeavesAsTheWheelWills();
		verify(con3, times(1)).theWheelWeavesAsTheWheelWills();
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void runCommandAsyncOnNodesShouldCompleteAndCollectErrorsOfAllNodes() {

		when(con1.theWheelWeavesAsTheWheelWills()).thenReturn("rand");
		when(con2.theWheelWeavesAsTheWheelWills()).thenThrow(new IllegalStateException("(error) mat lost the dagger..."));
		when(con3.theWheelWeavesAsTheWheelWills()).thenReturn("perrin");

		try {
			executor.runCommandOnAllNodes(COMMAND_CALLBACK);
		} catch (ClusterCommandExecutionFailureException e) {

			assertThat(e.getCauses().size(), is(1));
			assertThat(e.getCauses().iterator().next(), IsInstanceOf.instanceOf(DataAccessException.class));
		}

		verify(con1, times(1)).theWheelWeavesAsTheWheelWills();
		verify(con2, times(1)).theWheelWeavesAsTheWheelWills();
		verify(con3, times(1)).theWheelWeavesAsTheWheelWills();
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void runCommandAsyncOnNodesShouldCollectResultsCorrectly() {

		when(con1.theWheelWeavesAsTheWheelWills()).thenReturn("rand");
		when(con2.theWheelWeavesAsTheWheelWills()).thenReturn("mat");
		when(con3.theWheelWeavesAsTheWheelWills()).thenReturn("perrin");

		Map<RedisClusterNode, String> result = executor.runCommandOnAllNodes(COMMAND_CALLBACK);

		assertThat(result.keySet(), hasItems(CLUSTER_NODE_1, CLUSTER_NODE_2, CLUSTER_NODE_3));
		assertThat(result.values(), hasItems("rand", "mat", "perrin"));
	}

	class MockClusterNodeProvider implements ClusterTopologyProvider {

		@Override
		public ClusterTopology getTopology() {
			return new ClusterTopology(new LinkedHashSet<RedisClusterNode>(Arrays.asList(CLUSTER_NODE_1, CLUSTER_NODE_2,
					CLUSTER_NODE_3)));
		}

	}

	class MockClusterResourceProvider implements ClusterNodeResourceProvider {

		@Override
		public Connection getResourceForSpecificNode(RedisClusterNode node) {

			if (CLUSTER_NODE_1.equals(node)) {
				return con1;
			}
			if (CLUSTER_NODE_2.equals(node)) {
				return con2;
			}
			if (CLUSTER_NODE_3.equals(node)) {
				return con3;
			}

			return null;
		}

		@Override
		public void returnResourceForSpecificNode(RedisClusterNode node, Object resource) {
			// TODO Auto-generated method stub
		}

	}

	static interface ConnectionCommandCallback<S> extends ClusterCommandCallback<Connection, S> {

	}

	static interface Connection {
		String theWheelWeavesAsTheWheelWills();
	}

}
