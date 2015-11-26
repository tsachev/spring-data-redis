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

import java.util.List;

/**
 * Interface for the {@literal cluster} commands supported by Redis.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public interface RedisClusterCommands {

	/**
	 * Retrieve cluster node information such as {@literal id}, {@literal host}, {@literal port} and {@literal slots}.
	 * 
	 * @return never {@literal null}.
	 */
	Iterable<RedisClusterNode> getClusterNodes();

	/**
	 * Retrieve information about connected slaves for given master node.
	 * 
	 * @param node must not be {@literal null}.
	 * @return never {@literal null}.
	 */
	Iterable<RedisClusterNode> getClusterSlaves(RedisClusterNode master);

	/**
	 * Find the slot for a given {@code key}.
	 * 
	 * @param key must not be {@literal null}.
	 * @return
	 */
	Integer getClusterSlotForKey(byte[] key);

	/**
	 * Find the {@link RedisNode} serving given {@literal slot}.
	 * 
	 * @param slot
	 * @return
	 */
	RedisClusterNode getClusterNodeForSlot(int slot);

	/**
	 * Find the {@link RedisNode} serving given {@literal key}.
	 * 
	 * @param key must not be {@literal null}.
	 * @return
	 */
	RedisClusterNode getClusterNodeForKey(byte[] key);

	/**
	 * Get cluster information.
	 * 
	 * @return
	 */
	ClusterInfo getClusterInfo();

	/**
	 * Assign slots to given {@link RedisNode}.
	 * 
	 * @param node must not be {@literal null}.
	 * @param slots
	 */
	void addSlots(RedisClusterNode node, int... slots);

	/**
	 * Count the number of keys assigned to one {@literal slot}.
	 * 
	 * @param slot
	 * @return
	 */
	Long countKeys(int slot);

	/**
	 * Remove slots from {@link RedisNode}.
	 * 
	 * @param node must not be {@literal null}.
	 * @param slots
	 */
	void deleteSlots(RedisClusterNode node, int... slots);

	/**
	 * Remove given {@literal node} from cluster.
	 * 
	 * @param node must not be {@literal null}.
	 */
	void clusterForget(RedisClusterNode node);

	/**
	 * Add given {@literal node} to cluster.
	 * 
	 * @param node must not be {@literal null}.
	 */
	void clusterMeet(RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @param slot
	 * @param mode must not be{@literal null}.
	 */
	void clusterSetSlot(RedisClusterNode node, int slot, AddSlots mode);

	/**
	 * Get {@literal keys} served by slot.
	 * 
	 * @param slot
	 * @param count must not be {@literal null}.
	 * @return
	 */
	List<byte[]> getKeysInSlot(int slot, Integer count);

	/**
	 * Assign a {@literal slave} to given {@literal master}.
	 * 
	 * @param master must not be {@literal null}.
	 * @param slave must not be {@literal null}.
	 */
	void clusterReplicate(RedisClusterNode master, RedisClusterNode slave);

	public enum AddSlots {
		MIGRATING, IMPORTING, STABLE, NODE
	}

}
