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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.springframework.data.redis.ClusterStateFailureExeption;
import org.springframework.util.Assert;

/**
 * {@link ClusterTopology} holds snapshot like information about {@link RedisClusterNode}s.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public class ClusterTopology {

	private final Set<RedisClusterNode> nodes;

	/**
	 * Creates new instance of {@link ClusterTopology}.
	 * 
	 * @param nodes can be {@literal null}.
	 */
	public ClusterTopology(Set<RedisClusterNode> nodes) {
		this.nodes = nodes != null ? nodes : Collections.<RedisClusterNode> emptySet();
	}

	/**
	 * Get all {@link RedisClusterNode}s.
	 * 
	 * @return never {@literal null}.
	 */
	public Set<RedisClusterNode> getNodes() {
		return Collections.unmodifiableSet(nodes);
	}

	/**
	 * Get all master nodes in cluster.
	 * 
	 * @return never {@literal null}.
	 */
	public Set<RedisClusterNode> getMasterNodes() {

		Set<RedisClusterNode> masterNodes = new LinkedHashSet<RedisClusterNode>(nodes.size());
		for (RedisClusterNode node : nodes) {
			if (node.isMaster()) {
				masterNodes.add(node);
			}
		}
		return masterNodes;
	}

	/**
	 * Get the {@link RedisClusterNode}s (master and slave) serving s specific slot.
	 * 
	 * @param slot
	 * @return never {@literal null}.
	 */
	public Set<RedisClusterNode> getSlotServingNodes(int slot) {

		Set<RedisClusterNode> slotServingNodes = new LinkedHashSet<RedisClusterNode>(nodes.size());
		for (RedisClusterNode node : nodes) {
			if (node.servesSlot(slot)) {
				slotServingNodes.add(node);
			}
		}
		return slotServingNodes;
	}

	/**
	 * Get the {@link RedisClusterNode} that is the current master serving the given key.
	 * 
	 * @param key must not be {@literal null}.
	 * @return
	 * @throws ClusterStateFailureExeption
	 */
	public RedisClusterNode getKeyServingMasterNode(byte[] key) {

		Assert.notNull(key, "Key for node lookup must not be null!");

		int slot = ClusterSlotHashUtil.calculateSlot(key);
		for (RedisClusterNode node : nodes) {
			if (node.isMaster() && node.servesSlot(slot)) {
				return node;
			}
		}
		throw new ClusterStateFailureExeption(String.format("Could not find master node serving slot %s for key '%s',",
				slot, key));
	}

	/**
	 * @param key must not be {@literal null}.
	 * @return {@literal null}.
	 */
	public Set<RedisClusterNode> getKeyServingNodes(byte[] key) {

		Assert.notNull(key, "Key must not be null for Cluster Node lookup.");
		return getSlotServingNodes(ClusterSlotHashUtil.calculateSlot(key));
	}
}
