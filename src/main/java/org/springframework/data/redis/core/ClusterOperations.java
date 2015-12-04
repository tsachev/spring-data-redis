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

import java.util.Collection;
import java.util.Set;

import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterNode.SlotRange;

/**
 * Redis operations for cluster specific operations.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public interface ClusterOperations<K, V> {

	/**
	 * Get all keys located at given node.
	 * 
	 * @param node must not be {@literal null}.
	 * @param pattern
	 * @return never {@literal null}.
	 */
	Set<K> keys(RedisClusterNode node, K pattern);

	/**
	 * Ping the given node;
	 * 
	 * @param node must not be {@literal null}.
	 * @return
	 */
	String ping(RedisClusterNode node);

	/**
	 * Get a random key from the range served by the given node.
	 * 
	 * @param node must not be {@literal null}.
	 * @return
	 */
	K randomKey(RedisClusterNode node);

	/**
	 * Add slots to given node;
	 * 
	 * @param node must not be {@literal null}.
	 * @param slots must not be {@literal null}.
	 */
	void addSlots(RedisClusterNode node, int... slots);

	/**
	 * Add slots in {@link SlotRange} to given node.
	 * 
	 * @param node must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 */
	void addSlots(RedisClusterNode node, SlotRange range);

	/**
	 * @param node
	 */
	void bgReWriteAof(RedisClusterNode node);

	/**
	 * @param node
	 */
	void bgSave(RedisClusterNode node);

	/**
	 * @param node
	 */
	void meet(RedisClusterNode node);

	/**
	 * @param node
	 */
	void forget(RedisClusterNode node);

	/**
	 * @param node
	 */
	void flushDb(RedisClusterNode node);

	/**
	 * @param node
	 * @return
	 */
	Collection<RedisClusterNode> getSlaves(RedisClusterNode node);

	/**
	 * @param node
	 */
	void save(RedisClusterNode node);

	/**
	 * @param node
	 */
	void shutdown(RedisClusterNode node);
}
