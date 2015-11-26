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

import java.util.Properties;
import java.util.Set;

/**
 * {@link RedisClusterConnection} allows sending commands to dedicated nodes within the cluster.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public interface RedisClusterConnection extends RedisConnection, RedisClusterCommands {

	/**
	 * @param node must not be {@literal null}.
	 * @return
	 */
	String ping(RedisClusterNode node);

	/**
	 * @see RedisServerCommands#bgReWriteAof()
	 * @param node must not be {@literal null}.
	 */
	void bgReWriteAof(RedisClusterNode node);

	/**
	 * @see RedisServerCommands#bgSave()
	 * @param node must not be {@literal null}.
	 */
	void bgSave(RedisClusterNode node);

	/**
	 * @see RedisServerCommands#lastSave()
	 * @param node must not be {@literal null}.
	 * @return
	 */
	Long lastSave(RedisClusterNode node);

	/**
	 * @see RedisServerCommands#save()
	 * @param node must not be {@literal null}.
	 */
	void save(RedisClusterNode node);

	/**
	 * @see RedisServerCommands#dbSize()
	 * @param node must not be {@literal null}.
	 * @return
	 */
	Long dbSize(RedisClusterNode node);

	/**
	 * @see RedisServerCommands#flushDb()
	 * @param node must not be {@literal null}.
	 */
	void flushDb(RedisClusterNode node);

	/**
	 * @see RedisServerCommands#flushAll()
	 * @param node must not be {@literal null}.
	 */
	void flushAll(RedisClusterNode node);

	/**
	 * @see RedisServerCommands#info()
	 * @param node
	 * @return
	 */
	Properties info(RedisClusterNode node);

	/**
	 * @see RedisKeyCommands#keys(byte[])
	 * @param node must not be {@literal null}.
	 * @param pattern must not be {@literal null}.
	 * @return
	 */
	Set<byte[]> keys(RedisClusterNode node, byte[] pattern);

	/**
	 * @see RedisKeyCommands#randomKey()
	 * @param node must not be {@literal null}.
	 * @return
	 */
	byte[] randomKey(RedisClusterNode node);

	/**
	 * @see RedisServerCommands#shutdown()
	 * @param node must not be {@literal null}.
	 */
	void shutdown(RedisClusterNode node);

}
