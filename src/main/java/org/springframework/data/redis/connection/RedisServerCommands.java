/*
 * Copyright 2011-2014 the original author or authors.
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
import java.util.Properties;

import org.springframework.data.redis.core.types.RedisClientInfo;

/**
 * Server-specific commands supported by Redis.
 * 
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Thomas Darimont
 */
public interface RedisServerCommands {

	public enum ShutdownOption {
		SAVE, NOSAVE;
	}

	/**
	 * @since 1.7
	 */
	public enum MigrateOption {
		COPY, REPLACE
	}

	/**
	 * Start an {@literal Append Only File} rewrite process on server.
	 * <p>
	 * See http://redis.io/commands/bgrewriteaof
	 * 
	 * @deprecated As of 1.3, use {@link #bgReWriteAof}.
	 */
	@Deprecated
	void bgWriteAof();

	/**
	 * Start an {@literal Append Only File} rewrite process on server.
	 * <p>
	 * See http://redis.io/commands/bgrewriteaof
	 * 
	 * @since 1.3
	 */
	void bgReWriteAof();

	/**
	 * Start background saving of db on server.
	 * <p>
	 * See http://redis.io/commands/bgsave
	 */
	void bgSave();

	/**
	 * Get time of last {@link #bgSave()} operation in seconds.
	 * <p>
	 * See http://redis.io/commands/lastsave
	 * 
	 * @return
	 */
	Long lastSave();

	/**
	 * Synchronous save current db snapshot on server.
	 * <p>
	 * See http://redis.io/commands/save
	 */
	void save();

	/**
	 * Get the total number of available keys in currently selected database.
	 * <p>
	 * See http://redis.io/commands/dbsize
	 * 
	 * @return
	 */
	Long dbSize();

	/**
	 * Delete all keys of the currently selected database.
	 * <p>
	 * See http://redis.io/commands/flushdb
	 */
	void flushDb();

	/**
	 * Delete all <b>all keys</b> from <b>all databases</b>.
	 * <p>
	 * See http://redis.io/commands/flushall
	 */
	void flushAll();

	/**
	 * Load {@literal default} server information like
	 * <ul>
	 * <li>mempory</li>
	 * <li>cpu utilization</li>
	 * <li>replication</li>
	 * </ul>
	 * <p>
	 * See http://redis.io/commands/info
	 * 
	 * @return
	 */
	Properties info();

	/**
	 * Load server information for given {@code selection}.
	 * <p>
	 * See http://redis.io/commands/info
	 * 
	 * @return
	 */
	Properties info(String section);

	/**
	 * Shutdown server.
	 * <p>
	 * See http://redis.io/commands/shutdown
	 */
	void shutdown();

	/**
	 * Shutdown server.
	 * <p>
	 * See http://redis.io/commands/shutdown
	 * 
	 * @since 1.3
	 */
	void shutdown(ShutdownOption option);

	/**
	 * Load configuration parameters for given {@code pattern} from server.
	 * <p>
	 * See http://redis.io/commands/config-get
	 * 
	 * @param pattern
	 * @return
	 */
	List<String> getConfig(String pattern);

	/**
	 * Set server configuration for {@code param} to {@code value}.
	 * <p>
	 * See http://redis.io/commands/config-set
	 * 
	 * @param param
	 * @param value
	 */
	void setConfig(String param, String value);

	/**
	 * Reset statistic counters on server. <br>
	 * Counters can be retrieved using {@link #info()}.
	 * <p>
	 * See http://redis.io/commands/config-resetstat
	 */
	void resetConfigStats();

	/**
	 * Request server timestamp using {@code TIME} command.
	 * 
	 * @return current server time in milliseconds.
	 * @since 1.1
	 */
	Long time();

	/**
	 * Closes a given client connection identified by {@literal host:port}.
	 * 
	 * @param host of connection to close.
	 * @param port of connection to close
	 * @since 1.3
	 */
	void killClient(String host, int port);

	/**
	 * Assign given name to current connection.
	 * 
	 * @param name
	 * @since 1.3
	 */
	void setClientName(byte[] name);

	/**
	 * Returns the name of the current connection.
	 * <p>
	 * See http://redis.io/commands/client-getname
	 * 
	 * @return
	 * @since 1.3
	 */
	String getClientName();

	/**
	 * Request information and statistics about connected clients.
	 * <p>
	 * See http://redis.io/commands/client-list
	 * 
	 * @return {@link List} of {@link RedisClientInfo} objects.
	 * @since 1.3
	 */
	List<RedisClientInfo> getClientList();

	/**
	 * Change redis replication setting to new master.
	 * <p>
	 * See http://redis.io/commands/slaveof
	 * 
	 * @param host
	 * @param port
	 * @since 1.3
	 */
	void slaveOf(String host, int port);

	/**
	 * Change server into master.
	 * <p>
	 * See http://redis.io/commands/slaveof
	 * 
	 * @since 1.3
	 */
	void slaveOfNoOne();

	/**
	 * @param key must not be {@literal null}.
	 * @param target must not be {@literal null}.
	 * @param dbIndex
	 * @param option can be {@literal null}. Defaulted to {@link MigrateOption#COPY}.
	 * @since 1.7
	 */
	void migrate(byte[] key, RedisNode target, int dbIndex, MigrateOption option);

	/**
	 * @param key must not be {@literal null}.
	 * @param target must not be {@literal null}.
	 * @param dbIndex
	 * @param option can be {@literal null}. Defaulted to {@link MigrateOption#COPY}.
	 * @param timeout
	 * @since 1.7
	 */
	void migrate(byte[] key, RedisNode target, int dbIndex, MigrateOption option, long timeout);
}
