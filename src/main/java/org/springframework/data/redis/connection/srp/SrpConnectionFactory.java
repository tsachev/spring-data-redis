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

package org.springframework.data.redis.connection.srp;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.PassThroughExceptionTranslationStrategy;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisSentinelConnection;

/**
 * Connection factory creating <a href="http://github.com/spullara/redis-protocol">Redis Protocol</a> based connections.
 * 
 * @author Costin Leau
 * @author Thomas Darimont
 */
public class SrpConnectionFactory implements InitializingBean, DisposableBean, RedisConnectionFactory {

	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new PassThroughExceptionTranslationStrategy(
			SrpConverters.exceptionConverter());

	private String hostName = "localhost";
	private int port = 6379;
	private BlockingQueue<SrpConnection> trackedConnections = new ArrayBlockingQueue<SrpConnection>(50);
	private boolean convertPipelineAndTxResults = true;
	private String password;

	/**
	 * Constructs a new <code>SRedisConnectionFactory</code> instance with default settings.
	 */
	public SrpConnectionFactory() {}

	/**
	 * Constructs a new <code>SRedisConnectionFactory</code> instance with default settings.
	 */
	public SrpConnectionFactory(String host, int port) {
		this.hostName = host;
		this.port = port;
	}

	public void afterPropertiesSet() {}

	public void destroy() {
		SrpConnection con;
		do {
			con = trackedConnections.poll();
			if (con != null && !con.isClosed()) {
				try {
					con.close();
				} catch (Exception ex) {
					// ignore
				}
			}
		} while (con != null);
	}

	public RedisConnection getConnection() {
		SrpConnection connection = password != null ? new SrpConnection(hostName, port, password, trackedConnections)
				: new SrpConnection(hostName, port, trackedConnections);
		connection.setConvertPipelineAndTxResults(convertPipelineAndTxResults);
		return connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnectionFactory#getClusterConnection()
	 */
	@Override
	public RedisClusterConnection getClusterConnection() {
		throw new UnsupportedOperationException("Srp does not support Redis Cluster.");
	}

	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return EXCEPTION_TRANSLATION.translate(ex);
	}

	/**
	 * Returns the current host.
	 * 
	 * @return the host
	 */
	public String getHostName() {
		return hostName;
	}

	/**
	 * Sets the host.
	 * 
	 * @param host the host to set
	 */
	public void setHostName(String host) {
		this.hostName = host;
	}

	/**
	 * Returns the current port.
	 * 
	 * @return the port
	 */
	public int getPort() {
		return port;
	}

	/**
	 * Sets the port.
	 * 
	 * @param port the port to set
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * Returns the password used for authenticating with the Redis server.
	 * 
	 * @return password for authentication
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * Sets the password used for authenticating with the Redis server.
	 * 
	 * @param password the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * Specifies if pipelined results should be converted to the expected data type. If false, results of
	 * {@link SrpConnection#closePipeline()} and {@link SrpConnection#exec()} will be of the type returned by the SRP
	 * driver
	 * 
	 * @return Whether or not to convert pipeline and tx results
	 */
	public boolean getConvertPipelineAndTxResults() {
		return convertPipelineAndTxResults;
	}

	/**
	 * Specifies if pipelined results should be converted to the expected data type. If false, results of
	 * {@link SrpConnection#closePipeline()} and {@link SrpConnection#exec()} will be of the type returned by the SRP
	 * driver
	 * 
	 * @param convertPipelineAndTxResults Whether or not to convert pipeline and tx results
	 */
	public void setConvertPipelineAndTxResults(boolean convertPipelineAndTxResults) {
		this.convertPipelineAndTxResults = convertPipelineAndTxResults;
	}

	@Override
	public RedisSentinelConnection getSentinelConnection() {
		throw new UnsupportedOperationException();
	}
}
