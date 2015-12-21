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

import static org.springframework.util.Assert.*;
import static org.springframework.util.StringUtils.*;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.util.NumberUtils;
import org.springframework.util.StringUtils;

/**
 * Configuration class used for setting up {@link RedisConnection} via {@link RedisConnectionFactory} using connecting
 * to <a href="http://redis.io/topics/cluster-spec">Redis Cluster</a>. Useful when setting up a high availability Redis
 * environment.
 * 
 * @author Christoph Strobl
 * @since 1.5
 */
public class RedisClusterConfiguration {

	private static final String REDIS_CLUSTER_NODES_CONFIG_PROPERTY = "spring.redis.cluster.nodes";
	private static final String REDIS_CLUSTER_TIMEOUT_CONFIG_PROPERTY = "spring.redis.cluster.timeout";
	private static final String REDIS_CLUSTER_MAX_REDIRECTS_CONFIG_PROPERTY = "spring.redis.cluster.max-redirects";

	private Set<RedisNode> clusterNodes;
	private Long clusterTimeout;
	private Integer maxRedirects;

	/**
	 * Creates new {@link RedisClusterConfiguration}.
	 */
	public RedisClusterConfiguration() {
		this(new MapPropertySource("RedisClusterConfiguration", Collections.<String, Object> emptyMap()));
	}

	/**
	 * Creates {@link RedisClusterConfiguration} for given hostPort combinations.
	 * 
	 * <pre>
	 * clusterHostAndPorts[0] = 127.0.0.1:23679
	 * clusterHostAndPorts[1] = 127.0.0.1:23680
	 * ...
	 * 
	 * <pre>
	 * 
	 * @param cluster must not be {@literal null}.
	 * @since 1.5
	 */
	public RedisClusterConfiguration(Collection<String> clusterNodes) {
		this(new MapPropertySource("RedisClusterConfiguration", asMap(clusterNodes, -1, -1)));
	}

	/**
	 * Creates {@link RedisClusterConfiguration} looking up values in given {@link PropertySource}.
	 * 
	 * <pre>
	 * <code>
	 * spring.redis.cluster.nodes=127.0.0.1:23679,127.0.0.1:23680,127.0.0.1:23681
	 * spring.redis.cluster.timeout=5
	 * spring.redis.cluster.max-redirects=3
	 * </code>
	 * </pre>
	 * 
	 * @param propertySource must not be {@literal null}.
	 * @since 1.5
	 */
	public RedisClusterConfiguration(PropertySource<?> propertySource) {

		notNull(propertySource, "PropertySource must not be null!");

		this.clusterNodes = new LinkedHashSet<RedisNode>();

		if (propertySource.containsProperty(REDIS_CLUSTER_NODES_CONFIG_PROPERTY)) {
			appendClusterNodes(commaDelimitedListToSet(propertySource.getProperty(REDIS_CLUSTER_NODES_CONFIG_PROPERTY)
					.toString()));
		}
		if (propertySource.containsProperty(REDIS_CLUSTER_TIMEOUT_CONFIG_PROPERTY)) {
			this.clusterTimeout = NumberUtils.parseNumber(propertySource.getProperty(REDIS_CLUSTER_TIMEOUT_CONFIG_PROPERTY)
					.toString(), Long.class);
		}
		if (propertySource.containsProperty(REDIS_CLUSTER_MAX_REDIRECTS_CONFIG_PROPERTY)) {
			this.maxRedirects = NumberUtils.parseNumber(
					propertySource.getProperty(REDIS_CLUSTER_MAX_REDIRECTS_CONFIG_PROPERTY).toString(), Integer.class);
		}
	}

	/**
	 * Set {@literal cluster nodes} to connect to.
	 * 
	 * @param nodes must not be {@literal null}.
	 */
	public void setClusterNodes(Iterable<RedisNode> nodes) {

		notNull(nodes, "Cannot set cluster nodes to 'null'.");

		this.clusterNodes.clear();

		for (RedisNode clusterNode : nodes) {
			addClusterNode(clusterNode);
		}
	}

	/**
	 * Returns an {@link Collections#unmodifiableSet(Set)} of {@literal cluster nodes}.
	 * 
	 * @return {@link Set} of nodes. Never {@literal null}.
	 */
	public Set<RedisNode> getClusterNodes() {
		return Collections.unmodifiableSet(clusterNodes);
	}

	/**
	 * Add a cluster node to configuration.
	 * 
	 * @param node must not be {@literal null}.
	 */
	public void addClusterNode(RedisNode node) {

		notNull(node, "ClusterNode must not be 'null'.");
		this.clusterNodes.add(node);
	}

	/**
	 * @return
	 */
	public RedisClusterConfiguration clusterNode(RedisNode node) {
		this.clusterNodes.add(node);
		return this;
	}

	/**
	 * @return
	 */
	public Long getClusterTimeout() {
		return clusterTimeout != null && clusterTimeout > Long.MIN_VALUE ? clusterTimeout : null;
	}

	/**
	 * @return
	 */
	public Integer getMaxRedirects() {
		return maxRedirects != null && maxRedirects > Integer.MIN_VALUE ? maxRedirects : null;
	}

	/**
	 * @param host
	 * @param port
	 * @return
	 */
	public RedisClusterConfiguration clusterNode(String host, Integer port) {
		return clusterNode(new RedisNode(host, port));
	}

	private void appendClusterNodes(Set<String> hostAndPorts) {

		for (String hostAndPort : hostAndPorts) {
			addClusterNode(readHostAndPortFromString(hostAndPort));
		}
	}

	private RedisNode readHostAndPortFromString(String hostAndPort) {

		String[] args = split(hostAndPort, ":");

		notNull(args, "HostAndPort need to be seperated by  ':'.");
		isTrue(args.length == 2, "Host and Port String needs to specified as host:port");
		return new RedisNode(args[0], Integer.valueOf(args[1]).intValue());
	}

	/**
	 * @param master must not be {@literal null} or empty.
	 * @param clusterHostAndPorts must not be {@literal null}.
	 * @return
	 */
	private static Map<String, Object> asMap(Collection<String> clusterHostAndPorts, long timeout, int redirects) {

		notNull(clusterHostAndPorts, "ClusterHostAndPorts must not be null!");

		Map<String, Object> map = new HashMap<String, Object>();
		map.put(REDIS_CLUSTER_NODES_CONFIG_PROPERTY, StringUtils.collectionToCommaDelimitedString(clusterHostAndPorts));
		if (timeout >= 0) {
			map.put(REDIS_CLUSTER_TIMEOUT_CONFIG_PROPERTY, Long.valueOf(timeout));
		}
		if (redirects >= 0) {
			map.put(REDIS_CLUSTER_MAX_REDIRECTS_CONFIG_PROPERTY, Integer.valueOf(redirects));
		}

		return map;
	}
}
