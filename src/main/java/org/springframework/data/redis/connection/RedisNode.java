/*
 * Copyright 2014 the original author or authors.
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

import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @since 1.4
 */
public class RedisNode implements NamedNode {

	private String id;
	private String name;
	private String host;
	private int port;
	private NodeType type;
	private String masterId;

	/**
	 * Creates a new {@link RedisNode} with the given {@code host}, {@code port}.
	 * 
	 * @param host must not be {@literal null}
	 * @param port
	 */
	public RedisNode(String host, int port) {

		Assert.notNull(host, "host must not be null!");

		this.host = host;
		this.port = port;
	}

	protected RedisNode() {}

	public String getHost() {
		return host;
	}

	public Integer getPort() {
		return port;
	}

	public String asString() {
		return host + ":" + port;
	}

	@Override
	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return
	 * @since 1.7
	 */
	public String getMasterId() {
		return masterId;
	}

	/**
	 * @param masterId
	 * @since 1.7
	 */
	public void setMasterId(String masterId) {
		this.masterId = masterId;
	}

	/**
	 * @return
	 * @since 1.7
	 */
	public String getId() {
		return id;
	}

	/**
	 * @param id
	 * @since 1.7
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @param type
	 * @since 1.6
	 */
	public void setType(NodeType type) {
		this.type = type;
	}

	/**
	 * @return
	 * @since 1.7
	 */
	public NodeType getType() {
		return type;
	}

	/**
	 * @return
	 * @since 1.7
	 */
	public boolean isMaster() {
		return ObjectUtils.nullSafeEquals(NodeType.MASTER, getType());
	}

	/**
	 * @return
	 * @since 1.7
	 */
	public boolean isSlave() {
		return ObjectUtils.nullSafeEquals(NodeType.SLAVE, getType());
	}

	@Override
	public String toString() {
		return asString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ObjectUtils.nullSafeHashCode(host);
		result = prime * result + ObjectUtils.nullSafeHashCode(port);
		return result;
	}

	@Override
	public boolean equals(Object obj) {

		if (this == obj) {
			return true;
		}
		if (obj == null || !(obj instanceof RedisNode)) {
			return false;
		}

		RedisNode other = (RedisNode) obj;

		if (!ObjectUtils.nullSafeEquals(this.host, other.host)) {
			return false;
		}

		if (!ObjectUtils.nullSafeEquals(this.port, other.port)) {
			return false;
		}

		if (!ObjectUtils.nullSafeEquals(this.name, other.name)) {
			return false;
		}

		return true;
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	public enum NodeType {
		MASTER, SLAVE
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.4
	 */
	public static class RedisNodeBuilder {

		private RedisNode node;

		public RedisNodeBuilder() {
			node = new RedisNode();
		}

		public RedisNodeBuilder withName(String name) {
			node.name = name;
			return this;
		}

		public RedisNodeBuilder listeningAt(String host, int port) {

			Assert.hasText(host, "Hostname must not be empty or null.");
			node.host = host;
			node.port = port;
			return this;
		}

		public RedisNodeBuilder withId(String id) {

			node.id = id;
			return this;
		}

		public RedisNode build() {
			return this.node;
		}
	}

	public <T extends RedisNode> T withId(String id) {
		this.id = id;
		return (T) this;
	}

	public <T extends RedisNode> T withType(NodeType type) {
		this.type = type;
		return (T) this;
	}

}
