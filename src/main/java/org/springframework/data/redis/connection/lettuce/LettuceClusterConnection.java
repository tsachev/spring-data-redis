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
package org.springframework.data.redis.connection.lettuce;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.PassThroughExceptionTranslationStrategy;
import org.springframework.data.redis.connection.ClusterCommandExecutor;
import org.springframework.data.redis.connection.ClusterCommandExecutor.ClusterCommandCallback;
import org.springframework.data.redis.connection.ClusterInfo;
import org.springframework.data.redis.connection.ClusterNodeProvider;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;

import com.lambdaworks.redis.RedisAsyncConnection;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisClusterConnection;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.cluster.RedisClusterClient;
import com.lambdaworks.redis.cluster.SlotHash;
import com.lambdaworks.redis.cluster.models.partitions.Partitions;
import com.lambdaworks.redis.codec.RedisCodec;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class LettuceClusterConnection extends LettuceConnection implements
		org.springframework.data.redis.connection.RedisClusterConnection, ClusterNodeProvider {

	static final ExceptionTranslationStrategy exceptionConverter = new PassThroughExceptionTranslationStrategy(
			new LettuceExceptionConverter());
	static final RedisCodec<byte[], byte[]> CODEC = new BytesRedisCodec();
	static final Method CONNECT_CODEC_URI;

	static {
		CONNECT_CODEC_URI = ReflectionUtils.findMethod(RedisClient.class, "connect", RedisCodec.class, RedisURI.class);
		ReflectionUtils.makeAccessible(CONNECT_CODEC_URI);
	}

	private final RedisClusterClient clusterClient;
	private final RedisClient nodeClient;
	private Map<RedisClusterNode, RedisConnection<byte[], byte[]>> connections;
	private ClusterCommandExecutor clusterCommandExecutor;

	public LettuceClusterConnection(RedisClusterClient clusterClient) {

		super(null, 100, clusterClient, null);

		this.clusterClient = clusterClient;
		nodeClient = new RedisClient();
		connections = new HashMap<RedisClusterNode, RedisConnection<byte[], byte[]>>(1);
		clusterCommandExecutor = new ClusterCommandExecutor(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#keys(byte[])
	 */
	public Set<byte[]> keys(final byte[] pattern) {

		Assert.notNull(pattern, "Pattern must not be null!");

		Collection<List<byte[]>> keysPerNode = clusterCommandExecutor.executeCommandOnAllNodes(
				new LettuceCommandCallback<List<byte[]>>() {

					@Override
					public List<byte[]> doInCluster(RedisConnection<byte[], byte[]> connection) {
						return connection.keys(pattern);
					}
				}).values();

		Set<byte[]> keys = new HashSet<byte[]>();

		for (List<byte[]> keySet : keysPerNode) {
			keys.addAll(keySet);
		}
		return keys;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#flushAll()
	 */
	public void flushAll() {

		clusterCommandExecutor.executeCommandOnAllNodes(new LettuceCommandCallback<Void>() {

			@Override
			public Void doInCluster(RedisConnection<byte[], byte[]> client) {
				client.flushall();
				return null;
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#flushDb()
	 */
	@Override
	public void flushDb() {

		clusterCommandExecutor.executeCommandOnAllNodes(new LettuceCommandCallback<Void>() {

			@Override
			public Void doInCluster(RedisConnection<byte[], byte[]> client) {
				client.flushdb();
				return null;
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#dbSize()
	 */
	@Override
	public Long dbSize() {

		Map<RedisClusterNode, Long> dbSizes = clusterCommandExecutor
				.executeCommandOnAllNodes(new LettuceCommandCallback<Long>() {

					@Override
					public Long doInCluster(RedisConnection<byte[], byte[]> client) {
						return client.dbsize();
					}

				});

		if (CollectionUtils.isEmpty(dbSizes)) {
			return 0L;
		}

		Long size = 0L;
		for (Long value : dbSizes.values()) {
			size += value;
		}
		return size;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#info()
	 */
	@Override
	public Properties info() {

		Properties infos = new Properties();

		infos.putAll(clusterCommandExecutor.executeCommandOnAllNodes(new LettuceCommandCallback<Properties>() {

			@Override
			public Properties doInCluster(RedisConnection<byte[], byte[]> client) {
				return LettuceConverters.toProperties(client.info());
			}
		}));

		return infos;
	}

	@Override
	public Boolean move(byte[] key, int dbIndex) {
		throw new UnsupportedOperationException("MOVE not supported in CLUSTER mode!");
	}

	@Override
	public Long del(byte[]... keys) {

		Assert.noNullElements(keys, "Keys must not be null or contain null key!");

		long total = 0;
		for (byte[] key : keys) {
			Long delted = super.del(key);
			total += (delted != null ? delted.longValue() : 0);
		}
		return Long.valueOf(total);
	}

	@Override
	public Iterable<RedisClusterNode> getClusterSlaves(final RedisClusterNode master) {

		return clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<Set<RedisClusterNode>>() {

			@Override
			public Set<RedisClusterNode> doInCluster(RedisConnection<byte[], byte[]> client) {
				return LettuceConverters.toSetOfRedisClusterNodes(client.clusterSlaves(master.getId()));
			}
		}, master);
	}

	@Override
	public Integer getClusterSlotForKey(byte[] key) {
		return SlotHash.getSlot(key);
	}

	@Override
	public RedisClusterNode getClusterNodeForSlot(int slot) {

		DirectFieldAccessor accessor = new DirectFieldAccessor(clusterClient);
		return LettuceConverters.toRedisClusterNode(((Partitions) accessor.getPropertyValue("partitions"))
				.getPartitionBySlot(slot));
	}

	@Override
	public RedisClusterNode getClusterNodeForKey(byte[] key) {
		return getClusterNodeForSlot(getClusterSlotForKey(key));
	}

	@Override
	public ClusterInfo getClusterInfo() {

		return clusterCommandExecutor.executeCommandOnArbitraryNode(new LettuceCommandCallback<ClusterInfo>() {

			@Override
			public ClusterInfo doInCluster(RedisConnection<byte[], byte[]> client) {
				return new ClusterInfo(LettuceConverters.toProperties(client.clusterInfo()));
			}
		});
	}

	@Override
	public void addSlots(RedisClusterNode node, final int... slots) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisConnection<byte[], byte[]> client) {
				return client.clusterAddSlots(slots);
			}
		}, node);

	}

	@Override
	public Long countKeys(int slot) {
		throw new UnsupportedOperationException("COUNTKEYSINSLOT is not yet available for LETTUCE.");
	}

	@Override
	public void deleteSlots(RedisClusterNode node, final int... slots) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisConnection<byte[], byte[]> client) {
				return client.clusterDelSlots(slots);
			}
		}, node);

	}

	@Override
	public void clusterForget(final RedisClusterNode node) {

		List<RedisClusterNode> nodes = new ArrayList<RedisClusterNode>(getClusterNodes());
		nodes.remove(node);

		this.clusterCommandExecutor.executeCommandAsyncOnNodes(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisConnection<byte[], byte[]> client) {
				return client.clusterForget(node.getId());
			}

		}, nodes);
	}

	@Override
	public void clusterMeet(final RedisClusterNode node) {

		Assert.notNull(node, "Cluster node must not be null for CLUSTER MEET command!");

		this.clusterCommandExecutor.executeCommandOnAllNodes(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisConnection<byte[], byte[]> client) {
				return client.clusterMeet(node.getHost(), node.getPort());
			}
		});
	}

	@Override
	public void clusterSetSlot(final RedisClusterNode node, final int slot, final AddSlots mode) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisConnection<byte[], byte[]> client) {
				switch (mode) {
					case MIGRATING:
						return client.clusterSetSlotMigrating(slot, node.getId());
					case IMPORTING:
						return client.clusterSetSlotImporting(slot, node.getId());
					case NODE:
						return client.clusterSetSlotNode(slot, node.getId());
					default:
						throw new InvalidDataAccessApiUsageException("Invlid import mode for cluster slot: " + mode);
				}
			}
		}, node);

	}

	@Override
	public List<byte[]> getKeysInSlot(int slot, Integer count) {
		return getConnection().clusterGetKeysInSlot(slot, count);
	}

	@Override
	public void clusterReplicate(final RedisClusterNode master, RedisClusterNode slave) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisConnection<byte[], byte[]> client) {
				return client.clusterReplicate(master.getId());
			}
		}, slave);

	}

	@Override
	public String ping(RedisClusterNode node) {

		return clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisConnection<byte[], byte[]> client) {
				return client.ping();
			}
		}, node);
	}

	@Override
	public void bgReWriteAof(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisConnection<byte[], byte[]> client) {
				return client.bgrewriteaof();
			}
		}, node);
	}

	@Override
	public void bgSave(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisConnection<byte[], byte[]> client) {
				return client.bgsave();
			}
		}, node);
	}

	@Override
	public Long lastSave(RedisClusterNode node) {

		return clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<Long>() {

			@Override
			public Long doInCluster(RedisConnection<byte[], byte[]> client) {
				return client.lastsave().getTime();
			}
		}, node);
	}

	@Override
	public void save(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisConnection<byte[], byte[]> client) {
				return client.save();
			}
		}, node);

	}

	@Override
	public Long dbSize(RedisClusterNode node) {

		return clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<Long>() {

			@Override
			public Long doInCluster(RedisConnection<byte[], byte[]> client) {
				return client.dbsize();
			}
		}, node);
	}

	@Override
	public void flushDb(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisConnection<byte[], byte[]> client) {
				return client.flushdb();
			}
		}, node);
	}

	@Override
	public void flushAll(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisConnection<byte[], byte[]> client) {
				return client.flushall();
			}
		}, node);

	}

	@Override
	public Properties info(RedisClusterNode node) {

		return LettuceConverters.toProperties(clusterCommandExecutor.executeCommandOnSingleNode(
				new LettuceCommandCallback<String>() {

					@Override
					public String doInCluster(RedisConnection<byte[], byte[]> client) {
						return client.info();
					}
				}, node));
	}

	@Override
	public Set<byte[]> keys(RedisClusterNode node, final byte[] pattern) {

		return LettuceConverters.toBytesSet(clusterCommandExecutor.executeCommandOnSingleNode(
				new LettuceCommandCallback<List<byte[]>>() {

					@Override
					public List<byte[]> doInCluster(RedisConnection<byte[], byte[]> client) {
						return client.keys(pattern);
					}
				}, node));
	}

	@Override
	public byte[] randomKey(RedisClusterNode node) {

		return clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<byte[]>() {

			@Override
			public byte[] doInCluster(RedisConnection<byte[], byte[]> client) {
				return client.randomkey();
			}
		}, node);
	}

	@Override
	public byte[] randomKey() {

		List<RedisClusterNode> nodes = getClusterNodes();
		Set<RedisClusterNode> inspectedNodes = new HashSet<RedisClusterNode>(nodes.size());

		do {

			RedisClusterNode node = nodes.get(new Random().nextInt(nodes.size()));

			while (inspectedNodes.contains(node)) {
				node = nodes.get(new Random().nextInt(nodes.size()));
			}
			inspectedNodes.add(node);
			byte[] key = randomKey(node);

			if (key != null && key.length > 0) {
				return key;
			}
		} while (nodes.size() != inspectedNodes.size());

		return null;
	}

	@Override
	public void shutdown(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<Void>() {

			@Override
			public Void doInCluster(RedisConnection<byte[], byte[]> client) {
				client.shutdown(true);
				return null;
			}
		}, node);

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#getAsyncDedicatedConnection()
	 */
	@Override
	protected RedisAsyncConnection<byte[], byte[]> doGetAsyncDedicatedConnection() {
		return (RedisAsyncConnection<byte[], byte[]>) clusterClient.connectClusterAsync(CODEC);
	}

	// --> cluster node stuff
	@Override
	public List<RedisClusterNode> getClusterNodes() {

		DirectFieldAccessor accessor = new DirectFieldAccessor(clusterClient);
		return LettuceConverters.partitionsToClusterNodes((Partitions) accessor.getPropertyValue("partitions"));
	}

	@Override
	@SuppressWarnings("unchecked")
	public RedisConnection<byte[], byte[]> getResourceForSpecificNode(RedisClusterNode node) {

		if (this.connections.containsKey(node)) {
			return this.connections.get(node);
		}

		if (!getClusterNodes().contains(node)) {
			return null;
		}

		RedisConnection<byte[], byte[]> connection = getConnection(node);
		this.connections.put(node, connection);
		return connection;
	}

	@Override
	public RuntimeException convertToDataAccessExeption(Exception e) {
		return exceptionConverter.translate(e);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#close()
	 */
	public void close() {

		super.close();
		try {
			this.clusterCommandExecutor.destroy();
		} catch (Exception e) {
			e.printStackTrace();
		}

		for (RedisClusterConnection<byte[], byte[]> connection : this.connections.values()) {
			connection.close();
		}

		// takes otherwise decades :-)
		nodeClient.shutdown(0, 0, TimeUnit.MILLISECONDS);
	}

	@SuppressWarnings("rawtypes")
	private RedisConnection getConnection(RedisClusterNode node) {

		return (RedisConnection) ReflectionUtils.invokeMethod(CONNECT_CODEC_URI, nodeClient, CODEC,
				RedisURI.Builder.redis(node.getHost(), node.getPort()).build());
	}

	protected interface LettuceCommandCallback<T> extends ClusterCommandCallback<RedisConnection<byte[], byte[]>, T> {}

}
