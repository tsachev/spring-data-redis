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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.PassThroughExceptionTranslationStrategy;
import org.springframework.data.redis.connection.ClusterCommandExecutor;
import org.springframework.data.redis.connection.ClusterCommandExecutor.ClusterCommandCallback;
import org.springframework.data.redis.connection.ClusterCommandExecutor.MultiKeyClusterCommandCallback;
import org.springframework.data.redis.connection.ClusterInfo;
import org.springframework.data.redis.connection.ClusterNodeResourceProvider;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.ClusterTopology;
import org.springframework.data.redis.connection.ClusterTopologyProvider;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.util.ByteArraySet;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import com.lambdaworks.redis.KeyValue;
import com.lambdaworks.redis.RedisAsyncConnection;
import com.lambdaworks.redis.RedisAsyncConnectionImpl;
import com.lambdaworks.redis.RedisClusterConnection;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisException;
import com.lambdaworks.redis.cluster.RedisClusterClient;
import com.lambdaworks.redis.cluster.SlotHash;
import com.lambdaworks.redis.cluster.models.partitions.Partitions;
import com.lambdaworks.redis.codec.RedisCodec;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public class LettuceClusterConnection extends LettuceConnection implements
		org.springframework.data.redis.connection.RedisClusterConnection, ClusterNodeResourceProvider {

	static final ExceptionTranslationStrategy exceptionConverter = new PassThroughExceptionTranslationStrategy(
			new LettuceExceptionConverter());
	static final RedisCodec<byte[], byte[]> CODEC = new BytesRedisCodec();

	private final RedisClusterClient clusterClient;
	private Map<RedisClusterNode, RedisClusterConnection<byte[], byte[]>> connections;
	private ClusterCommandExecutor clusterCommandExecutor;
	private ClusterTopologyProvider topologyProvider;

	/**
	 * Creates new {@link LettuceClusterConnection} using {@link RedisClusterClient}.
	 * 
	 * @param clusterClient must not be {@literal null}.
	 */
	public LettuceClusterConnection(RedisClusterClient clusterClient) {

		super(null, 100, clusterClient, null, 0);

		this.clusterClient = clusterClient;
		connections = new HashMap<RedisClusterNode, RedisClusterConnection<byte[], byte[]>>(1);
		topologyProvider = new LettuceClusterTopologyProvider(clusterClient);
		clusterCommandExecutor = new ClusterCommandExecutor(topologyProvider, this, exceptionConverter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#scan(long, org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<byte[]> scan(long cursorId, ScanOptions options) {
		throw new InvalidDataAccessApiUsageException("Scan is not supported accros multiple nodes within a cluster.");
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
					public List<byte[]> doInCluster(RedisClusterConnection<byte[], byte[]> connection) {
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

		clusterCommandExecutor.executeCommandOnAllNodes(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterConnection<byte[], byte[]> client) {
				return client.flushall();
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#flushDb()
	 */
	@Override
	public void flushDb() {

		clusterCommandExecutor.executeCommandOnAllNodes(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterConnection<byte[], byte[]> client) {
				return client.flushdb();
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
					public Long doInCluster(RedisClusterConnection<byte[], byte[]> client) {
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
			public Properties doInCluster(RedisClusterConnection<byte[], byte[]> client) {
				return LettuceConverters.toProperties(client.info());
			}
		}));

		return infos;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#move(byte[], int)
	 */
	@Override
	public Boolean move(byte[] key, int dbIndex) {
		throw new UnsupportedOperationException("MOVE not supported in CLUSTER mode!");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#del(byte[][])
	 */
	@Override
	public Long del(byte[]... keys) {

		Assert.noNullElements(keys, "Keys must not be null or contain null key!");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.del(keys);
		}

		long total = 0;
		for (byte[] key : keys) {
			Long delted = super.del(key);
			total += (delted != null ? delted.longValue() : 0);
		}
		return Long.valueOf(total);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#getClusterSlaves(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Iterable<RedisClusterNode> getClusterSlaves(final RedisClusterNode master) {

		return clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<Set<RedisClusterNode>>() {

			@Override
			public Set<RedisClusterNode> doInCluster(RedisClusterConnection<byte[], byte[]> client) {
				return LettuceConverters.toSetOfRedisClusterNodes(client.clusterSlaves(master.getId()));
			}
		}, master);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#getClusterSlotForKey(byte[])
	 */
	@Override
	public Integer getClusterSlotForKey(byte[] key) {
		return SlotHash.getSlot(key);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#getClusterNodeForSlot(int)
	 */
	@Override
	public RedisClusterNode getClusterNodeForSlot(int slot) {

		DirectFieldAccessor accessor = new DirectFieldAccessor(clusterClient);
		return LettuceConverters.toRedisClusterNode(((Partitions) accessor.getPropertyValue("partitions"))
				.getPartitionBySlot(slot));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#getClusterNodeForKey(byte[])
	 */
	@Override
	public RedisClusterNode getClusterNodeForKey(byte[] key) {
		return getClusterNodeForSlot(getClusterSlotForKey(key));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#getClusterInfo()
	 */
	@Override
	public ClusterInfo getClusterInfo() {

		return clusterCommandExecutor.executeCommandOnArbitraryNode(new LettuceCommandCallback<ClusterInfo>() {

			@Override
			public ClusterInfo doInCluster(RedisClusterConnection<byte[], byte[]> client) {
				return new ClusterInfo(LettuceConverters.toProperties(client.clusterInfo()));
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#addSlots(org.springframework.data.redis.connection.RedisClusterNode, int[])
	 */
	@Override
	public void addSlots(RedisClusterNode node, final int... slots) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterConnection<byte[], byte[]> client) {
				return client.clusterAddSlots(slots);
			}
		}, node);

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#deleteSlots(org.springframework.data.redis.connection.RedisClusterNode, int[])
	 */
	@Override
	public void deleteSlots(RedisClusterNode node, final int... slots) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterConnection<byte[], byte[]> client) {
				return client.clusterDelSlots(slots);
			}
		}, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterForget(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void clusterForget(final RedisClusterNode node) {

		List<RedisClusterNode> nodes = new ArrayList<RedisClusterNode>(getClusterNodes());
		nodes.remove(node);

		this.clusterCommandExecutor.executeCommandAsyncOnNodes(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterConnection<byte[], byte[]> client) {
				return client.clusterForget(node.getId());
			}

		}, nodes);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterMeet(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void clusterMeet(final RedisClusterNode node) {

		Assert.notNull(node, "Cluster node must not be null for CLUSTER MEET command!");

		this.clusterCommandExecutor.executeCommandOnAllNodes(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterConnection<byte[], byte[]> client) {
				return client.clusterMeet(node.getHost(), node.getPort());
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterSetSlot(org.springframework.data.redis.connection.RedisClusterNode, int, org.springframework.data.redis.connection.RedisClusterCommands.AddSlots)
	 */
	@Override
	public void clusterSetSlot(final RedisClusterNode node, final int slot, final AddSlots mode) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterConnection<byte[], byte[]> client) {
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#getKeysInSlot(int, java.lang.Integer)
	 */
	@Override
	public List<byte[]> getKeysInSlot(int slot, Integer count) {

		try {
			return getConnection().clusterGetKeysInSlot(slot, count);
		} catch (Exception ex) {
			throw exceptionConverter.translate(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#countKeys(int)
	 */
	@Override
	public Long countKeys(int slot) {

		try {
			return getConnection().clusterCountKeysInSlot(slot);
		} catch (Exception ex) {
			throw exceptionConverter.translate(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterReplicate(org.springframework.data.redis.connection.RedisClusterNode, org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void clusterReplicate(final RedisClusterNode master, RedisClusterNode slave) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterConnection<byte[], byte[]> client) {
				return client.clusterReplicate(master.getId());
			}
		}, slave);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#ping()
	 */
	@Override
	public String ping() {
		Collection<String> ping = clusterCommandExecutor.executeCommandOnAllNodes(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterConnection<byte[], byte[]> connection) {
				return doPing(connection);
			}
		}).values();

		for (String result : ping) {
			if (!ObjectUtils.nullSafeEquals("PONG", result)) {
				return "";
			}
		}

		return "PONG";
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#ping(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public String ping(RedisClusterNode node) {

		return clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterConnection<byte[], byte[]> client) {
				return doPing(client);
			}
		}, node);
	}

	protected String doPing(RedisClusterConnection<byte[], byte[]> client) {

		if (client instanceof RedisConnection) {
			return ((RedisConnection) client).ping();
		}

		if (client instanceof RedisAsyncConnectionImpl) {
			try {
				return (String) ((RedisAsyncConnectionImpl) client).ping().get();
			} catch (Exception e) {
				throw exceptionConverter.translate(e);
			}
		}

		throw new DataAccessResourceFailureException("Cannot execute ping using " + client);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#bgReWriteAof(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void bgReWriteAof(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterConnection<byte[], byte[]> client) {
				return client.bgrewriteaof();
			}
		}, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#bgSave(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void bgSave(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterConnection<byte[], byte[]> client) {
				return client.bgsave();
			}
		}, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#lastSave(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Long lastSave(RedisClusterNode node) {

		return clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<Long>() {

			@Override
			public Long doInCluster(RedisClusterConnection<byte[], byte[]> client) {
				return client.lastsave().getTime();
			}
		}, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#save(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void save(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterConnection<byte[], byte[]> client) {
				return client.save();
			}
		}, node);

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#dbSize(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Long dbSize(RedisClusterNode node) {

		return clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<Long>() {

			@Override
			public Long doInCluster(RedisClusterConnection<byte[], byte[]> client) {
				return client.dbsize();
			}
		}, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#flushDb(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void flushDb(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterConnection<byte[], byte[]> client) {
				return client.flushdb();
			}
		}, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#flushAll(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void flushAll(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterConnection<byte[], byte[]> client) {
				return client.flushall();
			}
		}, node);

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#info(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Properties info(RedisClusterNode node) {

		return LettuceConverters.toProperties(clusterCommandExecutor.executeCommandOnSingleNode(
				new LettuceCommandCallback<String>() {

					@Override
					public String doInCluster(RedisClusterConnection<byte[], byte[]> client) {
						return client.info();
					}
				}, node));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#keys(org.springframework.data.redis.connection.RedisClusterNode, byte[])
	 */
	@Override
	public Set<byte[]> keys(RedisClusterNode node, final byte[] pattern) {

		return LettuceConverters.toBytesSet(clusterCommandExecutor.executeCommandOnSingleNode(
				new LettuceCommandCallback<List<byte[]>>() {

					@Override
					public List<byte[]> doInCluster(RedisClusterConnection<byte[], byte[]> client) {
						return client.keys(pattern);
					}
				}, node));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#randomKey(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public byte[] randomKey(RedisClusterNode node) {

		return clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<byte[]>() {

			@Override
			public byte[] doInCluster(RedisClusterConnection<byte[], byte[]> client) {
				return client.randomkey();
			}
		}, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#randomKey()
	 */
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#rename(byte[], byte[])
	 */
	@Override
	public void rename(byte[] oldName, byte[] newName) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(oldName, newName)) {
			super.rename(oldName, newName);
			return;
		}

		byte[] value = dump(oldName);

		if (value != null && value.length > 0) {

			restore(newName, 0, value);
			del(oldName);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#renameNX(byte[], byte[])
	 */
	@Override
	public Boolean renameNX(byte[] oldName, byte[] newName) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(oldName, newName)) {
			return super.renameNX(oldName, newName);
		}

		byte[] value = dump(oldName);

		if (value != null && value.length > 0 && !exists(newName)) {

			restore(newName, 0, value);
			del(oldName);
			return Boolean.TRUE;
		}
		return Boolean.FALSE;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#shutdown(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void shutdown(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceCommandCallback<Void>() {

			@Override
			public Void doInCluster(RedisClusterConnection<byte[], byte[]> client) {
				client.shutdown(true);
				return null;
			}
		}, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#sort(byte[], org.springframework.data.redis.connection.SortParameters, byte[])
	 */
	@Override
	public Long sort(byte[] key, SortParameters params, byte[] storeKey) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(key, storeKey)) {
			return super.sort(key, params, storeKey);
		}

		List<byte[]> sorted = sort(key, params);
		if (!CollectionUtils.isEmpty(sorted)) {

			byte[][] arr = new byte[sorted.size()][];
			switch (type(key)) {

				case SET:
					sAdd(storeKey, sorted.toArray(arr));
					return 1L;
				case LIST:
					lPush(storeKey, sorted.toArray(arr));
					return 1L;
				default:
					throw new IllegalArgumentException("sort and store is only supported for SET and LIST");
			}
		}
		return 0L;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#mGet(byte[][])
	 */
	@Override
	public List<byte[]> mGet(byte[]... keys) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.mGet(keys);
		}

		Map<RedisClusterNode, byte[]> nodeResult = this.clusterCommandExecutor.executeMuliKeyCommand(
				new LettuceMultiKeyClusterCommandCallback<byte[]>() {

					@Override
					public byte[] doInCluster(RedisClusterConnection<byte[], byte[]> client, byte[] key) {
						return client.get(key);
					}
				}, Arrays.asList(keys));

		return new ArrayList<byte[]>(nodeResult.values());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#mSet(java.util.Map)
	 */
	@Override
	public void mSet(Map<byte[], byte[]> tuples) {

		Assert.notNull(tuples, "Tuple must not be null!");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(tuples.keySet().toArray(new byte[tuples.keySet().size()][]))) {
			super.mSet(tuples);
			return;
		}

		for (Map.Entry<byte[], byte[]> entry : tuples.entrySet()) {
			set(entry.getKey(), entry.getValue());
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#mSetNX(java.util.Map)
	 */
	@Override
	public Boolean mSetNX(Map<byte[], byte[]> tuples) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(tuples.keySet().toArray(new byte[tuples.keySet().size()][]))) {
			return super.mSetNX(tuples);
		}

		boolean result = true;
		for (Map.Entry<byte[], byte[]> entry : tuples.entrySet()) {
			if (!setNX(entry.getKey(), entry.getValue()) && result) {
				result = false;
			}
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#bLPop(int, byte[][])
	 */
	@Override
	public List<byte[]> bLPop(final int timeout, byte[]... keys) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.bLPop(timeout, keys);
		}

		Map<RedisClusterNode, KeyValue<byte[], byte[]>> nodeResult = this.clusterCommandExecutor.executeMuliKeyCommand(
				new LettuceMultiKeyClusterCommandCallback<KeyValue<byte[], byte[]>>() {

					@Override
					public KeyValue<byte[], byte[]> doInCluster(RedisClusterConnection<byte[], byte[]> client, byte[] key) {
						return client.blpop(timeout, key);
					}
				}, Arrays.asList(keys));

		for (KeyValue<byte[], byte[]> partial : nodeResult.values()) {
			return LettuceConverters.toBytesList(partial);
		}

		return Collections.emptyList();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#bRPop(int, byte[][])
	 */
	@Override
	public List<byte[]> bRPop(final int timeout, byte[]... keys) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.bRPop(timeout, keys);
		}

		Map<RedisClusterNode, KeyValue<byte[], byte[]>> nodeResult = this.clusterCommandExecutor.executeMuliKeyCommand(
				new LettuceMultiKeyClusterCommandCallback<KeyValue<byte[], byte[]>>() {

					@Override
					public KeyValue<byte[], byte[]> doInCluster(RedisClusterConnection<byte[], byte[]> client, byte[] key) {
						return client.brpop(timeout, key);
					}
				}, Arrays.asList(keys));

		for (KeyValue<byte[], byte[]> partial : nodeResult.values()) {
			return LettuceConverters.toBytesList(partial);
		}

		return Collections.emptyList();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#rPopLPush(byte[], byte[])
	 */
	@Override
	public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(srcKey, dstKey)) {
			return super.rPopLPush(srcKey, dstKey);
		}

		byte[] val = rPop(srcKey);
		lPush(dstKey, val);
		return val;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#bRPopLPush(int, byte[], byte[])
	 */
	@Override
	public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(srcKey, dstKey)) {
			return super.bRPopLPush(timeout, srcKey, dstKey);
		}

		List<byte[]> val = bRPop(timeout, srcKey);
		if (!CollectionUtils.isEmpty(val)) {
			lPush(dstKey, val.get(1));
			return val.get(1);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#sMove(byte[], byte[], byte[])
	 */
	@Override
	public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(srcKey, destKey)) {
			return super.sMove(srcKey, destKey, value);
		}

		if (exists(srcKey)) {
			if (sRem(srcKey, value) > 0 && !sIsMember(destKey, value)) {
				return LettuceConverters.toBoolean(sAdd(destKey, value));
			}
		}
		return Boolean.FALSE;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#sInter(byte[][])
	 */
	@Override
	public Set<byte[]> sInter(byte[]... keys) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.sInter(keys);
		}

		Map<RedisClusterNode, Set<byte[]>> nodeResult = this.clusterCommandExecutor.executeMuliKeyCommand(
				new LettuceMultiKeyClusterCommandCallback<Set<byte[]>>() {

					@Override
					public Set<byte[]> doInCluster(RedisClusterConnection<byte[], byte[]> client, byte[] key) {
						return client.smembers(key);
					}
				}, Arrays.asList(keys));

		ByteArraySet result = null;
		for (Entry<RedisClusterNode, Set<byte[]>> entry : nodeResult.entrySet()) {

			ByteArraySet tmp = new ByteArraySet(entry.getValue());
			if (result == null) {
				result = tmp;
			} else {
				result.retainAll(tmp);
				if (result.isEmpty()) {
					break;
				}
			}
		}

		if (result.isEmpty()) {
			return Collections.emptySet();
		}

		return result.asRawSet();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#sInterStore(byte[], byte[][])
	 */
	@Override
	public Long sInterStore(byte[] destKey, byte[]... keys) {

		byte[][] allKeys = new byte[keys.length + 1][];
		allKeys[0] = destKey;
		System.arraycopy(keys, 0, allKeys, 1, keys.length);
		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {
			return super.sInterStore(destKey, keys);
		}

		Set<byte[]> result = sInter(keys);
		if (result.isEmpty()) {
			return 0L;
		}
		return sAdd(destKey, result.toArray(new byte[result.size()][]));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#sUnion(byte[][])
	 */
	@Override
	public Set<byte[]> sUnion(byte[]... keys) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.sUnion(keys);
		}

		Map<RedisClusterNode, Set<byte[]>> nodeResult = this.clusterCommandExecutor.executeMuliKeyCommand(
				new LettuceMultiKeyClusterCommandCallback<Set<byte[]>>() {

					@Override
					public Set<byte[]> doInCluster(RedisClusterConnection<byte[], byte[]> client, byte[] key) {
						return client.smembers(key);
					}
				}, Arrays.asList(keys));

		ByteArraySet result = new ByteArraySet();
		for (Entry<RedisClusterNode, Set<byte[]>> entry : nodeResult.entrySet()) {
			result.addAll(entry.getValue());
		}

		if (result.isEmpty()) {
			return Collections.emptySet();
		}

		return result.asRawSet();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#sUnionStore(byte[], byte[][])
	 */
	@Override
	public Long sUnionStore(byte[] destKey, byte[]... keys) {

		byte[][] allKeys = new byte[keys.length + 1][];
		allKeys[0] = destKey;
		System.arraycopy(keys, 0, allKeys, 1, keys.length);
		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {
			return super.sUnionStore(destKey, keys);
		}

		Set<byte[]> result = sUnion(keys);
		if (result.isEmpty()) {
			return 0L;
		}
		return sAdd(destKey, result.toArray(new byte[result.size()][]));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#sDiff(byte[][])
	 */
	@Override
	public Set<byte[]> sDiff(byte[]... keys) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.sDiff(keys);
		}

		byte[] source = keys[0];
		byte[][] others = Arrays.copyOfRange(keys, 1, keys.length - 1);

		ByteArraySet values = new ByteArraySet(sMembers(source));
		Map<RedisClusterNode, Set<byte[]>> nodeResult = clusterCommandExecutor.executeMuliKeyCommand(
				new LettuceMultiKeyClusterCommandCallback<Set<byte[]>>() {

					@Override
					public Set<byte[]> doInCluster(RedisClusterConnection<byte[], byte[]> client, byte[] key) {
						return client.smembers(key);
					}
				}, Arrays.asList(others));

		if (values.isEmpty()) {
			return Collections.emptySet();
		}

		for (Set<byte[]> toSubstract : nodeResult.values()) {
			values.removeAll(toSubstract);
		}

		return values.asRawSet();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#sDiffStore(byte[], byte[][])
	 */
	@Override
	public Long sDiffStore(byte[] destKey, byte[]... keys) {

		byte[][] allKeys = new byte[keys.length + 1][];
		allKeys[0] = destKey;
		System.arraycopy(keys, 0, allKeys, 1, keys.length);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {
			return super.sDiffStore(destKey, keys);
		}

		Set<byte[]> diff = sDiff(keys);
		if (diff.isEmpty()) {
			return 0L;
		}

		return sAdd(destKey, diff.toArray(new byte[diff.size()][]));
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#getClusterNodes()
	 */
	@Override
	public List<RedisClusterNode> getClusterNodes() {
		return LettuceConverters.partitionsToClusterNodes(clusterClient.getPartitions());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ClusterNodeResourceProvider#getResourceForSpecificNode(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public RedisClusterConnection<byte[], byte[]> getResourceForSpecificNode(RedisClusterNode node) {

		Assert.notNull(node, "Node must not be null!");

		if (this.connections.containsKey(node)) {
			return this.connections.get(node);
		}

		try {
			RedisClusterConnection<byte[], byte[]> connection = clusterClient.connectCluster(CODEC).getConnection(
					node.getHost(), node.getPort());
			this.connections.put(node, connection);
			return connection;
		} catch (RedisException e) {

			// unwrap cause when cluster node not known in cluster
			if (e.getCause() instanceof IllegalArgumentException) {
				throw (IllegalArgumentException) e.getCause();
			}
			throw e;
		}
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
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#pfCount(byte[][])
	 */
	@Override
	public Long pfCount(byte[]... keys) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {

			try {
				return super.pfCount(keys);
			} catch (Exception ex) {
				throw convertLettuceAccessException(ex);
			}

		}
		throw new InvalidDataAccessApiUsageException("All keys must map to same slot for pfcount in cluster mode.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#pfMerge(byte[], byte[][])
	 */
	@Override
	public void pfMerge(byte[] destinationKey, byte[]... sourceKeys) {

		byte[][] allKeys = new byte[sourceKeys.length + 1][];
		allKeys[0] = destinationKey;
		System.arraycopy(sourceKeys, 0, allKeys, 1, sourceKeys.length);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {
			try {
				super.pfMerge(destinationKey, sourceKeys);
				return;
			} catch (Exception ex) {
				throw convertLettuceAccessException(ex);
			}

		}
		throw new InvalidDataAccessApiUsageException("All keys must map to same slot for pfmerge in cluster mode.");
	}

	@Override
	public void returnResourceForSpecificNode(RedisClusterNode node, Object resource) {
		// nothing to do here!
	}

	/**
	 * Lettuce specific implementation of {@link ClusterCommandCallback}.
	 * 
	 * @author Christoph Strobl
	 * @param <T>
	 * @since 1.7
	 */
	protected interface LettuceCommandCallback<T> extends
			ClusterCommandCallback<RedisClusterConnection<byte[], byte[]>, T> {}

	/**
	 * Lettuce specific implementation of {@link MultiKeyClusterCommandCallback}.
	 * 
	 * @author Christoph Strobl
	 * @param <T>
	 * @since 1.7
	 */
	protected interface LettuceMultiKeyClusterCommandCallback<T> extends
			MultiKeyClusterCommandCallback<RedisClusterConnection<byte[], byte[]>, T> {

	}

	/**
	 * Lettuce specific implementation of {@link ClusterTopologyProvider}.
	 * 
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static class LettuceClusterTopologyProvider implements ClusterTopologyProvider {

		private final RedisClusterClient client;

		/**
		 * @param client must not be {@literal null}.
		 */
		public LettuceClusterTopologyProvider(RedisClusterClient client) {

			Assert.notNull(client, "RedisClusteClient must not be null.");
			this.client = client;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.ClusterTopologyProvider#getTopology()
		 */
		@Override
		public ClusterTopology getTopology() {
			return new ClusterTopology(new LinkedHashSet<RedisClusterNode>(LettuceConverters.partitionsToClusterNodes(client
					.getPartitions())));
		}
	}

}
