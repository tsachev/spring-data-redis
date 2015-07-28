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
package org.springframework.data.redis.connection.jedis;

import static org.springframework.util.Assert.*;
import static org.springframework.util.ReflectionUtils.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.FallbackExceptionTranslationStrategy;
import org.springframework.data.redis.connection.ClusterCommandExecutor;
import org.springframework.data.redis.connection.ClusterCommandExecutor.ClusterCommandCallback;
import org.springframework.data.redis.connection.ClusterInfo;
import org.springframework.data.redis.connection.ClusterNodeResourceProvider;
import org.springframework.data.redis.connection.ClusterTopology;
import org.springframework.data.redis.connection.ClusterTopologyProvider;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.data.redis.connection.RedisSentinelConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisClusterCommand;
import redis.clients.jedis.JedisClusterConnectionHandler;
import redis.clients.jedis.JedisPool;

/**
 * @author Christoph Strobl
 */
public class JedisClusterConnection implements RedisClusterConnection, ClusterTopologyProvider,
		ClusterNodeResourceProvider {

	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new FallbackExceptionTranslationStrategy(
			JedisConverters.exceptionConverter());

	private static final Field CONNECTION_HANDLER;
	private final JedisCluster cluster;
	private final ThreadPoolTaskExecutor executor;

	private boolean closed;

	private final JedisClusterTopologyProvider topologyProvider;
	private ClusterCommandExecutor clusterCommandExecutor;

	static {

		Field connectionHandler = findField(JedisCluster.class, "connectionHandler");
		makeAccessible(connectionHandler);
		CONNECTION_HANDLER = connectionHandler;
	}

	public JedisClusterConnection(JedisCluster cluster) {

		notNull(cluster);
		this.cluster = cluster;

		executor = new ThreadPoolTaskExecutor();
		this.executor.initialize();
		closed = false;
		this.topologyProvider = new JedisClusterTopologyProvider(cluster);
		clusterCommandExecutor = new ClusterCommandExecutor(this, this, EXCEPTION_TRANSLATION);
	}

	@Override
	public void close() throws DataAccessException {
		closed = true;
	}

	@Override
	public boolean isClosed() {
		return closed;
	}

	@Override
	public Object getNativeConnection() {
		return cluster;
	}

	@Override
	public boolean isQueueing() {
		return false;
	}

	@Override
	public boolean isPipelined() {
		return false;
	}

	@Override
	public void openPipeline() {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<Object> closePipeline() throws RedisPipelineException {
		throw new UnsupportedOperationException();
	}

	@Override
	public RedisSentinelConnection getSentinelConnection() {
		return null;
	}

	@Override
	public Object execute(String command, byte[]... args) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long del(byte[]... keys) {

		noNullElements(keys, "Keys must not be null or contain null key!");
		try {
			for (byte[] key : keys) {
				cluster.del(key);
			}
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
		return null;
	}

	@Override
	public DataType type(byte[] key) {
		try {
			return JedisConverters.toDataType(cluster.type(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> keys(final byte[] pattern) {

		notNull(pattern, "Pattern must not be null!");

		Collection<Set<byte[]>> keysPerNode = clusterCommandExecutor.runCommandOnAllNodes(
				new JedisCommandCallback<Set<byte[]>>() {

					@Override
					public Set<byte[]> doInCluster(Jedis client) {
						return client.keys(pattern);
					}
				}).values();

		Set<byte[]> keys = new HashSet<byte[]>();
		for (Set<byte[]> keySet : keysPerNode) {
			keys.addAll(keySet);
		}
		return keys;
	}

	@Override
	public Set<byte[]> keys(RedisClusterNode node, final byte[] pattern) {

		notNull(pattern, "Pattern must not be null!");

		return clusterCommandExecutor.runCommandOnSingleNode(new JedisCommandCallback<Set<byte[]>>() {

			@Override
			public Set<byte[]> doInCluster(Jedis client) {
				return client.keys(pattern);
			}
		}, node);
	}

	@Override
	public Cursor<byte[]> scan(ScanOptions options) {

		// TODO: add scan(RedisNode node, ScanOptions options) since we can scan keys at a single node.
		throw new UnsupportedOperationException("Scan is not supported accros multiple nodes within a cluster");
	}

	@Override
	public byte[] randomKey() {

		List<RedisClusterNode> nodes = new ArrayList<RedisClusterNode>(getClusterNodes());
		Set<RedisNode> inspectedNodes = new HashSet<RedisNode>(nodes.size());

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
	public byte[] randomKey(RedisClusterNode node) {

		return clusterCommandExecutor.runCommandOnSingleNode(new JedisCommandCallback<byte[]>() {

			@Override
			public byte[] doInCluster(Jedis client) {
				return client.randomBinaryKey();
			}
		}, node);
	}

	@Override
	public void rename(byte[] oldName, byte[] newName) {

		// TODO: check if we can do this using dump -> restore.
		throw new UnsupportedOperationException("RENAME is not supported in cluster mode");
	}

	@Override
	public Boolean renameNX(byte[] oldName, byte[] newName) {

		// TODO: check if we can do this using dump -> restore.
		throw new UnsupportedOperationException("RENAMENX is not supported in cluster mode");
	}

	@Override
	public Boolean expire(byte[] key, long seconds) {

		if (seconds > Integer.MAX_VALUE) {
			throw new UnsupportedOperationException("Jedis does not support seconds exceeding Integer.MAX_VALUE.");
		}
		try {
			return JedisConverters.toBoolean(cluster.expire(key, Long.valueOf(seconds).intValue()));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean pExpire(final byte[] key, final long millis) {

		return runClusterCommand(new JedisClusterCommand<Boolean>(getClusterConnectionHandler(), 1) {
			@Override
			public Boolean execute(Jedis connection) {
				return JedisConverters.toBoolean(connection.pexpire(key, millis));
			}
		}, key);
	}

	@Override
	public Boolean expireAt(byte[] key, long unixTime) {

		try {
			return JedisConverters.toBoolean(cluster.expireAt(key, unixTime));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean pExpireAt(final byte[] key, final long unixTimeInMillis) {

		return runClusterCommand(new JedisClusterCommand<Boolean>(getClusterConnectionHandler(), 1) {

			@Override
			public Boolean execute(Jedis connection) {
				return JedisConverters.toBoolean(connection.pexpire(key, unixTimeInMillis));
			}
		}, key);
	}

	@Override
	public Boolean persist(byte[] key) {

		try {
			return JedisConverters.toBoolean(cluster.persist(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean move(byte[] key, int dbIndex) {
		throw new UnsupportedOperationException("Cluster mode does not allow moving keys.");
	}

	@Override
	public Long ttl(byte[] key) {

		try {
			return cluster.ttl(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long pTtl(final byte[] key) {

		return runClusterCommand(new JedisClusterCommand<Long>(getClusterConnectionHandler(), 5) {

			@Override
			public Long execute(Jedis connection) {
				return connection.pttl(key);
			}
		}, key);
	}

	@Override
	public List<byte[]> sort(byte[] key, SortParameters params) {

		try {
			return cluster.sort(key, JedisConverters.toSortingParams(params));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long sort(byte[] key, SortParameters params, byte[] storeKey) {

		// TODO: Maybe we could sort and then store using new key
		throw new UnsupportedOperationException("Storing sort result is not supported in cluster mode");
	}

	@Override
	public byte[] dump(final byte[] key) {

		return runClusterCommand(new JedisClusterCommand<byte[]>(getClusterConnectionHandler(), 5) {

			@Override
			public byte[] execute(Jedis connection) {
				return connection.dump(key);
			}
		}, key);
	}

	@Override
	public void restore(final byte[] key, final long ttlInMillis, final byte[] serializedValue) {

		if (ttlInMillis > Integer.MAX_VALUE) {
			throw new UnsupportedOperationException("Jedis does not support ttlInMillis exceeding Integer.MAX_VALUE.");
		}

		runClusterCommand(new JedisClusterCommand<Void>(getClusterConnectionHandler(), 5) {

			@Override
			public Void execute(Jedis connection) {

				connection.restore(key, Long.valueOf(ttlInMillis).intValue(), serializedValue);
				return null;
			}
		}, key);
	}

	@Override
	public byte[] get(byte[] key) {

		try {
			return cluster.get(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] getSet(byte[] key, byte[] value) {

		try {
			return cluster.getSet(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> mGet(byte[]... keys) {

		// TODO: we could fetch those one by one and combine the results in code.
		throw new UnsupportedOperationException("MGET is not supported in cluster mode");
	}

	@Override
	public void set(byte[] key, byte[] value) {

		try {
			cluster.set(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean setNX(byte[] key, byte[] value) {

		try {
			return JedisConverters.toBoolean(cluster.setnx(key, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void setEx(byte[] key, long seconds, byte[] value) {

		if (seconds > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Seconds have cannot exceed Integer.MAX_VALUE!");
		}

		try {
			cluster.setex(key, Long.valueOf(seconds).intValue(), value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void pSetEx(final byte[] key, final long milliseconds, final byte[] value) {

		if (milliseconds > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Milliseconds have cannot exceed Integer.MAX_VALUE!");
		}

		runClusterCommand(new JedisClusterCommand<Void>(getClusterConnectionHandler(), 1) {

			@Override
			public Void execute(Jedis connection) {

				connection.psetex(key, Long.valueOf(milliseconds).intValue(), value);
				return null;
			}
		}, key);
	}

	@Override
	public void mSet(Map<byte[], byte[]> tuple) {

		// TODO: Need to check this since it should be possible if key prefix is correct. see
		// http://redis.io/topics/cluster-spec#multiple-keys-operations
		throw new UnsupportedOperationException("MSET is not supported in cluster mode.");
	}

	@Override
	public Boolean mSetNX(Map<byte[], byte[]> tuple) {

		// TODO: Need to check this since it should be possible if key prefix is correct. see
		// http://redis.io/topics/cluster-spec#multiple-keys-operations
		throw new UnsupportedOperationException("MSETNX is not supported in cluster mode.");
	}

	@Override
	public Long incr(byte[] key) {

		try {
			return cluster.incr(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long incrBy(byte[] key, long value) {

		try {
			return cluster.incrBy(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Double incrBy(final byte[] key, final double value) {

		return runClusterCommand(new JedisClusterCommand<Double>(getClusterConnectionHandler(), 5) {

			@Override
			public Double execute(Jedis connection) {
				return connection.incrByFloat(key, value);
			}
		}, key);
	}

	@Override
	public Long decr(byte[] key) {

		try {
			return cluster.decr(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long decrBy(byte[] key, long value) {

		try {
			return cluster.decrBy(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long append(byte[] key, byte[] value) {

		try {
			return cluster.append(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] getRange(byte[] key, long begin, long end) {

		try {
			return cluster.getrange(key, begin, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void setRange(byte[] key, byte[] value, long offset) {

		try {
			cluster.setrange(key, offset, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean getBit(byte[] key, long offset) {

		try {
			return cluster.getbit(key, offset);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean setBit(byte[] key, long offset, boolean value) {

		try {
			return cluster.setbit(key, offset, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long bitCount(byte[] key) {

		try {
			return cluster.bitcount(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long bitCount(byte[] key, long begin, long end) {

		try {
			return cluster.bitcount(key, begin, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long bitOp(BitOperation op, byte[] destination, byte[]... keys) {

		// TODO: not allowed on multiple keys, but one on same host would be ok"
		throw new UnsupportedOperationException("BITOP is not supported in cluster mode");
	}

	@Override
	public Long strLen(byte[] key) {

		try {
			return cluster.strlen(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long rPush(byte[] key, byte[]... values) {

		try {
			return cluster.rpush(key, values);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long lPush(byte[] key, byte[]... values) {

		try {
			return cluster.lpush(key, values);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long rPushX(byte[] key, byte[] value) {

		try {
			return cluster.rpushx(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long lPushX(byte[] key, byte[] value) {

		try {
			return cluster.lpushx(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long lLen(byte[] key) {

		try {
			return cluster.llen(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> lRange(byte[] key, long begin, long end) {

		try {
			return cluster.lrange(key, begin, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void lTrim(final byte[] key, final long begin, final long end) {

		runClusterCommand(new JedisClusterCommand<Void>(getClusterConnectionHandler(), 5) {

			@Override
			public Void execute(Jedis connection) {

				connection.ltrim(key, begin, end);
				return null;
			}
		}, key);

	}

	@Override
	public byte[] lIndex(byte[] key, long index) {

		try {
			return cluster.lindex(key, index);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {

		try {
			return cluster.linsert(key, JedisConverters.toListPosition(where), pivot, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void lSet(byte[] key, long index, byte[] value) {

		try {
			cluster.lset(key, index, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long lRem(byte[] key, long count, byte[] value) {

		try {
			return cluster.lrem(key, count, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] lPop(byte[] key) {

		try {
			return cluster.lpop(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] rPop(byte[] key) {

		try {
			return cluster.rpop(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> bLPop(int timeout, byte[]... keys) {

		List<byte[]> result = new ArrayList<byte[]>();
		try {
			for (byte[] key : keys) {
				result.addAll(JedisConverters.stringListToByteList().convert(
						cluster.blpop(timeout, JedisConverters.toString(key))));
			}
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
		return result;
	}

	@Override
	public List<byte[]> bRPop(int timeout, byte[]... keys) {

		List<byte[]> result = new ArrayList<byte[]>();
		try {
			for (byte[] key : keys) {

				result.addAll(JedisConverters.stringListToByteList().convert(
						cluster.brpop(timeout, JedisConverters.toString(key))));
			}
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
		return result;
	}

	@Override
	public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {

		// TODO: this is only possible of both keys map to the same slot
		throw new UnsupportedOperationException("RPOPLPUSH is not supported in cluster mode.");
	}

	@Override
	public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {

		// TODO: this is only possible of both keys map to the same slot
		throw new UnsupportedOperationException("BRPOPLPUSH is not supported in cluster mode.");
	}

	@Override
	public Long sAdd(byte[] key, byte[]... values) {

		try {
			return cluster.sadd(key, values);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long sRem(byte[] key, byte[]... values) {

		try {
			return cluster.srem(key, values);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] sPop(byte[] key) {
		try {
			return cluster.spop(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {

		// TODO: Only possible if both map to the same slot
		throw new UnsupportedOperationException("SMOVE is not supported in cluster mode.");
	}

	@Override
	public Long sCard(byte[] key) {

		try {
			return cluster.scard(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean sIsMember(byte[] key, byte[] value) {

		try {
			return cluster.sismember(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> sInter(byte[]... keys) {

		// TODO: Only works when keys map to same slot
		throw new UnsupportedOperationException("SINTER is not supported in cluster mode.");
	}

	@Override
	public Long sInterStore(byte[] destKey, byte[]... keys) {

		// TODO: Only works when keys map to same slot
		throw new UnsupportedOperationException("SINTERSTORE is not supported in cluster mode.");
	}

	@Override
	public Set<byte[]> sUnion(byte[]... keys) {

		// TODO: Only works when keys map to same slot
		throw new UnsupportedOperationException("SUNION is not supported in cluster mode.");
	}

	@Override
	public Long sUnionStore(byte[] destKey, byte[]... keys) {

		// TODO: Only works when keys map to same slot
		throw new UnsupportedOperationException("SUNIONSTORE is not supported in cluster mode.");
	}

	@Override
	public Set<byte[]> sDiff(byte[]... keys) {

		// TODO: Only works when keys map to same slot
		throw new UnsupportedOperationException("SDIFF is not supported in cluster mode.");
	}

	@Override
	public Long sDiffStore(byte[] destKey, byte[]... keys) {

		// TODO: Only works when keys map to same slot
		throw new UnsupportedOperationException("SDIFFSTORE is not supported in cluster mode.");
	}

	@Override
	public Set<byte[]> sMembers(byte[] key) {

		try {
			return cluster.smembers(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] sRandMember(byte[] key) {

		try {
			return cluster.srandmember(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> sRandMember(byte[] key, long count) {

		if (count > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Count have cannot exceed Integer.MAX_VALUE!");
		}

		try {
			return cluster.srandmember(key, Long.valueOf(count).intValue());
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Cursor<byte[]> sScan(final byte[] key, ScanOptions options) {

		return new ScanCursor<byte[]>(options) {

			@Override
			protected ScanIteration<byte[]> doScan(long cursorId, ScanOptions options) {

				redis.clients.jedis.ScanResult<String> result = cluster.sscan(JedisConverters.toString(key),
						Long.toString(cursorId));
				return new ScanIteration<byte[]>(Long.valueOf(result.getCursor()), JedisConverters.stringListToByteList()
						.convert(result.getResult()));
			}
		}.open();
	}

	@Override
	public Boolean zAdd(byte[] key, double score, byte[] value) {

		try {
			return JedisConverters.toBoolean(cluster.zadd(key, score, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zAdd(byte[] key, Set<Tuple> tuples) {

		// TODO: need to move the tuple conversion form jedisconnection.
		throw new UnsupportedOperationException();
	}

	@Override
	public Long zRem(byte[] key, byte[]... values) {

		try {
			return cluster.zrem(key, values);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}

	}

	@Override
	public Double zIncrBy(byte[] key, double increment, byte[] value) {
		try {
			return cluster.zincrby(key, increment, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zRank(byte[] key, byte[] value) {

		try {
			return cluster.zrank(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zRevRank(byte[] key, byte[] value) {

		try {
			return cluster.zrevrank(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> zRange(byte[] key, long begin, long end) {

		try {
			return cluster.zrange(key, begin, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> zRangeByLex(byte[] key) {
		return zRangeByLex(key, Range.unbounded());
	}

	@Override
	public Set<byte[]> zRangeByLex(byte[] key, Range range) {
		return zRangeByLex(key, range, null);
	}

	@Override
	public Set<byte[]> zRangeByLex(byte[] key, Range range, Limit limit) {

		Assert.notNull(range, "Range cannot be null for ZRANGEBYLEX.");

		byte[] min = JedisConverters.boundaryToBytesForZRangeByLex(range.getMin(), JedisConverters.toBytes("-"));
		byte[] max = JedisConverters.boundaryToBytesForZRangeByLex(range.getMax(), JedisConverters.toBytes("+"));

		try {
			if (limit != null) {
				return cluster.zrangeByLex(key, min, max, limit.getOffset(), limit.getCount());
			}
			return cluster.zrangeByLex(key, min, max);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRangeWithScores(byte[] key, long begin, long end) {

		try {
			return JedisConverters.toTupleSet(cluster.zrangeWithScores(key, begin, end));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {

		try {
			return cluster.zrangeByScore(key, min, max);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max) {

		try {
			return JedisConverters.toTupleSet(cluster.zrangeByScoreWithScores(key, min, max));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {

		if (offset > Integer.MAX_VALUE || count > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Count/Offset cannot exceed Integer.MAX_VALUE!");
		}

		try {
			return cluster.zrangeByScore(key, min, max, Long.valueOf(offset).intValue(), Long.valueOf(count).intValue());
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {

		if (offset > Integer.MAX_VALUE || count > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Count/Offset cannot exceed Integer.MAX_VALUE!");
		}

		try {
			return JedisConverters.toTupleSet(cluster.zrangeByScoreWithScores(key, min, max, Long.valueOf(offset).intValue(),
					Long.valueOf(count).intValue()));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> zRevRange(byte[] key, long begin, long end) {

		try {
			return cluster.zrevrange(key, begin, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRevRangeWithScores(byte[] key, long begin, long end) {

		try {
			return JedisConverters.toTupleSet(cluster.zrevrangeWithScores(key, begin, end));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max) {

		try {
			return cluster.zrevrangeByScore(key, max, min);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max) {

		try {
			return JedisConverters.toTupleSet(cluster.zrevrangeByScoreWithScores(key, max, min));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count) {

		if (offset > Integer.MAX_VALUE || count > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Count/Offset cannot exceed Integer.MAX_VALUE!");
		}

		try {
			return cluster.zrevrangeByScore(key, max, min, Long.valueOf(offset).intValue(), Long.valueOf(count).intValue());
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {

		if (offset > Integer.MAX_VALUE || count > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Count/Offset cannot exceed Integer.MAX_VALUE!");
		}

		try {
			return JedisConverters.toTupleSet(cluster.zrevrangeByScoreWithScores(key, max, min, Long.valueOf(offset)
					.intValue(), Long.valueOf(count).intValue()));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zCount(byte[] key, double min, double max) {

		try {
			return cluster.zcount(key, min, max);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zCard(byte[] key) {

		try {
			return cluster.zcard(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Double zScore(byte[] key, byte[] value) {

		try {
			return cluster.zscore(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zRemRange(byte[] key, long begin, long end) {

		try {
			return cluster.zremrangeByScore(key, begin, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zRemRangeByScore(byte[] key, double min, double max) {

		try {
			return cluster.zremrangeByScore(key, min, max);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zUnionStore(byte[] destKey, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long zInterStore(byte[] destKey, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Cursor<Tuple> zScan(byte[] key, ScanOptions options) {
		throw new UnsupportedOperationException("this seems to be possible need to check cursor callback impl.");
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, String min, String max) {

		try {
			return cluster.zrangeByScore(key, JedisConverters.toBytes(min), JedisConverters.toBytes(max));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, String min, String max, long offset, long count) {

		if (offset > Integer.MAX_VALUE || count > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Count/Offset cannot exceed Integer.MAX_VALUE!");
		}

		try {
			return cluster.zrangeByScore(key, JedisConverters.toBytes(min), JedisConverters.toBytes(max), Long
					.valueOf(offset).intValue(), Long.valueOf(count).intValue());
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean hSet(byte[] key, byte[] field, byte[] value) {

		try {
			return JedisConverters.toBoolean(cluster.hset(key, field, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {

		try {
			return JedisConverters.toBoolean(cluster.hsetnx(key, field, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] hGet(byte[] key, byte[] field) {

		try {
			return cluster.hget(key, field);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> hMGet(byte[] key, byte[]... fields) {

		try {
			return cluster.hmget(key, fields);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void hMSet(byte[] key, Map<byte[], byte[]> hashes) {

		try {
			cluster.hmset(key, hashes);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long hIncrBy(byte[] key, byte[] field, long delta) {

		try {
			return cluster.hincrBy(key, field, delta);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Double hIncrBy(byte[] key, byte[] field, double delta) {
		try {
			return cluster.hincrByFloat(key, field, delta);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean hExists(byte[] key, byte[] field) {

		try {
			return cluster.hexists(key, field);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long hDel(byte[] key, byte[]... fields) {

		try {
			return cluster.hdel(key, fields);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long hLen(byte[] key) {

		try {
			return cluster.hlen(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> hKeys(byte[] key) {

		try {
			return cluster.hkeys(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> hVals(byte[] key) {

		try {
			return new ArrayList<byte[]>(cluster.hvals(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Map<byte[], byte[]> hGetAll(byte[] key) {

		try {
			return cluster.hgetAll(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Cursor<Entry<byte[], byte[]>> hScan(final byte[] key, ScanOptions options) {

		return new ScanCursor<Map.Entry<byte[], byte[]>>(options) {

			@Override
			protected ScanIteration<Entry<byte[], byte[]>> doScan(long cursorId, ScanOptions options) {
				throw new UnsupportedOperationException("Jedis does currently not support binary hscan");
			}
		}.open();
	}

	@Override
	public void multi() {
		throw new UnsupportedOperationException(
				"Well not really its just that all subsequent ops have to use same connection und must be performed on same slot");
	}

	@Override
	public List<Object> exec() {
		throw new UnsupportedOperationException(
				"Well not really its just that all subsequent ops have to use same connection und must be performed on same slot");
	}

	@Override
	public void discard() {
		throw new UnsupportedOperationException(
				"Well not really its just that all subsequent ops have to use same connection und must be performed on same slot");
	}

	@Override
	public void watch(byte[]... keys) {
		throw new UnsupportedOperationException(
				"Well not really its just that all subsequent ops have to use same connection und must be performed on same slot");
	}

	@Override
	public void unwatch() {
		throw new UnsupportedOperationException(
				"Well not really its just that all subsequent ops have to use same connection und must be performed on same slot");

	}

	@Override
	public boolean isSubscribed() {
		return false;
	}

	@Override
	public Subscription getSubscription() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long publish(byte[] channel, byte[] message) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void subscribe(MessageListener listener, byte[]... channels) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void pSubscribe(MessageListener listener, byte[]... patterns) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void select(final int dbIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] echo(final byte[] message) {

		return clusterCommandExecutor.runCommandOnArbitraryNode(new JedisCommandCallback<byte[]>() {

			@Override
			public byte[] doInCluster(Jedis client) {
				return client.echo(message);
			}
		});
	}

	@Override
	public String ping() {

		return !clusterCommandExecutor.runCommandOnAllNodes(new JedisCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.ping();
			}
		}).isEmpty() ? "PONG" : null;

	}

	@Override
	public String ping(RedisClusterNode node) {

		return clusterCommandExecutor.runCommandOnSingleNode(new JedisCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.ping();
			}
		}, node);
	}

	@Override
	public void bgWriteAof() {

		clusterCommandExecutor.runCommandOnAllNodes(new JedisCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.bgrewriteaof();
			}
		});
	}

	@Override
	public void bgReWriteAof(RedisClusterNode node) {

		clusterCommandExecutor.runCommandOnSingleNode(new JedisCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.bgrewriteaof();
			}
		}, node);
	}

	@Override
	public void bgReWriteAof() {

		clusterCommandExecutor.runCommandOnAllNodes(new JedisCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.bgrewriteaof();
			}
		});
	}

	@Override
	public void bgSave() {

		clusterCommandExecutor.runCommandOnAllNodes(new JedisCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.bgsave();
			}
		});
	}

	@Override
	public void bgSave(RedisClusterNode node) {

		clusterCommandExecutor.runCommandOnSingleNode(new JedisCommandCallback<Void>() {

			@Override
			public Void doInCluster(Jedis client) {
				client.bgsave();
				return null;
			}
		}, node);
	}

	@Override
	public Long lastSave() {

		List<Long> result = new ArrayList<Long>(clusterCommandExecutor.runCommandOnAllNodes(
				new JedisCommandCallback<Long>() {

					@Override
					public Long doInCluster(Jedis client) {
						return client.lastsave();
					}
				}).values());

		if (CollectionUtils.isEmpty(result)) {
			return null;
		}

		Collections.sort(result, Collections.reverseOrder());
		return result.get(0);
	}

	@Override
	public Long lastSave(RedisClusterNode node) {

		return clusterCommandExecutor.runCommandOnSingleNode(new JedisCommandCallback<Long>() {

			@Override
			public Long doInCluster(Jedis client) {
				return client.lastsave();
			}
		}, node);
	}

	@Override
	public void save() {

		clusterCommandExecutor.runCommandOnAllNodes(new JedisCommandCallback<Void>() {

			@Override
			public Void doInCluster(Jedis client) {
				client.save();
				return null;
			}
		});
	}

	@Override
	public void save(RedisClusterNode node) {

		clusterCommandExecutor.runCommandOnSingleNode(new JedisCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.save();
			}
		}, node);

	}

	@Override
	public Long dbSize() {

		Map<RedisClusterNode, Long> dbSizes = clusterCommandExecutor.runCommandOnAllNodes(new JedisCommandCallback<Long>() {

			@Override
			public Long doInCluster(Jedis client) {
				return client.dbSize();
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

	@Override
	public Long dbSize(RedisClusterNode node) {

		return clusterCommandExecutor.runCommandOnSingleNode(new JedisCommandCallback<Long>() {

			@Override
			public Long doInCluster(Jedis client) {
				return client.dbSize();
			}
		}, node);
	}

	@Override
	public void flushDb() {

		clusterCommandExecutor.runCommandOnAllNodes(new JedisCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.flushDB();
			}
		});
	}

	@Override
	public void flushDb(RedisClusterNode node) {

		clusterCommandExecutor.runCommandOnSingleNode(new JedisCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.flushDB();
			}
		}, node);
	}

	@Override
	public void flushAll() {

		clusterCommandExecutor.runCommandOnAllNodes(new JedisCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.flushAll();
			}
		});
	}

	@Override
	public void flushAll(RedisClusterNode node) {

		clusterCommandExecutor.runCommandOnSingleNode(new JedisCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.flushAll();
			}
		}, node);
	}

	@Override
	public Properties info() {

		Properties infos = new Properties();

		infos.putAll(clusterCommandExecutor.runCommandOnAllNodes(new JedisCommandCallback<Properties>() {

			@Override
			public Properties doInCluster(Jedis client) {
				return JedisConverters.toProperties(client.info());
			}
		}));

		return infos;
	}

	@Override
	public Properties info(RedisClusterNode node) {

		return clusterCommandExecutor.runCommandOnSingleNode(new JedisCommandCallback<Properties>() {

			@Override
			public Properties doInCluster(Jedis client) {
				return JedisConverters.toProperties(client.info());
			}
		}, node);
	}

	@Override
	public Properties info(String section) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void shutdown() {

		clusterCommandExecutor.runCommandOnAllNodes(new JedisCommandCallback<Void>() {

			@Override
			public Void doInCluster(Jedis client) {
				client.shutdown();
				return null;
			}
		});
	}

	@Override
	public void shutdown(RedisClusterNode node) {

		clusterCommandExecutor.runCommandOnSingleNode(new JedisCommandCallback<Void>() {

			@Override
			public Void doInCluster(Jedis client) {
				client.shutdown();
				return null;
			}
		}, node);

	}

	@Override
	public void shutdown(ShutdownOption option) {
		throw new UnsupportedOperationException("TODO: can be done using eval and shutdown script");
	}

	@Override
	public List<String> getConfig(String pattern) {
		throw new UnsupportedOperationException("Cannot get config from multiple Nodes since return type does not match");
	}

	@Override
	public void setConfig(String param, String value) {
		throw new UnsupportedOperationException("Cannot get config from multiple Nodes since return type does not match");
	}

	@Override
	public void resetConfigStats() {
		// TODO Auto-generated method stub

	}

	@Override
	public Long time() {
		throw new UnsupportedOperationException("Need to use a single host to do so");
	}

	@Override
	public void killClient(String host, int port) {
		throw new UnsupportedOperationException("Requires to have a specific client.");

	}

	@Override
	public void setClientName(byte[] name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getClientName() {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<RedisClientInfo> getClientList() {

		Map<RedisClusterNode, List<RedisClientInfo>> map = clusterCommandExecutor
				.runCommandOnAllNodes(new JedisCommandCallback<List<RedisClientInfo>>() {

					@Override
					public List<RedisClientInfo> doInCluster(Jedis client) {
						return JedisConverters.toListOfRedisClientInformation(client.clientList());
					}
				});

		ArrayList<RedisClientInfo> result = new ArrayList<RedisClientInfo>();
		for (List<RedisClientInfo> infos : map.values()) {
			result.addAll(infos);
		}
		return result;

	}

	@Override
	public void slaveOf(String host, int port) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void slaveOfNoOne() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void scriptFlush() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void scriptKill() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String scriptLoad(byte[] script) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<Boolean> scriptExists(String... scriptShas) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T eval(byte[] script, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T evalSha(String scriptSha, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T evalSha(byte[] scriptSha, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long pfAdd(byte[] key, byte[]... values) {

		try {
			return cluster.pfadd(key, values);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long pfCount(byte[]... keys) {
		throw new UnsupportedOperationException("well that would work if keys are on same slot");
	}

	@Override
	public void pfMerge(byte[] destinationKey, byte[]... sourceKeys) {
		throw new UnsupportedOperationException("well that would work if keys are on same slot");
	}

	@Override
	public Boolean exists(final byte[] key) {
		return runClusterCommand(new JedisClusterCommand<Boolean>(getClusterConnectionHandler(), 5) {

			@Override
			public Boolean execute(Jedis connection) {
				return connection.exists(key);
			}
		}, key);
	}

	@Override
	public void clusterSetSlot(RedisClusterNode node, final int slot, final AddSlots mode) {

		clusterCommandExecutor.runCommandOnSingleNode(new JedisCommandCallback<Void>() {

			@Override
			public Void doInCluster(Jedis client) {
				throw new RuntimeException();
			}
		}, node);

	}

	@Override
	public List<byte[]> getKeysInSlot(final int slot, final Integer count) {

		RedisClusterNode node = getClusterNodeForSlot(slot);

		clusterCommandExecutor.runCommandOnSingleNode(new JedisCommandCallback<List<byte[]>>() {

			@Override
			public List<byte[]> doInCluster(Jedis client) {
				return JedisConverters.stringListToByteList().convert(
						client.clusterGetKeysInSlot(slot, count != null ? count.intValue() : Integer.MAX_VALUE));
			}
		}, node);
		return null;
	}

	/*
	 * --> Cluster Commands
	 */

	@Override
	public void addSlots(RedisClusterNode node, final int... slots) {

		clusterCommandExecutor.runCommandOnSingleNode(new JedisCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {

				return client.clusterAddSlots(slots);
			}
		}, node);

	}

	@Override
	public Long countKeys(final int slot) {

		RedisClusterNode node = getClusterNodeForSlot(slot);

		return clusterCommandExecutor.runCommandOnSingleNode(new JedisCommandCallback<Long>() {

			@Override
			public Long doInCluster(Jedis client) {

				return client.clusterCountKeysInSlot(slot);
			}
		}, node);

	}

	@Override
	public void deleteSlots(RedisClusterNode node, final int... slots) {

		clusterCommandExecutor.runCommandOnSingleNode(new JedisCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.clusterDelSlots(slots);
			}
		}, node);

	}

	@Override
	public void clusterForget(final RedisClusterNode node) {

		Set<RedisClusterNode> nodes = getClusterNodes();
		nodes.remove(node);

		clusterCommandExecutor.runCommandAsyncOnNodes(new JedisCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.clusterForget(node.getId());
			}
		}, nodes);

	}

	@Override
	public void clusterMeet(final RedisClusterNode node) {

		Assert.notNull(node, "Node to meet cluster must not be null!");

		clusterCommandExecutor.runCommandOnAllNodes(new JedisCommandCallback<Void>() {

			@Override
			public Void doInCluster(Jedis client) {

				client.clusterMeet(node.getHost(), node.getPort());
				return null;
			}
		});
	}

	@Override
	public void clusterReplicate(final RedisClusterNode master, RedisClusterNode slave) {

		clusterCommandExecutor.runCommandOnSingleNode(new JedisCommandCallback<Void>() {

			@Override
			public Void doInCluster(Jedis client) {

				client.clusterReplicate(master.getId());
				return null;
			}
		}, slave);

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#getClusterSlotForKey(byte[])
	 */
	@Override
	public Integer getClusterSlotForKey(final byte[] key) {

		return clusterCommandExecutor.runCommandOnArbitraryNode(new JedisCommandCallback<Integer>() {

			@Override
			public Integer doInCluster(Jedis client) {
				return client.clusterKeySlot(JedisConverters.toString(key)).intValue();
			}
		});
	}

	@Override
	public RedisClusterNode getClusterNodeForSlot(int slot) {

		for (RedisClusterNode node : getClusterNodes()) {
			if (node.servesSlot(slot)) {
				return node;
			}
		}

		return null;
	}

	@Override
	public Set<RedisClusterNode> getClusterNodes() {

		return clusterCommandExecutor.runCommandOnArbitraryNode(new JedisCommandCallback<Set<RedisClusterNode>>() {

			@Override
			public Set<RedisClusterNode> doInCluster(Jedis client) {
				return JedisConverters.toSetOfRedisClusterNodes(client.clusterNodes());
			}
		});
	}

	@Override
	public Set<RedisClusterNode> getClusterSlaves(final RedisClusterNode master) {

		return clusterCommandExecutor.runCommandOnSingleNode(new JedisCommandCallback<Set<RedisClusterNode>>() {

			@Override
			public Set<RedisClusterNode> doInCluster(Jedis client) {
				return JedisConverters.toSetOfRedisClusterNodes(client.clusterSlaves(master.getId()));
			}
		}, master);
	}

	@Override
	public RedisClusterNode getClusterNodeForKey(byte[] key) {
		return getClusterNodeForSlot(getClusterSlotForKey(key));
	}

	@Override
	public ClusterInfo getClusterInfo() {

		return clusterCommandExecutor.runCommandOnArbitraryNode(new JedisCommandCallback<ClusterInfo>() {

			@Override
			public ClusterInfo doInCluster(Jedis client) {
				return new ClusterInfo(JedisConverters.toProperties(client.clusterInfo()));
			}
		});
	}

	/*
	 * --> Little helpers to make it work
	 */

	private <T> T runClusterCommand(JedisClusterCommand<T> cmd, byte[] key) {
		try {
			return cmd.runBinary(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	protected DataAccessException convertJedisAccessException(Exception ex) {
		return EXCEPTION_TRANSLATION.translate(ex);
	}

	protected JedisPool getResourcePoolForSpecificNode(RedisNode node) {

		notNull(node, "Cannot get Pool for 'null' node!");

		Map<String, JedisPool> clusterNodes = cluster.getClusterNodes();
		if (clusterNodes.containsKey(node.asString())) {
			return clusterNodes.get(node.asString());
		}

		return null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Jedis getResourceForSpecificNode(RedisClusterNode node) {

		JedisPool pool = getResourcePoolForSpecificNode(node);
		if (pool != null) {
			return pool.getResource();
		}

		throw new IllegalArgumentException(String.format("Node %s is unknown to cluster", node));
	}

	@Override
	public void returnResourceForSpecificNode(RedisClusterNode node, Object client) {
		getResourcePoolForSpecificNode(node).returnResource((Jedis) client);
	}

	private JedisClusterConnectionHandler getClusterConnectionHandler() {
		return (JedisClusterConnectionHandler) getField(CONNECTION_HANDLER, cluster);
	}

	protected interface JedisCommandCallback<T> extends ClusterCommandCallback<Jedis, T> {}

	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range) {
		throw new UnsupportedOperationException("TODO: implement me!");
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range, Limit limit) {
		throw new UnsupportedOperationException("TODO: implement me!");
	}

	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, Range range) {
		throw new UnsupportedOperationException("TODO: implement me!");
	}

	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, Range range, Limit limit) {
		throw new UnsupportedOperationException("TODO: implement me!");
	}

	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range) {
		throw new UnsupportedOperationException("TODO: implement me!");
	}

	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range, Limit limit) {
		throw new UnsupportedOperationException("TODO: implement me!");
	}

	@Override
	public Long zCount(byte[] key, Range range) {
		throw new UnsupportedOperationException("TODO: implement me!");
	}

	@Override
	public Long zRemRangeByScore(byte[] key, Range range) {
		throw new UnsupportedOperationException("TODO: implement me!");
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, Range range) {
		throw new UnsupportedOperationException("TODO: implement me!");
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, Range range, Limit limit) {
		throw new UnsupportedOperationException("TODO: implement me!");
	}

	static class JedisClusterTopologyProvider implements ClusterTopologyProvider {

		private final JedisCluster cluster;

		public JedisClusterTopologyProvider(JedisCluster cluster) {
			this.cluster = cluster;
		}

		@Override
		public ClusterTopology getTopology() {

			for (Entry<String, JedisPool> entry : cluster.getClusterNodes().entrySet()) {
				Jedis jedis = entry.getValue().getResource();

				try {
					Set<RedisClusterNode> nodes = JedisConverters.toSetOfRedisClusterNodes(jedis.clusterNodes());
					return new ClusterTopology(nodes);
				} catch (Exception e) {
					// ingnore for now
				} finally {
					if (jedis != null) {
						entry.getValue().returnResource(jedis);
					}
				}
			}

			throw new IllegalArgumentException("could not get cluster info");
		}

	}

	@Override
	public ClusterTopology getTopology() {
		return this.topologyProvider.getTopology();
	}

}
