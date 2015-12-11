/*
 * Copyright 2013-2014 the original author or authors.
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
package org.springframework.data.redis.connection.convert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterNode.SlotRange;
import org.springframework.data.redis.connection.RedisNode.NodeType;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.util.CollectionUtils;
import org.springframework.util.NumberUtils;
import org.springframework.util.StringUtils;

/**
 * Common type converters
 * 
 * @author Jennifer Hickey
 * @author Thomas Darimont
 */
abstract public class Converters {

	private static final byte[] ONE = new byte[] { '1' };
	private static final byte[] ZERO = new byte[] { '0' };
	private static final Converter<String, Properties> STRING_TO_PROPS = new StringToPropertiesConverter();
	private static final Converter<Long, Boolean> LONG_TO_BOOLEAN = new LongToBooleanConverter();
	private static final Converter<String, DataType> STRING_TO_DATA_TYPE = new StringToDataTypeConverter();
	private static final Converter<Map<?, ?>, Properties> MAP_TO_PROPERTIES = MapToPropertiesConverter.INSTANCE;
	private static final Converter<String, RedisClusterNode> STRING_TO_CLUSTER_NODE_CONVERTER;

	static {

		STRING_TO_CLUSTER_NODE_CONVERTER = new Converter<String, RedisClusterNode>() {

			static final int ID_INDEX = 0;
			static final int HOST_PORT_INDEX = 1;
			static final int FLAGS_INDEX = 2;
			static final int MASTER_ID_INDEX = 3;
			static final int SLOTS_INDEX = 8;

			@Override
			public RedisClusterNode convert(String source) {

				String[] args = source.split(" ");
				String[] hostAndPort = StringUtils.split(args[HOST_PORT_INDEX], ":");

				SlotRange range = null;
				if (args.length > SLOTS_INDEX) {
					if (args[SLOTS_INDEX].contains("-")) {
						String[] slotRange = StringUtils.split(args[SLOTS_INDEX], "-");

						if (slotRange != null) {
							range = new RedisClusterNode.SlotRange(Integer.valueOf(slotRange[0]), Integer.valueOf(slotRange[1]));
						}
					} else {
						range = new SlotRange(Integer.valueOf(args[SLOTS_INDEX]), Integer.valueOf(args[SLOTS_INDEX]));
					}
				}

				RedisClusterNode node = new RedisClusterNode(hostAndPort[0], Integer.valueOf(hostAndPort[1]), range);
				node.setId(args[ID_INDEX]);

				if (!args[MASTER_ID_INDEX].isEmpty() && !args[MASTER_ID_INDEX].startsWith("-")) {
					node.setMasterId(args[MASTER_ID_INDEX]);
				}

				if (args[FLAGS_INDEX].contains("master")) {
					node.setType(NodeType.MASTER);
				} else if (args[FLAGS_INDEX].contains("slave")) {
					node.setType(NodeType.SLAVE);
				}

				return node;
			}

		};
	}

	public static Converter<String, Properties> stringToProps() {
		return STRING_TO_PROPS;
	}

	public static Converter<Long, Boolean> longToBoolean() {
		return LONG_TO_BOOLEAN;
	}

	public static Converter<String, DataType> stringToDataType() {
		return STRING_TO_DATA_TYPE;
	}

	public static Properties toProperties(String source) {
		return STRING_TO_PROPS.convert(source);
	}

	public static Properties toProperties(Map<?, ?> source) {
		return MAP_TO_PROPERTIES.convert(source);
	}

	public static Boolean toBoolean(Long source) {
		return LONG_TO_BOOLEAN.convert(source);
	}

	public static DataType toDataType(String source) {
		return STRING_TO_DATA_TYPE.convert(source);
	}

	public static byte[] toBit(Boolean source) {
		return (source ? ONE : ZERO);
	}

	/**
	 * Converts the result of a single line of {@code CLUSTER NODES} into a {@link RedisClusterNode}.
	 * 
	 * @param clusterNodesLine
	 * @return
	 * @since 1.7
	 */
	protected static RedisClusterNode toClusterNode(String clusterNodesLine) {
		return STRING_TO_CLUSTER_NODE_CONVERTER.convert(clusterNodesLine);
	}

	/**
	 * Converts the result of {@code CLUSTER NODES} into {@link RedisClusterNode}s.
	 * 
	 * @param clusterNodes
	 * @return
	 * @since 1.7
	 */
	public static Set<RedisClusterNode> toSetOfRedisClusterNodes(Collection<String> lines) {

		if (CollectionUtils.isEmpty(lines)) {
			return Collections.emptySet();
		}

		Set<RedisClusterNode> nodes = new LinkedHashSet<RedisClusterNode>(lines.size());

		for (String line : lines) {
			nodes.add(toClusterNode(line));
		}

		return nodes;
	}

	public static List<Object> toObjects(Set<Tuple> tuples) {
		List<Object> tupleArgs = new ArrayList<Object>(tuples.size() * 2);
		for (Tuple tuple : tuples) {
			tupleArgs.add(tuple.getScore());
			tupleArgs.add(tuple.getValue());
		}
		return tupleArgs;
	}

	/**
	 * Returns the timestamp constructed from the given {@code seconds} and {@code microseconds}.
	 *
	 * @param seconds server time in seconds
	 * @param microseconds elapsed microseconds in current second
	 * @return
	 */
	public static Long toTimeMillis(String seconds, String microseconds) {
		return NumberUtils.parseNumber(seconds, Long.class) * 1000L + NumberUtils.parseNumber(microseconds, Long.class)
				/ 1000L;
	}
}
