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

/**
 * @author Christoph Strobl
 */
public abstract class ClusterTestVariables {

	public static final String KEY_1 = "key1";
	public static final String KEY_2 = "key2";
	public static final String KEY_3 = "key3";

	public static final String VALUE_1 = "value1";
	public static final String VALUE_2 = "value2";
	public static final String VALUE_3 = "value3";

	public static final String SAME_SLOT_KEY_1 = "key2660";
	public static final String SAME_SLOT_KEY_2 = "key7112";
	public static final String SAME_SLOT_KEY_3 = "key8885";

	public static final String CLUSTER_HOST = "127.0.0.1";
	public static final int MASTER_NODE_1_PORT = 7379;
	public static final int MASTER_NODE_2_PORT = 7380;
	public static final int MASTER_NODE_3_PORT = 7381;
	public static final int SLAVEOF_NODE_1_PORT = 7382;

	private ClusterTestVariables() {}

}
