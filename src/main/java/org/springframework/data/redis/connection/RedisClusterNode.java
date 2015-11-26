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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * Representation of a Redis server within the cluster.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public class RedisClusterNode extends RedisNode {

	private final SlotRange slotRange;

	/**
	 * @param host must not be {@literal null}.
	 * @param port
	 * @param slotRange can be {@literal null}.
	 */
	public RedisClusterNode(String host, int port, SlotRange slotRange) {

		super(host, port);
		this.slotRange = slotRange != null ? slotRange : new SlotRange(Collections.<Integer> emptySet());
	}

	/**
	 * Get the served {@link SlotRange}.
	 * 
	 * @return never {@literal null}.
	 */
	public SlotRange getSlotRange() {
		return slotRange;
	}

	/**
	 * @param slot
	 * @return true if slot is covered.
	 */
	public boolean servesSlot(int slot) {
		return slotRange.contains(slot);
	}

	@Override
	public String toString() {
		return super.toString();
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	public static class SlotRange {

		private final Set<Integer> range;

		/**
		 * @param lowerBound must not be {@literal null}.
		 * @param upperBound must not be {@literal null}.
		 */
		public SlotRange(Integer lowerBound, Integer upperBound) {

			Assert.notNull(lowerBound, "LowerBound must not be null!");
			Assert.notNull(upperBound, "LowerBound must not be null!");

			this.range = new LinkedHashSet<Integer>();
			for (int i = lowerBound; i <= upperBound; i++) {
				this.range.add(i);
			}
		}

		public SlotRange(Collection<Integer> range) {
			this.range = CollectionUtils.isEmpty(range) ? Collections.<Integer> emptySet()
					: new LinkedHashSet<Integer>(range);
		}

		@Override
		public String toString() {
			return range.toString();
		}

		/**
		 * @param slot
		 * @return true when slot is part of the range.
		 */
		public boolean contains(int slot) {
			return range.contains(slot);
		}
	}
}
