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
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @author Christoph Strobl
 * @since 1.6
 */
public class RedisClusterNode extends RedisNode {

	private final SlotRange slotRange;

	public RedisClusterNode(String host, int port, SlotRange slotRange) {
		super(host, port);
		this.slotRange = slotRange;
	}

	public SlotRange getSlotRange() {
		return slotRange;
	}

	public boolean servesSlot(int slot) {

		if (slotRange == null) {
			return false;
		}
		return slotRange.contains(slot);
	}

	@Override
	public String toString() {
		return super.toString();
	}

	public static class SlotRange {

		private Set<Integer> range;

		public SlotRange(Integer lowerBound, Integer upperBound) {

			this.range = new LinkedHashSet<Integer>();
			for (int i = lowerBound; i <= upperBound; i++) {
				this.range.add(i);
			}
		}

		public SlotRange(Collection<Integer> range) {
			this.range = new LinkedHashSet<Integer>(range);
		}

		@Override
		public String toString() {
			return range.toString();
		}

		public boolean contains(int slot) {
			return range.contains(slot);
		}

	}
}
