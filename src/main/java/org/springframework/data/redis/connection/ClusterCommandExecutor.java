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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 */
public class ClusterCommandExecutor implements DisposableBean {

	private AsyncTaskExecutor executor;
	private ClusterTopologyProvider topologyProvider;
	private ClusterNodeResourceProvider resourceProvider;
	private ExceptionTranslationStrategy exceptionTranslationStrategy;

	public ClusterCommandExecutor(ClusterTopologyProvider topologyProvider, ClusterNodeResourceProvider resourceProvider,
			ExceptionTranslationStrategy exceptionTranslation) {

		Assert.notNull(topologyProvider);
		Assert.notNull(resourceProvider);
		Assert.notNull(exceptionTranslation);

		this.topologyProvider = topologyProvider;
		this.resourceProvider = resourceProvider;
		this.exceptionTranslationStrategy = exceptionTranslation;
	}

	public ClusterCommandExecutor(ClusterTopologyProvider topologyProvider, ClusterNodeResourceProvider resourceProvider,
			ExceptionTranslationStrategy exceptionTranslation, AsyncTaskExecutor executor) {

		this(topologyProvider, resourceProvider, exceptionTranslation);
		this.executor = executor;
	}

	{
		if (executor == null) {
			ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
			threadPoolTaskExecutor.initialize();
			this.executor = threadPoolTaskExecutor;
		}
	}

	public interface ClusterCommandCallback<T, S> {
		S doInCluster(T client);
	}

	public <T> T runCommandOnArbitraryNode(ClusterCommandCallback<?, T> cmd) {

		List<RedisClusterNode> nodes = new ArrayList<RedisClusterNode>(getClusterTopology().getNodes());
		return runCommandOnSingleNode(cmd, nodes.get(new Random().nextInt(nodes.size())));
	}

	public <S, T> T runCommandOnSingleNode(ClusterCommandCallback<S, T> cmd, RedisClusterNode node) {

		Assert.notNull(cmd, "Callback must not be null!");

		S client = this.resourceProvider.getResourceForSpecificNode(node);
		Assert.notNull(client, "Node not know in cluster. Is your cluster info up to date");

		try {
			return cmd.doInCluster(client);
		} catch (RuntimeException ex) {
			RuntimeException translatedException = convertToDataAccessExeption(ex);
			throw translatedException != null ? translatedException : ex;
		} finally {
			this.resourceProvider.returnResourceForSpecificNode(node, client);
		}
	}

	public <S, T> Map<RedisClusterNode, T> runCommandOnAllNodes(final ClusterCommandCallback<S, T> cmd) {
		return runCommandAsyncOnNodes(cmd, getClusterTopology().getMasterNodes());
	}

	public <S, T> java.util.Map<RedisClusterNode, T> runCommandAsyncOnNodes(final ClusterCommandCallback<S, T> callback,
			Iterable<RedisClusterNode> nodes) {

		Assert.notNull(callback, "Callback must not be null!");
		Assert.notNull(nodes, "Nodes must not be null!");

		Map<RedisClusterNode, Future<T>> futures = new LinkedHashMap<RedisClusterNode, Future<T>>();
		for (final RedisClusterNode node : nodes) {

			futures.put(node, executor.submit(new Callable<T>() {

				@Override
				public T call() throws Exception {
					return runCommandOnSingleNode(callback, node);
				}
			}));
		}

		return collectResults(futures);
	}

	private <T> Map<RedisClusterNode, T> collectResults(Map<RedisClusterNode, Future<T>> futures) {

		boolean done = false;

		Map<RedisClusterNode, T> result = new HashMap<RedisClusterNode, T>();
		Map<RedisClusterNode, Throwable> exceptions = new HashMap<RedisClusterNode, Throwable>();
		while (!done) {

			done = true;
			for (Map.Entry<RedisClusterNode, Future<T>> entry : futures.entrySet()) {

				if (!entry.getValue().isDone() && !entry.getValue().isCancelled()) {
					done = false;
				} else {
					if (!result.containsKey(entry.getKey()) && !exceptions.containsKey(entry.getKey())) {
						try {
							result.put(entry.getKey(), entry.getValue().get());
						} catch (ExecutionException e) {

							RuntimeException ex = convertToDataAccessExeption((Exception) e.getCause());
							exceptions.put(entry.getKey(), ex != null ? ex : e.getCause());
						} catch (InterruptedException e) {

							RuntimeException ex = convertToDataAccessExeption((Exception) e.getCause());
							exceptions.put(entry.getKey(), ex != null ? ex : e.getCause());
						}
					}
				}
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {

				done = true;
				Thread.currentThread().interrupt();
			}
		}

		if (!exceptions.isEmpty()) {
			throw new ClusterCommandExecutionFailureException(new ArrayList<Throwable>(exceptions.values()));
		}
		return result;
	}

	private ClusterTopology getClusterTopology() {
		return this.topologyProvider.getTopology();
	}

	private DataAccessException convertToDataAccessExeption(Exception e) {
		return exceptionTranslationStrategy.translate(e);
	}

	@Override
	public void destroy() throws Exception {

		if (executor instanceof DisposableBean) {
			((DisposableBean) executor).destroy();
		}
	}
}
