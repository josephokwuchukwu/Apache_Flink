/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.utils.ZKPaths;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.state.StateHandleProvider;
import org.apache.flink.util.InstantiationUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * State handles backed by ZooKeeper.
 *
 * <p>Added state is persisted via {@link StateHandle}s, which in turn are written to
 * ZooKeeper. This level of indirection is necessary to keep the amount of data in ZooKeeper
 * small. ZooKeeper is build for data in the KB range whereas state can grow to multiple MBs.
 *
 * <p>State modifications require some care, because it is possible that certain failures bring
 * the state handle backend and ZooKeeper out of sync.
 *
 * <p>ZooKeeper holds the ground truth about state handles, i.e. the following holds:
 *
 * <pre>
 * State handle in ZooKeeper => State handle exists
 * </pre>
 *
 * But not:
 *
 * <pre>
 * State handle exists => State handle in ZooKeeper
 * </pre>
 *
 * There can be lingering state handles when failures happen during operation. They
 * need to be cleaned up manually (see <a href="https://issues.apache.org/jira/browse/FLINK-2513">
 * FLINK-2513</a> about a possible way to overcome this).
 *
 * @param <T> Type of state
 */
public class ZooKeeperStateHandleStore<T extends Serializable> {

	/** Curator ZooKeeper client */
	private final CuratorFramework client;

	/** State handle provider */
	private final StateHandleProvider<T> stateHandleProvider;

	/**
	 * Creates a {@link ZooKeeperStateHandleStore}.
	 *
	 * @param client              The Curator ZooKeeper client. <strong>Important:</strong> It is
	 *                            expected that the client's namespace ensures that the root
	 *                            path is exclusive for all state handles managed by this
	 *                            instance, e.g. <code>client.usingNamespace("/stateHandles")</code>
	 * @param stateHandleProvider The state handle provider for the state
	 */
	public ZooKeeperStateHandleStore(
			CuratorFramework client,
			StateHandleProvider<T> stateHandleProvider) {

		this.client = checkNotNull(client, "Curator client");
		this.stateHandleProvider = checkNotNull(stateHandleProvider, "State handle provider");
	}

	/**
	 * Creates a state handle and stores it in ZooKeeper with create mode {@link
	 * CreateMode#PERSISTENT}.
	 *
	 * @see #add(String, Serializable, CreateMode)
	 */
	public ZooKeeperStateHandle<T> add(String pathInZooKeeper, T state) throws Exception {
		return add(pathInZooKeeper, state, CreateMode.PERSISTENT);
	}

	/**
	 * Creates a state handle and stores it in ZooKeeper.
	 *
	 * <p><strong>Important</strong>: This will <em>not</em> store the actual state in
	 * ZooKeeper, but create a state handle and store it in ZooKeeper. This level of indirection
	 * makes sure that data in ZooKeeper is small.
	 *
	 * @param pathInZooKeeper Destination path in ZooKeeper (expected to *not* exist yet and
	 *                        start with a '/')
	 * @param state           State to be added
	 * @param createMode      The create mode for the new path in ZooKeeper
	 * @return Created {@link ZooKeeperStateHandle}
	 * @throws Exception
	 */
	public ZooKeeperStateHandle<T> add(String pathInZooKeeper, T state, CreateMode createMode) throws Exception {
		checkNotNull(pathInZooKeeper, "Path in ZooKeeper");
		checkNotNull(state, "State");

		// Create the state handle. Nothing persisted yet.
		StateHandle<T> stateHandle = stateHandleProvider.createStateHandle(state);

		boolean success = false;

		try {
			// Serialize the state handle. This writes the state to the backend.
			byte[] serializedStateHandle = InstantiationUtil.serializeObject(stateHandle);

			// Write state handle (not the actual state) to ZooKeeper. This is expected to be
			// smaller than the state itself. This level of indirection makes sure that data in
			// ZooKeeper is small, because ZooKeeper is designed for data in the KB range, but
			// the state can be larger.
			client
					.create()
					.withMode(createMode)
					.forPath(pathInZooKeeper, serializedStateHandle);

			success = true;

			return new ZooKeeperStateHandle<>(stateHandle, pathInZooKeeper);
		}
		finally {
			if (!success) {
				// Cleanup the state handle if it was not written to ZooKeeper.
				if (stateHandle != null) {
					stateHandle.discardState();
				}
			}
		}
	}

	/**
	 * Replaces a state handle in ZooKeeper and discards the old state handle.
	 *
	 * <p><strong>Important</strong>: This method will only discard the state handle and not the
	 * state itself. Don't forget to run custom cleanup code of the state, if necessary.
	 *
	 * <pre>
	 * T state = get(path).getState();
	 * state.discard(); // Custom clean up
	 * replace(path, version, newState)
	 * </pre>
	 *
	 * @param pathInZooKeeper Destination path in ZooKeeper (expected to exist and start with a '/')
	 * @param expectedVersion Expected version of the node to replace
	 * @param state           The new state to replace the old one
	 */
	public void replace(String pathInZooKeeper, int expectedVersion, T state) throws Exception {
		checkNotNull(pathInZooKeeper, "Path in ZooKeeper");
		checkNotNull(state, "State");

		StateHandle<T> oldStateHandle = get(pathInZooKeeper);

		StateHandle<T> stateHandle = stateHandleProvider.createStateHandle(state);

		boolean success = false;

		try {
			// Serialize the new state handle. This writes the state to the backend.
			byte[] serializedStateHandle = InstantiationUtil.serializeObject(stateHandle);

			// Replace state handle in ZooKeeper.
			client.setData()
					.withVersion(expectedVersion)
					.forPath(pathInZooKeeper, serializedStateHandle);

			success = true;
		}
		finally {
			if (success) {
				oldStateHandle.discardState();
			}
			else {
				stateHandle.discardState();
			}
		}
	}

	/**
	 * Returns the version of the node if it exists or <code>-1</code> if it doesn't.
	 *
	 * @param pathInZooKeeper Path in ZooKeeper to check
	 * @return Version of the ZNode if the path exists, <code>-1</code> otherwise.
	 * @throws Exception
	 */
	public int exists(String pathInZooKeeper) throws Exception {
		checkNotNull(pathInZooKeeper, "Path in ZooKeeper");

		Stat stat = client.checkExists().forPath(pathInZooKeeper);

		if (stat != null) {
			return stat.getVersion();
		}

		return -1;
	}

	/**
	 * Gets a state handle from ZooKeeper.
	 *
	 * @param pathInZooKeeper Path in ZooKeeper to get the state handle from (expected to
	 *                        exist and start with a '/').
	 * @return The state handle
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public StateHandle<T> get(String pathInZooKeeper) throws Exception {
		checkNotNull(pathInZooKeeper, "Path in ZooKeeper");

		byte[] data = client.getData().forPath(pathInZooKeeper);

		return (StateHandle<T>) InstantiationUtil.deserializeObject(data, ClassLoader
				.getSystemClassLoader());
	}

	/**
	 * Gets all available state handles from ZooKeeper.
	 *
	 * @return All state handles from ZooKeeper.
	 * @throws ConcurrentModificationException
	 */
	@SuppressWarnings("unchecked")
	public List<ZooKeeperStateHandle<T>> getAll() throws Exception {
		// Initial cVersion (number of changes to the children of this node)
		int initialCVersion = client.checkExists().forPath("/").getCversion();

		List<String> children = client.getChildren().forPath("/");

		final List<ZooKeeperStateHandle<T>> stateHandles = new ArrayList<>(children.size());

		for (String path : children) {
			path = "/" + path;

			try {
				final StateHandle<T> stateHandle = get(path);
				stateHandles.add(new ZooKeeperStateHandle(stateHandle, path));
			}
			catch (KeeperException.NoNodeException e) {
				throw new ConcurrentModificationException("Modification during getAll()", e);
			}
		}

		verifyExpectedCVersion(initialCVersion, "/", "Modification during getAll()");

		return stateHandles;
	}

	/**
	 * Gets all available state handles from ZooKeeper sorted by name (ascending).
	 *
	 * @return All state handles in ZooKeeper.
	 */
	@SuppressWarnings("unchecked")
	public List<ZooKeeperStateHandle<T>> getAllSortedByName() throws Exception {
		// Initial cVersion (number of changes to the children of this node)
		int initialCVersion = client.checkExists().forPath("/").getCversion();

		List<String> children = ZKPaths.getSortedChildren(
				client.getZookeeperClient().getZooKeeper(),
				ZKPaths.fixForNamespace(client.getNamespace(), "/"));

		final List<ZooKeeperStateHandle<T>> stateHandles = new ArrayList<>(children.size());

		for (String path : children) {
			path = "/" + path;
			try {
				final StateHandle<T> stateHandle = get(path);
				stateHandles.add(new ZooKeeperStateHandle(stateHandle, path));
			}
			catch (KeeperException.NoNodeException e) {
				throw new ConcurrentModificationException("Modification during getAllSortedByName()", e);
			}
		}

		verifyExpectedCVersion(initialCVersion, "/", "Modification during getAllSortedByName()");

		return stateHandles;
	}

	/**
	 * Removes a state handle from ZooKeeper.
	 *
	 * <p><stong>Important</stong>: this does not discard the state handle. If you want to
	 * discard the state handle call {@link #discard(String)}.
	 *
	 * @param pathInZooKeeper Path of state handle to remove (expected to start with a '/')
	 * @throws Exception
	 */
	public void remove(String pathInZooKeeper) throws Exception {
		checkNotNull(pathInZooKeeper, "Path in ZooKeeper");

		client
				.delete()
				.deletingChildrenIfNeeded()
				.forPath(pathInZooKeeper);
	}

	/**
	 * Removes a state handle from ZooKeeper asynchronously.
	 *
	 * <p><stong>Important</stong>: this does not discard the state handle. If you want to
	 * discard the state handle call {@link #discard(String)}.
	 *
	 * @param pathInZooKeeper Path of state handle to remove (expected to start with a '/')
	 * @param callback        The callback after the operation finishes
	 * @throws Exception
	 */
	public void remove(String pathInZooKeeper, BackgroundCallback callback) throws Exception {
		checkNotNull(pathInZooKeeper, "Path in ZooKeeper");
		checkNotNull(callback, "Background callback");

		client
				.delete()
				.deletingChildrenIfNeeded()
				.inBackground(callback)
				.forPath(pathInZooKeeper);
	}

	/**
	 * Discards a state handle and removes it from ZooKeeper.
	 *
	 * <p><strong>Important</strong>: This method will only discard the state handle and not the
	 * state itself. Don't forget to run custom cleanup code of the state, if necessary.
	 *
	 * <pre>
	 * T state = get(path).getState();
	 * state.discard(); // Custom clean up
	 * discard(path)
	 * </pre>
	 *
	 * <p>If you only want to remove the state handle in ZooKeeper call {@link #remove(String)}.
	 *
	 * @param pathInZooKeeper Path of state handle to discard (expected to start with a '/')
	 * @throws Exception
	 */
	public void discard(String pathInZooKeeper) throws Exception {
		checkNotNull(pathInZooKeeper, "Path in ZooKeeper");

		StateHandle<T> stateHandle = get(pathInZooKeeper);

		// Delete the state handle from ZooKeeper first
		client
				.delete()
				.deletingChildrenIfNeeded()
				.forPath(pathInZooKeeper);

		// Discard the state handle only after it has been successfully deleted from ZooKeeper.
		// Otherwise we might enter an illegal state after failures (with a state handle in
		// ZooKeeper, which has already been discarded).
		stateHandle.discardState();
	}

	/**
	 * Discards all available state handles and removes them from ZooKeeper.
	 *
	 * <p><strong>Important</strong>: This method will only discard the state handles and not the
	 * states itself. Don't forget to run custom cleanup code of the states, if necessary.
	 *
	 * <pre>
	 * for (StateHandle<T> handle : getAll()) {
	 *     handle.getState().discard(); // Custom clean up
	 * }
	 * discardAll()
	 * </pre>
	 *
	 * @throws Exception
	 */
	public void discardAll() throws Exception {
		final List<ZooKeeperStateHandle<T>> allStateHandles = getAll();

		ZKPaths.deleteChildren(
				client.getZookeeperClient().getZooKeeper(),
				ZKPaths.fixForNamespace(client.getNamespace(), "/"),
				false);

		// Discard the state handles only after they have been successfully deleted from ZooKeeper.
		for (ZooKeeperStateHandle<T> stateHandle : allStateHandles) {
			stateHandle.getStateHandle().discardState();
		}
	}

	/**
	 * Throws a {@link ConcurrentModificationException} if the cVersion of the specified path
	 * does not match the expected cVersion.
	 *
	 * @param expectedCVersion The expected cVersion
	 * @param pathInZooKeeper  Path in ZooKeeper to check (expected to exist and start with '/')
	 * @param errorMessage     The error message of the thrown Exception
	 * @throws Exception
	 */
	private void verifyExpectedCVersion(
			int expectedCVersion,
			String pathInZooKeeper,
			String errorMessage) throws Exception {

		int cVersion = client.checkExists().forPath(pathInZooKeeper).getCversion();

		if (expectedCVersion != cVersion) {
			throw new ConcurrentModificationException(errorMessage);
		}
	}

}
