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

package org.apache.flink.runtime.blob;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.flink.runtime.blob.BlobServerProtocol.BUFFER_SIZE;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class implements the BLOB server. The BLOB server is responsible for listening for incoming requests and
 * spawning threads to handle these requests. Furthermore, it takes care of creating the directory structure to store
 * the BLOBs or temporarily cache them.
 */
public class BlobServer extends Thread implements BlobService, PermanentBlobService, TransientBlobService {

	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(BlobServer.class);

	/** Counter to generate unique names for temporary files. */
	private final AtomicLong tempFileCounter = new AtomicLong(0);

	/** The server socket listening for incoming connections. */
	private final ServerSocket serverSocket;

	/** The SSL server context if ssl is enabled for the connections */
	private SSLContext serverSSLContext = null;

	/** Blob Server configuration */
	private final Configuration blobServiceConfiguration;

	/** Indicates whether a shutdown of server component has been requested. */
	private final AtomicBoolean shutdownRequested = new AtomicBoolean();

	/** Root directory for local file storage */
	private final File storageDir;

	/** Blob store for distributed file storage, e.g. in HA */
	private final BlobStore blobStore;

	/** Set of currently running threads */
	private final Set<BlobServerConnection> activeConnections = new HashSet<>();

	/** The maximum number of concurrent connections */
	private final int maxConnections;

	/** Lock guarding concurrent file accesses */
	private final ReadWriteLock readWriteLock;

	/**
	 * Shutdown hook thread to ensure deletion of the storage directory (or <code>null</code> if
	 * the configured high availability mode does not equal{@link HighAvailabilityMode#NONE})
	 */
	private final Thread shutdownHook;

	/**
	 * Instantiates a new BLOB server and binds it to a free network port.
	 *
	 * @param config Configuration to be used to instantiate the BlobServer
	 * @param blobStore BlobStore to store blobs persistently
	 *
	 * @throws IOException
	 * 		thrown if the BLOB server cannot bind to a free network port or if the
	 * 		(local or distributed) file storage cannot be created or is not usable
	 */
	public BlobServer(Configuration config, BlobStore blobStore) throws IOException {
		this.blobServiceConfiguration = checkNotNull(config);
		this.blobStore = checkNotNull(blobStore);
		this.readWriteLock = new ReentrantReadWriteLock();

		// configure and create the storage directory
		String storageDirectory = config.getString(BlobServerOptions.STORAGE_DIRECTORY);
		this.storageDir = BlobUtils.initLocalStorageDirectory(storageDirectory);
		LOG.info("Created BLOB server storage directory {}", storageDir);

		// configure the maximum number of concurrent connections
		final int maxConnections = config.getInteger(BlobServerOptions.FETCH_CONCURRENT);
		if (maxConnections >= 1) {
			this.maxConnections = maxConnections;
		}
		else {
			LOG.warn("Invalid value for maximum connections in BLOB server: {}. Using default value of {}",
					maxConnections, BlobServerOptions.FETCH_CONCURRENT.defaultValue());
			this.maxConnections = BlobServerOptions.FETCH_CONCURRENT.defaultValue();
		}

		// configure the backlog of connections
		int backlog = config.getInteger(BlobServerOptions.FETCH_BACKLOG);
		if (backlog < 1) {
			LOG.warn("Invalid value for BLOB connection backlog: {}. Using default value of {}",
					backlog, BlobServerOptions.FETCH_BACKLOG.defaultValue());
			backlog = BlobServerOptions.FETCH_BACKLOG.defaultValue();
		}

		this.shutdownHook = BlobUtils.addShutdownHook(this, LOG);

		if (config.getBoolean(BlobServerOptions.SSL_ENABLED)) {
			try {
				serverSSLContext = SSLUtils.createSSLServerContext(config);
			} catch (Exception e) {
				throw new IOException("Failed to initialize SSLContext for the blob server", e);
			}
		}

		//  ----------------------- start the server -------------------

		String serverPortRange = config.getString(BlobServerOptions.PORT);

		Iterator<Integer> ports = NetUtils.getPortRangeFromString(serverPortRange);

		final int finalBacklog = backlog;
		ServerSocket socketAttempt = NetUtils.createSocketFromPorts(ports, new NetUtils.SocketFactory() {
			@Override
			public ServerSocket createSocket(int port) throws IOException {
				if (serverSSLContext == null) {
					return new ServerSocket(port, finalBacklog);
				} else {
					LOG.info("Enabling ssl for the blob server");
					return serverSSLContext.getServerSocketFactory().createServerSocket(port, finalBacklog);
				}
			}
		});

		if(socketAttempt == null) {
			throw new IOException("Unable to allocate socket for blob server in specified port range: "+serverPortRange);
		} else {
			SSLUtils.setSSLVerAndCipherSuites(socketAttempt, config);
			this.serverSocket = socketAttempt;
		}

		// start the server thread
		setName("BLOB Server listener at " + getPort());
		setDaemon(true);
		start();

		if (LOG.isInfoEnabled()) {
			LOG.info("Started BLOB server at {}:{} - max concurrent requests: {} - max backlog: {}",
					serverSocket.getInetAddress().getHostAddress(), getPort(), maxConnections, backlog);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Path Accessors
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns a file handle to the file associated with the given blob key on the blob
	 * server.
	 *
	 * <p><strong>This is only called from {@link BlobServerConnection} or unit tests.</strong>
	 *
	 * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
	 * @param key identifying the file
	 * @return file handle to the file
	 *
	 * @throws IOException
	 * 		if creating the directory fails
	 */
	@VisibleForTesting
	public File getStorageLocation(@Nullable JobID jobId, BlobKey key) throws IOException {
		return BlobUtils.getStorageLocation(storageDir, jobId, key);
	}

	/**
	 * Returns a temporary file inside the BLOB server's incoming directory.
	 *
	 * @return a temporary file inside the BLOB server's incoming directory
	 *
	 * @throws IOException
	 * 		if creating the directory fails
	 */
	File createTemporaryFilename() throws IOException {
		return new File(BlobUtils.getIncomingDirectory(storageDir),
				String.format("temp-%08d", tempFileCounter.getAndIncrement()));
	}

	/**
	 * Returns the blob store.
	 */
	BlobStore getBlobStore() {
		return blobStore;
	}

	/**
	 * Returns the lock used to guard file accesses
	 */
	ReadWriteLock getReadWriteLock() {
		return readWriteLock;
	}

	@Override
	public void run() {
		try {
			while (!this.shutdownRequested.get()) {
				BlobServerConnection conn = new BlobServerConnection(serverSocket.accept(), this);
				try {
					synchronized (activeConnections) {
						while (activeConnections.size() >= maxConnections) {
							activeConnections.wait(2000);
						}
						activeConnections.add(conn);
					}

					conn.start();
					conn = null;
				}
				finally {
					if (conn != null) {
						conn.close();
						synchronized (activeConnections) {
							activeConnections.remove(conn);
						}
					}
				}
			}
		}
		catch (Throwable t) {
			if (!this.shutdownRequested.get()) {
				LOG.error("BLOB server stopped working. Shutting down", t);

				try {
					close();
				} catch (Throwable closeThrowable) {
					LOG.error("Could not properly close the BlobServer.", closeThrowable);
				}
			}
		}
	}

	/**
	 * Shuts down the BLOB server.
	 */
	@Override
	public void close() throws IOException {
		if (shutdownRequested.compareAndSet(false, true)) {
			Exception exception = null;

			try {
				this.serverSocket.close();
			}
			catch (IOException ioe) {
				exception = ioe;
			}

			// wake the thread up, in case it is waiting on some operation
			interrupt();

			try {
				join();
			}
			catch (InterruptedException ie) {
				Thread.currentThread().interrupt();

				LOG.debug("Error while waiting for this thread to die.", ie);
			}

			synchronized (activeConnections) {
				if (!activeConnections.isEmpty()) {
					for (BlobServerConnection conn : activeConnections) {
						LOG.debug("Shutting down connection {}.", conn.getName());
						conn.close();
					}
					activeConnections.clear();
				}
			}

			// Clean up the storage directory
			try {
				FileUtils.deleteDirectory(storageDir);
			}
			catch (IOException e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			// Remove shutdown hook to prevent resource leaks, unless this is invoked by the
			// shutdown hook itself
			if (shutdownHook != null && shutdownHook != Thread.currentThread()) {
				try {
					Runtime.getRuntime().removeShutdownHook(shutdownHook);
				}
				catch (IllegalStateException e) {
					// race, JVM is in shutdown already, we can safely ignore this
				}
				catch (Throwable t) {
					LOG.warn("Exception while unregistering BLOB server's cleanup shutdown hook.", t);
				}
			}

			if(LOG.isInfoEnabled()) {
				LOG.info("Stopped BLOB server at {}:{}", serverSocket.getInetAddress().getHostAddress(), getPort());
			}

			ExceptionUtils.tryRethrowIOException(exception);
		}
	}

	protected BlobClient createClient() throws IOException {
		return new BlobClient(new InetSocketAddress(serverSocket.getInetAddress(), getPort()),
			blobServiceConfiguration);
	}

	/**
	 * Retrieves the local path of a (job-unrelated) file associated with a job and a blob key.
	 * <p>
	 * The blob server looks the blob key up in its local storage. If the file exists, it is
	 * returned. If the file does not exist, it is retrieved from the HA blob store (if available)
	 * or a {@link FileNotFoundException} is thrown.
	 *
	 * @param key
	 * 		blob key associated with the requested file
	 *
	 * @return file referring to the local storage location of the BLOB
	 *
	 * @throws IOException
	 * 		Thrown if the file retrieval failed.
	 */
	@Override
	public File getFile(BlobKey key) throws IOException {
		return getFileInternal(null, key, false);
	}

	/**
	 * Retrieves the local path of a file associated with a job and a blob key.
	 * <p>
	 * The blob server looks the blob key up in its local storage. If the file exists, it is
	 * returned. If the file does not exist, it is retrieved from the HA blob store (if available)
	 * or a {@link FileNotFoundException} is thrown.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to
	 * @param key
	 * 		blob key associated with the requested file
	 *
	 * @return file referring to the local storage location of the BLOB
	 *
	 * @throws IOException
	 * 		Thrown if the file retrieval failed.
	 */
	@Override
	public File getFile(JobID jobId, BlobKey key) throws IOException {
		checkNotNull(jobId);
		return getFileInternal(jobId, key, false);
	}

	/**
	 * Returns the path to a local copy of the file associated with the provided job ID and blob
	 * key.
	 * <p>
	 * We will first attempt to serve the BLOB from the local storage. If the BLOB is not in
	 * there, we will try to download it from the HA store.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to
	 * @param key
	 * 		blob key associated with the requested file
	 *
	 * @return The path to the file.
	 *
	 * @throws java.io.FileNotFoundException
	 * 		if the BLOB does not exist;
	 * @throws IOException
	 * 		if any other error occurs when retrieving the file
	 */
	@Override
	public File getHAFile(JobID jobId, BlobKey key) throws IOException {
		checkNotNull(jobId);
		return getFileInternal(jobId, key, true);
	}

	/**
	 * Retrieves the local path of a file associated with a job and a blob key.
	 * <p>
	 * The blob server looks the blob key up in its local storage. If the file exists, it is
	 * returned. If the file does not exist, it is retrieved from the HA blob store (if available)
	 * or a {@link FileNotFoundException} is thrown.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
	 * @param blobKey
	 * 		blob key associated with the requested file
	 * @param highlyAvailable
	 * 		whether to the requested file is highly available (HA)
	 *
	 * @return file referring to the local storage location of the BLOB
	 *
	 * @throws IOException
	 * 		Thrown if the file retrieval failed.
	 */
	private File getFileInternal(@Nullable JobID jobId, BlobKey blobKey, boolean highlyAvailable) throws IOException {
		checkArgument(blobKey != null, "BLOB key cannot be null.");

		final File localFile = BlobUtils.getStorageLocation(storageDir, jobId, blobKey);
		readWriteLock.readLock().lock();

		try {

			if (localFile.exists()) {
				return localFile;
			} else if (highlyAvailable) {
				// Try the HA blob store
				// first we have to release the read lock in order to acquire the write lock
				readWriteLock.readLock().unlock();
				readWriteLock.writeLock().lock();

				try {
					if (localFile.exists()) {
						LOG.debug("Blob file {} has been downloaded from the (distributed) blob store by a different thread.", localFile);
					} else {
						blobStore.get(jobId, blobKey, localFile);
					}
				} finally {
					readWriteLock.writeLock().unlock();
				}

				readWriteLock.readLock().lock();

				if (localFile.exists()) {
					return localFile;
				}
			}

			throw new FileNotFoundException("Local file " + localFile + " does not exist " +
				"and failed to copy from blob store.");
		} finally {
			readWriteLock.readLock().unlock();
		}
	}

	@Override
	public BlobKey put(byte[] value) throws IOException {
		return putBuffer(null, value, false);
	}

	@Override
	public BlobKey put(JobID jobId, byte[] value) throws IOException {
		checkNotNull(jobId);
		return putBuffer(jobId, value, false);
	}

	@Override
	public BlobKey put(InputStream inputStream) throws IOException {
		return putInputStream(null, inputStream, false);
	}

	@Override
	public BlobKey put(JobID jobId, InputStream inputStream) throws IOException {
		checkNotNull(jobId);
		return putInputStream(jobId, inputStream, false);
	}

	/**
	 * Uploads the data of the given byte array for the given job to the BLOB server and makes it
	 * highly available (HA).
	 *
	 * @param jobId
	 * 		the ID of the job the BLOB belongs to
	 * @param value
	 * 		the buffer to upload
	 *
	 * @return the computed BLOB key identifying the BLOB on the server
	 *
	 * @throws IOException
	 * 		thrown if an I/O error occurs while writing it to a local file, or uploading it to the HA
	 * 		store
	 */
	public BlobKey putHA(JobID jobId, byte[] value) throws IOException {
		checkNotNull(jobId);
		return putBuffer(jobId, value, true);
	}

	/**
	 * Uploads the data from the given input stream for the given job to the BLOB server and makes it
	 * highly available (HA).
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to
	 * @param inputStream
	 * 		the input stream to read the data from
	 *
	 * @return the computed BLOB key identifying the BLOB on the server
	 *
	 * @throws IOException
	 * 		thrown if an I/O error occurs while reading the data from the input stream, writing it to a
	 * 		local file, or uploading it to the HA store
	 */
	public BlobKey putHA(JobID jobId, InputStream inputStream) throws IOException {
		checkNotNull(jobId);
		return putInputStream(null, inputStream, true);
	}

	/**
	 * Uploads the data of the given byte array for the given job to the BLOB server.
	 *
	 * @param jobId
	 * 		the ID of the job the BLOB belongs to
	 * @param value
	 * 		the buffer to upload
	 * @param highlyAvailable
	 * 		whether to make the data highly available (HA)
	 *
	 * @return the computed BLOB key identifying the BLOB on the server
	 *
	 * @throws IOException
	 * 		thrown if an I/O error occurs while writing it to a local file, or uploading it to the HA
	 * 		store
	 */
	BlobKey putBuffer(@Nullable JobID jobId, byte[] value, boolean highlyAvailable)
			throws IOException {

		if (LOG.isDebugEnabled()) {
			LOG.debug("Received PUT call for BLOB of job {}.", jobId);
		}

		File incomingFile = createTemporaryFilename();

		// read stream
		MessageDigest md = BlobUtils.createMessageDigest();
		FileOutputStream fos = new FileOutputStream(incomingFile);

		final BlobKey blobKey;
		try {
			md.update(value);
			fos.write(value);

			blobKey = new BlobKey(md.digest());
		} finally {
			try {
				fos.close();
			} catch (Throwable t) {
				LOG.warn("Cannot close stream to BLOB staging file", t);
			}
		}

		// persist file
		moveTempFileToStore(incomingFile, jobId, blobKey, highlyAvailable);

		return blobKey;
	}


	/**
	 * Uploads the data from the given input stream for the given job to the BLOB server.
	 *
	 * @param jobId
	 * 		the ID of the job the BLOB belongs to
	 * @param inputStream
	 * 		the input stream to read the data from
	 * @param highlyAvailable
	 * 		whether to make the data highly available (HA)
	 *
	 * @return the computed BLOB key identifying the BLOB on the server
	 *
	 * @throws IOException
	 * 		thrown if an I/O error occurs while reading the data from the input stream, writing it to a
	 * 		local file, or uploading it to the HA store
	 */
	BlobKey putInputStream(@Nullable JobID jobId, InputStream inputStream, boolean highlyAvailable)
			throws IOException {

		if (LOG.isDebugEnabled()) {
			LOG.debug("Received PUT call for BLOB of job {}.", jobId);
		}

		File incomingFile = createTemporaryFilename();

		// read stream
		MessageDigest md = BlobUtils.createMessageDigest();
		FileOutputStream fos = new FileOutputStream(incomingFile);
		byte[] buf = new byte[BUFFER_SIZE];

		final BlobKey blobKey;
		try {
			while (true) {
				final int bytesRead = inputStream.read(buf);
				if (bytesRead == -1) {
					// done
					break;
				}
				fos.write(buf, 0, bytesRead);
				md.update(buf, 0, bytesRead);
			}
			blobKey = new BlobKey(md.digest());
		} finally {
			try {
				fos.close();
			} catch (Throwable t) {
				LOG.warn("Cannot close stream to BLOB staging file", t);
			}
		}

		// persist file
		moveTempFileToStore(incomingFile, jobId, blobKey, highlyAvailable);

		return blobKey;
	}

	/**
	 * Moves the temporary <tt>incomingFile</tt> to its permanent location where it is available for
	 * use.
	 *
	 * @param incomingFile
	 * 		temporary file created during transfer
	 * @param jobId
	 * 		ID of the job this blob belongs to or <tt>null</tt> if job-unrelated
	 * @param blobKey
	 * 		BLOB key identifying the file
	 * @param highlyAvailable
	 * 		whether this file should be stored in the HA store
	 *
	 * @throws IOException
	 * 		thrown if an I/O error occurs while moving the file or uploading it to the HA store
	 */
	private void moveTempFileToStore(
			File incomingFile, @Nullable JobID jobId, BlobKey blobKey, boolean highlyAvailable)
			throws IOException {

		File storageFile = BlobUtils.getStorageLocation(storageDir, jobId, blobKey);
		readWriteLock.writeLock().lock();

		try {
			// first check whether the file already exists
			if (!storageFile.exists()) {
				try {
					// only move the file if it does not yet exist
					Files.move(incomingFile.toPath(), storageFile.toPath());

					incomingFile = null;

				} catch (FileAlreadyExistsException ignored) {
					LOG.warn("Detected concurrent file modifications. This should only happen if multiple" +
						"BlobServer use the same storage directory.");
					// we cannot be sure at this point whether the file has already been uploaded to the blob
					// store or not. Even if the blobStore might shortly be in an inconsistent state, we have
					// persist the blob. Otherwise we might not be able to recover the job.
				}

				if (highlyAvailable) {
					// only the one moving the incoming file to its final destination is allowed to upload the
					// file to the blob store
					blobStore.put(storageFile, jobId, blobKey);
				}
			} else {
				LOG.warn("File upload for an existing file with key {} for job {}. This may indicate a duplicate upload or a hash collision. Ignoring newest upload.", blobKey, jobId);
			}
			storageFile = null;
		} finally {
			// we failed to either create the local storage file or to upload it --> try to delete the local file
			// while still having the write lock
			if (storageFile != null && !storageFile.delete() && storageFile.exists()) {
				LOG.warn("Could not delete the storage file {}.", storageFile);
			}
			if (incomingFile != null && !incomingFile.delete() && incomingFile.exists()) {
				LOG.warn("Could not delete the staging file {} for blob key {} and job {}.", incomingFile, blobKey, jobId);
			}

			readWriteLock.writeLock().unlock();
		}
	}

	/**
	 * Deletes the (job-unrelated) file associated with the blob key in the local storage of the
	 * blob server.
	 *
	 * @param key
	 * 		blob key associated with the file to be deleted
	 *
	 * @throws IOException
	 */
	@Override
	public void delete(BlobKey key) throws IOException {
		deleteInternal(null, key);
	}

	/**
	 * Deletes the file associated with the blob key in the local storage of the blob server.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to
	 * @param key
	 * 		blob key associated with the file to be deleted
	 *
	 * @throws IOException
	 */
	@Override
	public void delete(JobID jobId, BlobKey key) throws IOException {
		checkNotNull(jobId);
		deleteInternal(jobId, key);
	}

	/**
	 * Deletes the file associated with the blob key in the local storage of the blob server.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
	 * @param key
	 * 		blob key associated with the file to be deleted
	 *
	 * @throws IOException
	 */
	void deleteInternal(@Nullable JobID jobId, BlobKey key) throws IOException {
		final File localFile = BlobUtils.getStorageLocation(storageDir, jobId, key);

		readWriteLock.writeLock().lock();

		try {
			if (!localFile.delete() && localFile.exists()) {
				LOG.warn("Failed to locally delete BLOB " + key + " at " + localFile.getAbsolutePath());
			}
		} finally {
			readWriteLock.writeLock().unlock();
		}
	}

	/**
	 * Removes all BLOBs from local and HA store belonging to the given job ID.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to
	 */
	public void cleanupJob(JobID jobId) {
		checkNotNull(jobId);

		final File jobDir =
			new File(BlobUtils.getStorageLocationPath(storageDir.getAbsolutePath(), jobId));

		readWriteLock.writeLock().lock();

		try {
			// delete locally
			try {
				FileUtils.deleteDirectory(jobDir);
			} catch (IOException e) {
				LOG.warn("Failed to locally delete BLOB storage directory at " +
					jobDir.getAbsolutePath(), e);
			}

			// delete in HA store
			blobStore.deleteAll(jobId);
		} finally {
			readWriteLock.writeLock().unlock();
		}
	}


	@Override
	public PermanentBlobService getPermanentBlobStore() {
		return this;
	}

	@Override
	public TransientBlobService getTransientBlobStore() {
		return this;
	}

	/**
	 * Returns the port on which the server is listening.
	 *
	 * @return port on which the server is listening
	 */
	@Override
	public int getPort() {
		return this.serverSocket.getLocalPort();
	}

	/**
	 * Tests whether the BLOB server has been requested to shut down.
	 *
	 * @return True, if the server has been requested to shut down, false otherwise.
	 */
	public boolean isShutdown() {
		return this.shutdownRequested.get();
	}

	/**
	 * Access to the server socket, for testing
	 */
	ServerSocket getServerSocket() {
		return this.serverSocket;
	}

	void unregisterConnection(BlobServerConnection conn) {
		synchronized (activeConnections) {
			activeConnections.remove(conn);
			activeConnections.notifyAll();
		}
	}

	/**
	 * Returns all the current active connections in the BlobServer.
	 *
	 * @return the list of all the active in current BlobServer
	 */
	List<BlobServerConnection> getCurrentActiveConnections() {
		synchronized (activeConnections) {
			return new ArrayList<BlobServerConnection>(activeConnections);
		}
	}
}
