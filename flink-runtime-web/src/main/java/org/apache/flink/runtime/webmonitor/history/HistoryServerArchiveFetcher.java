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

package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.HistoryServerOptions;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.history.FsJobArchivist;
import org.apache.flink.runtime.rest.handler.legacy.CurrentJobsOverviewHandler;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.FileUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is used by the {@link HistoryServer} to fetch the job archives that are located at
 * {@link HistoryServerOptions#HISTORY_SERVER_ARCHIVE_DIRS}. The directories are polled in regular intervals, defined
 * by {@link HistoryServerOptions#HISTORY_SERVER_ARCHIVE_REFRESH_INTERVAL}.
 *
 * <p>The archives are downloaded and expanded into a file structure analog to the REST API defined in the WebRuntimeMonitor.
 */
class HistoryServerArchiveFetcher {

	private static final Logger LOG = LoggerFactory.getLogger(HistoryServerArchiveFetcher.class);

	private static final JsonFactory jacksonFactory = new JsonFactory();
	private static final ObjectMapper mapper = new ObjectMapper();

	private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
		new ExecutorThreadFactory("Flink-HistoryServer-ArchiveFetcher"));
	private final JobArchiveFetcherTask fetcherTask;
	private final long refreshIntervalMillis;

	HistoryServerArchiveFetcher(long refreshIntervalMillis, List<HistoryServer.RefreshLocation> refreshDirs, File webDir, CountDownLatch numFinishedPolls) {
		this.refreshIntervalMillis = refreshIntervalMillis;
		this.fetcherTask = new JobArchiveFetcherTask(refreshDirs, webDir, numFinishedPolls);
		if (LOG.isInfoEnabled()) {
			for (HistoryServer.RefreshLocation refreshDir : refreshDirs) {
				LOG.info("Monitoring directory {} for archived jobs.", refreshDir.getPath());
			}
		}
	}

	void start() {
		executor.scheduleWithFixedDelay(fetcherTask, 0, refreshIntervalMillis, TimeUnit.MILLISECONDS);
	}

	void stop() {
		executor.shutdown();

		try {
			if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
				executor.shutdownNow();
			}
		} catch (InterruptedException ignored) {
			executor.shutdownNow();
		}
	}

	/**
	 * {@link TimerTask} that polls the directories configured as {@link HistoryServerOptions#HISTORY_SERVER_ARCHIVE_DIRS} for
	 * new job archives.
	 */
	static class JobArchiveFetcherTask extends TimerTask {

		private final List<HistoryServer.RefreshLocation> refreshDirs;
		private final CountDownLatch numFinishedPolls;

		/** Cache of all available jobs identified by their id. */
		private final Set<String> cachedArchives;

		private final File webDir;
		private final File webJobDir;
		private final File webOverviewDir;

		private static final String JSON_FILE_ENDING = ".json";

		JobArchiveFetcherTask(List<HistoryServer.RefreshLocation> refreshDirs, File webDir, CountDownLatch numFinishedPolls) {
			this.refreshDirs = checkNotNull(refreshDirs);
			this.numFinishedPolls = numFinishedPolls;
			this.cachedArchives = new HashSet<>();
			this.webDir = checkNotNull(webDir);
			this.webJobDir = new File(webDir, "jobs");
			webJobDir.mkdir();
			this.webOverviewDir = new File(webDir, "overviews");
			webOverviewDir.mkdir();
		}

		@Override
		public void run() {
			try {
				for (HistoryServer.RefreshLocation refreshLocation : refreshDirs) {
					Path refreshDir = refreshLocation.getPath();
					FileSystem refreshFS = refreshLocation.getFs();

					// contents of /:refreshDir
					FileStatus[] jobArchives;
					try {
						jobArchives = refreshFS.listStatus(refreshDir);
					} catch (IOException e) {
						LOG.error("Failed to access job archive location for path {}.", refreshDir, e);
						continue;
					}
					if (jobArchives == null) {
						continue;
					}
					boolean updateOverview = false;
					for (FileStatus jobArchive : jobArchives) {
						Path jobArchivePath = jobArchive.getPath();
						String jobID = jobArchivePath.getName();
						try {
							JobID.fromHexString(jobID);
						} catch (IllegalArgumentException iae) {
							LOG.debug("Archive directory {} contained file with unexpected name {}. Ignoring file.",
								refreshDir, jobID, iae);
							continue;
						}
						if (cachedArchives.add(jobID)) {
							try {
								for (ArchivedJson archive : FsJobArchivist.getArchivedJsons(jobArchive.getPath())) {
									String path = archive.getPath();
									String json = archive.getJson();

									File target;
									if (path.equals("/joboverview")) {
										target = new File(webOverviewDir, jobID + JSON_FILE_ENDING);
									} else {
										target = new File(webDir, path + JSON_FILE_ENDING);
									}

									java.nio.file.Path parent = target.getParentFile().toPath();

									try {
										Files.createDirectories(parent);
									} catch (FileAlreadyExistsException ignored) {
										// there may be left-over directories from the previous attempt
									}

									java.nio.file.Path targetPath = target.toPath();

									// We overwrite existing files since this may be another attempt at fetching this archive.
									// Existing files may be incomplete/corrupt.
									if (Files.exists(targetPath)) {
										Files.delete(targetPath);
									}

									Files.createFile(target.toPath());
									try (FileWriter fw = new FileWriter(target)) {
										fw.write(json);
										fw.flush();
									}
								}
								updateOverview = true;
							} catch (IOException e) {
								LOG.error("Failure while fetching/processing job archive for job {}.", jobID, e);
								// Make sure we attempt to fetch the archive again
								cachedArchives.remove(jobID);
								// Make sure we do not include this job in the overview
								try {
									Files.delete(new File(webOverviewDir, jobID + JSON_FILE_ENDING).toPath());
								} catch (IOException ioe) {
									LOG.debug("Could not delete file from overview directory.", ioe);
								}

								// Clean up job files we may have created
								File jobDirectory = new File(webJobDir, jobID);
								try {
									FileUtils.deleteDirectory(jobDirectory);
								} catch (IOException ioe) {
									LOG.debug("Could not clean up job directory.", ioe);
								}
							}
						}
					}
					if (updateOverview) {
						updateJobOverview(webDir);
					}
				}
			} catch (Exception e) {
				LOG.error("Critical failure while fetching/processing job archives.", e);
			}
			numFinishedPolls.countDown();
		}
	}

	/**
	 * This method replicates the JSON response that would be given by the {@link CurrentJobsOverviewHandler} when
	 * listing both running and finished jobs.
	 *
	 * <p>Every job archive contains a joboverview.json file containing the same structure. Since jobs are archived on
	 * their own however the list of finished jobs only contains a single job.
	 *
	 * <p>For the display in the HistoryServer WebFrontend we have to combine these overviews.
	 */
	private static void updateJobOverview(File webDir) {
		File webOverviewDir = new File(webDir, "overviews");
		try (JsonGenerator gen = jacksonFactory.createGenerator(HistoryServer.createOrGetFile(webDir, "joboverview"))) {
			gen.writeStartObject();
			gen.writeArrayFieldStart("running");
			gen.writeEndArray();
			gen.writeArrayFieldStart("finished");

			File[] overviews = new File(webOverviewDir.getPath()).listFiles();
			if (overviews != null) {
				for (File overview : overviews) {
					JsonNode root = mapper.readTree(overview);
					JsonNode finished = root.get("finished");
					JsonNode job = finished.get(0);
					mapper.writeTree(gen, job);
				}
			}

			gen.writeEndArray();
			gen.writeEndObject();
		} catch (IOException ioe) {
			LOG.error("Failed to update job overview.", ioe);
		}
	}
}
