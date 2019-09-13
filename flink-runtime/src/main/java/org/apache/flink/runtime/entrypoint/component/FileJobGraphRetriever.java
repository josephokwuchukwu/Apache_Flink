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

package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.FlinkException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link JobGraphRetriever} implementation which retrieves the {@link JobGraph} from
 * a file on disk.
 */
public class FileJobGraphRetriever extends AbstractUserClassPathJobGraphRetriever {

	public static final ConfigOption<String> JOB_GRAPH_FILE_PATH = ConfigOptions
		.key("internal.jobgraph-path")
		.defaultValue("job.graph");

	@Nonnull
	private final String jobGraphFile;

	public FileJobGraphRetriever(@Nonnull String jobGraphFile, @Nullable File jobDir) throws IOException {
		super(jobDir);
		this.jobGraphFile = jobGraphFile;
	}

	@Override
	public JobGraph retrieveJobGraph(Configuration configuration) throws FlinkException {
		File fp = new File(jobGraphFile);

		try (FileInputStream input = new FileInputStream(fp);
			ObjectInputStream obInput = new ObjectInputStream(input)) {
			final JobGraph jobGraph = (JobGraph) obInput.readObject();

			if (!getUserClassPaths().isEmpty()) {
				if (jobGraph.getClasspaths() != null && !jobGraph.getClasspaths().isEmpty()) {
					final List<URL> list = new ArrayList<>();
					list.addAll(jobGraph.getClasspaths());
					list.addAll(getUserClassPaths());
					jobGraph.setClasspaths(list);
				} else {
					jobGraph.setClasspaths(getUserClassPaths());
				}
			}
			return jobGraph;
		} catch (FileNotFoundException e) {
			throw new FlinkException("Could not find the JobGraph file.", e);
		} catch (ClassNotFoundException | IOException e) {
			throw new FlinkException("Could not load the JobGraph from file.", e);
		}
	}

	public static FileJobGraphRetriever createFrom(Configuration configuration) throws IOException {
		return new FileJobGraphRetriever(configuration.getString(JOB_GRAPH_FILE_PATH), null);
	}

	public static FileJobGraphRetriever createFrom(Configuration configuration, File jobDir) throws IOException {
		return new FileJobGraphRetriever(configuration.getString(JOB_GRAPH_FILE_PATH), jobDir);
	}
}
