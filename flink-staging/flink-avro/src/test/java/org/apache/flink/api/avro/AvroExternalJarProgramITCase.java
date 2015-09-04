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

package org.apache.flink.api.avro;

import java.io.File;
import java.net.InetAddress;

import org.apache.flink.api.common.Plan;
import org.apache.flink.client.CliFrontend;
import org.apache.flink.client.RemoteExecutor;
import org.apache.flink.client.program.Client;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;

import org.junit.Assert;
import org.junit.Test;


public class AvroExternalJarProgramITCase {

	private static final String JAR_FILE = "target/maven-test-jar.jar";

	private static final String TEST_DATA_FILE = "/testdata.avro";

	@Test
	public void testExternalProgram() {

		ForkableFlinkMiniCluster testMiniCluster = null;

		try {
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 4);
			testMiniCluster = new ForkableFlinkMiniCluster(config, false);
			testMiniCluster.start();

			String jarFile = JAR_FILE;
			String testData = getClass().getResource(TEST_DATA_FILE).toString();

			PackagedProgram program = new PackagedProgram(new File(jarFile), new String[] { testData });


			config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
			config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, testMiniCluster.getLeaderRPCPort());

			Client client = new Client(config);

			client.setPrintStatusDuringExecution(false);
			client.runBlocking(program, 4);

		}
		catch (Throwable t) {
			System.err.println(t.getMessage());
			t.printStackTrace();
			Assert.fail("Error during the packaged program execution: " + t.getMessage());
		}
		finally {
			if (testMiniCluster != null) {
				try {
					testMiniCluster.stop();
				} catch (Throwable t) {
					// ignore
				}
			}
		}
	}
}
