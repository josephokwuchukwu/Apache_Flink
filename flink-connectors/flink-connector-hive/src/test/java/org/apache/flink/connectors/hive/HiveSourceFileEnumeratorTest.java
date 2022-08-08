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

package org.apache.flink.connectors.hive;

import org.apache.flink.connectors.hive.read.HiveSourceSplit;

import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HiveSourceFileEnumerator} . */
public class HiveSourceFileEnumeratorTest {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testCreateInputSplits() throws Exception {
        int numSplits = 1000;
        // set configuration related to create input splits
        JobConf jobConf = new JobConf();
        jobConf.set(
                HiveOptions.TABLE_EXEC_HIVE_SPLIT_MAX_BYTES.key(),
                String.valueOf(
                        HiveOptions.TABLE_EXEC_HIVE_SPLIT_MAX_BYTES.defaultValue().getBytes()));
        jobConf.set(
                HiveOptions.TABLE_EXEC_HIVE_FILE_OPEN_COST.key(),
                String.valueOf(
                        HiveOptions.TABLE_EXEC_HIVE_SPLIT_MAX_BYTES.defaultValue().getBytes()));
        jobConf.set(HiveOptions.TABLE_EXEC_HIVE_LOAD_PARTITION_SPLITS_THREAD_NUM.key(), "1");
        // set to the strategy to 'etl` manually to make we can test configuration for splits
        // with these small files

        File wareHouse = temporaryFolder.newFolder("testCreateInputSplits");
        // init the files for the partition
        StorageDescriptor sd = new StorageDescriptor();
        // set parquet format
        SerDeInfo serdeInfo = new SerDeInfo();
        serdeInfo.setSerializationLib("orc");
        sd.setSerdeInfo(serdeInfo);
        sd.setInputFormat(OrcInputFormat.class.getName());
        sd.setLocation(wareHouse.toString());
        String baseFilePath =
                Objects.requireNonNull(this.getClass().getResource("/orc/test.orc")).getPath();
        Files.copy(Paths.get(baseFilePath), Paths.get(wareHouse.toString(), "t.orc"));
        // use default configuration
        List<HiveSourceSplit> hiveSourceSplits =
                HiveSourceFileEnumerator.createInputSplits(
                        numSplits,
                        Collections.singletonList(new HiveTablePartition(sd, new Properties())),
                        jobConf,
                        false);
        // the single file is a single split
        assertThat(hiveSourceSplits.size()).isEqualTo(1);

        // change split max size and verify it works
        jobConf.set(HiveOptions.TABLE_EXEC_HIVE_SPLIT_MAX_BYTES.key(), "10");
        // the splits should be more than the number of files
        hiveSourceSplits =
                HiveSourceFileEnumerator.createInputSplits(
                        numSplits,
                        Collections.singletonList(new HiveTablePartition(sd, new Properties())),
                        jobConf,
                        false);
        // the single file should be enumerated into two splits
        assertThat(hiveSourceSplits.size()).isEqualTo(2);

        // revert the change of split max size
        jobConf.set(
                HiveOptions.TABLE_EXEC_HIVE_SPLIT_MAX_BYTES.key(),
                String.valueOf(
                        HiveOptions.TABLE_EXEC_HIVE_SPLIT_MAX_BYTES.defaultValue().getBytes()));
        // change open cost and verify it works
        jobConf.set(HiveOptions.TABLE_EXEC_HIVE_FILE_OPEN_COST.key(), "1");
        hiveSourceSplits =
                HiveSourceFileEnumerator.createInputSplits(
                        numSplits,
                        Collections.singletonList(new HiveTablePartition(sd, new Properties())),
                        jobConf,
                        false);
        // the single file should be enumerated into two splits
        assertThat(hiveSourceSplits.size()).isEqualTo(2);
    }
}
