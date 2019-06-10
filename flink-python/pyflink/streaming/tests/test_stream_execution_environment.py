################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import os
import tempfile
import json

from pyflink.common import (ExecutionConfig, RestartStrategies, CheckpointConfig,
                            CheckpointingMode, TimeCharacteristic)
from pyflink.streaming import StreamExecutionEnvironment
from pyflink.table import DataTypes, CsvTableSource, CsvTableSink, StreamTableEnvironment
from pyflink.testing.test_case_utils import PyFlinkTestCase


class StreamExecutionEnvironmentTests(PyFlinkTestCase):

    def setUp(self):
        self.env = StreamExecutionEnvironment.get_execution_environment()

    def test_get_config(self):
        execution_config = self.env.get_config()

        assert isinstance(execution_config, ExecutionConfig)

    def test_get_set_parallelism(self):

        self.env.set_parallelism(10)

        parallelism = self.env.get_parallelism()

        assert parallelism == 10

    def test_get_set_buffer_timeout(self):

        self.env.set_buffer_timeout(12000)

        timeout = self.env.get_buffer_timeout()

        assert timeout == 12000

    def test_get_set_default_local_parallelism(self):

        self.env.set_default_local_parallelism(8)

        parallelism = self.env.get_default_local_parallelism()

        assert parallelism == 8

    def test_set_get_restart_strategy(self):

        self.env.set_restart_strategy(RestartStrategies.no_restart())

        restart_strategy = self.env.get_restart_strategy()

        assert restart_strategy == RestartStrategies.no_restart()

    def test_add_default_kryo_serializer(self):

        self.env.add_default_kryo_serializer(
            "org.apache.flink.runtime.state.StateBackendTestBase$TestPojo",
            "org.apache.flink.runtime.state.StateBackendTestBase$CustomKryoTestSerializer")

        dict = self.env.get_config().get_default_kryo_serializer_classes()

        assert dict == {'org.apache.flink.runtime.state.StateBackendTestBase$TestPojo':
                        'org.apache.flink.runtime.state'
                        '.StateBackendTestBase$CustomKryoTestSerializer'}

    def test_register_type_with_kryo_serializer(self):

        self.env.register_type_with_kryo_serializer(
            "org.apache.flink.runtime.state.StateBackendTestBase$TestPojo",
            "org.apache.flink.runtime.state.StateBackendTestBase$CustomKryoTestSerializer")

        dict = self.env.get_config().get_registered_types_with_kryo_serializer_classes()

        assert dict == {'org.apache.flink.runtime.state.StateBackendTestBase$TestPojo':
                        'org.apache.flink.runtime.state'
                        '.StateBackendTestBase$CustomKryoTestSerializer'}

    def test_register_type(self):

        self.env.register_type("org.apache.flink.runtime.state.StateBackendTestBase$TestPojo")

        type_list = self.env.get_config().get_registered_pojo_types()

        assert type_list == ['org.apache.flink.runtime.state.StateBackendTestBase$TestPojo']

    def test_get_set_max_parallelism(self):

        self.env.set_max_parallelism(12)

        parallelism = self.env.get_max_parallelism()

        assert parallelism == 12

    def test_operation_chaining(self):

        assert self.env.is_chaining_enabled() is True

        self.env.disable_operation_chaining()

        assert self.env.is_chaining_enabled() is False

    def test_get_checkpoint_config(self):

        checkpoint_config = self.env.get_checkpoint_config()

        assert isinstance(checkpoint_config, CheckpointConfig)

    def test_get_set_checkpoint_interval(self):

        self.env.enable_checkpointing(30000)

        interval = self.env.get_checkpoint_interval()

        assert interval == 30000

    def test_get_set_checkpointing_mode(self):
        mode = self.env.get_checkpointing_mode()
        assert mode == CheckpointingMode.EXACTLY_ONCE

        self.env.enable_checkpointing(30000, CheckpointingMode.AT_LEAST_ONCE)

        mode = self.env.get_checkpointing_mode()

        assert mode == CheckpointingMode.AT_LEAST_ONCE

    def test_get_set_stream_time_characteristic(self):

        default_time_characteristic = self.env.get_stream_time_characteristic()

        assert default_time_characteristic == TimeCharacteristic.ProcessingTime

        self.env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

        time_characteristic = self.env.get_stream_time_characteristic()

        assert time_characteristic == TimeCharacteristic.EventTime

    def test_get_execution_plan(self):
        tmp_dir = tempfile.gettempdir()
        source_path = os.path.join(tmp_dir + '/streaming.csv')
        tmp_csv = os.path.join(tmp_dir + '/streaming2.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()]

        t_env = StreamTableEnvironment.create(self.env)
        csv_source = CsvTableSource(source_path, field_names, field_types)
        t_env.register_table_source("Orders", csv_source)
        t_env.register_table_sink(
            "Results",
            field_names, field_types, CsvTableSink(tmp_csv))
        t_env.scan("Orders").insert_into("Results")

        plan = t_env.exec_env().get_execution_plan()

        json.loads(plan)
