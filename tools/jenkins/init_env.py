#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

# Because of end-to-end-performance-test running in the cluster, so This init_env.py only contains flink on
# standalone env, doesn't contains the init environment of maven, java and jenkins. Defaultly maven, java and jenkins
# environment are ready in the cluster
#

import os
from logger import logger
from utils import run_command
from restapi_common import execute_get


def init_standalone_env(host_list, user, source_path, dest_path):
    for host in host_list:
        cmd = "sudo su %s -c 'ssh %s \" rm -rf %s\"; scp -r %s %s@%s:%s'" % (user, host, dest_path, source_path,
                                                                             user, host, dest_path)
        logger.info("init_standalone_env  cmd:%s" % cmd)
        run_command(cmd)


def get_host_list(slave_file):
    hostlist = []
    with open(slave_file) as file:
        datas = file.read()
        for data in datas.split("\n"):
            if not(data == "" or data.startswith("#")):
                hostlist.append(data)
    return hostlist


def package(flink_home):
    cmd = "cd %s; mvn clean install -B -U -DskipTests -Drat.skip=true -Dcheckstyle.skip=true " % flink_home
    status, output = run_command(cmd)
    if status and output.find("BUILD SUCCESS") > 0:
        return True
    else:
        return False


def get_target(flink_home):
    cmd = "ls -lt %s/flink-dist/target/flink-*-bin/ |grep -v tar.gz"
    status, output = run_command(cmd)
    if status:
        target_file = output.split("\n")[0]
        return target_file, "%s/flink-dist/target/%s-bin/%s" % (flink_home, target_file, target_file)
    else:
        return "", ""


def update_conf_slaves(dest_path, slave_file):
    cmd = "cp %s %s/conf/" % (slave_file, dest_path)
    run_command(cmd)


def init_env():
    flink_home = os.getcwd()
    package_result = package(flink_home)
    if not package_result:
        logger.error("package error")
        return False
    slave_file = "%s/tool/jenkins/slaves" % flink_home
    host_list = get_host_list(slave_file)
    flink_path, source_path = get_target(flink_home)
    dest_path = "/home/admin/%s" % flink_path
    if source_path != "":
        update_conf_slaves(source_path, slave_file)
        init_standalone_env(host_list, source_path, dest_path)
        return True
    else:
        logger.error("find target file error")
        return False


if __name__ == "__main__":
    init_env()


