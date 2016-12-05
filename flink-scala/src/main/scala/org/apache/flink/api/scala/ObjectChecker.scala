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

package org.apache.flink.api.scala

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.InvalidProgramException

/**
  * Scala Object checker tries to verify if a class is implemented by
  * Scala Object
  */
@Internal
object ObjectChecker {
  def isSingleton(obj: Any): Boolean = {
    obj.getClass.getFields.map(_.getName) contains "MODULE$"
  }

  def assertScalaSingleton(a: Any) = {
    if (isSingleton(a)) {
      val msg = "User defined function implemented by class " + a.getClass.getName +
        " might be implemented by a Scala Object," +
        "it is forbidden by Flink since concurrent modification risks."
      throw new InvalidProgramException(msg)
    }
  }
}
