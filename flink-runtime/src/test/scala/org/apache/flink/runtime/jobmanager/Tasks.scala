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

package org.apache.flink.runtime.jobmanager

import org.apache.flink.runtime.checkpoint.{CheckpointMetaData, CheckpointMetrics, CheckpointOptions}
import org.apache.flink.runtime.execution.Environment
import org.apache.flink.runtime.io.network.api.reader.RecordReader
import org.apache.flink.runtime.io.network.api.writer.RecordWriter
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable
import org.apache.flink.runtime.state.TaskStateHandles
import org.apache.flink.types.IntValue


object Tasks {

  class Sender(environment: Environment, taskStateHandles: TaskStateHandles)
    extends AbstractInvokable(environment, taskStateHandles) {

    override def invoke(): Unit = {
      val writer = new RecordWriter[IntValue](getEnvironment.getWriter(0))

      try{
        writer.emit(new IntValue(42))
        writer.emit(new IntValue(1337))
        writer.flush()
      }finally{
        writer.clearBuffers()
      }
    }

    override def triggerCheckpoint(checkpointMetaData: CheckpointMetaData,
                                    checkpointOptions: CheckpointOptions): Boolean = {
      throw new UnsupportedOperationException(
        String.format("triggerCheckpoint not supported by %s", this.getClass.getName))
    }

    override def triggerCheckpointOnBarrier(checkpointMetaData: CheckpointMetaData,
                                            checkpointOptions: CheckpointOptions,
                                            checkpointMetrics: CheckpointMetrics): Unit = {
      throw new UnsupportedOperationException(
        String.format("triggerCheckpointOnBarrier not supported by %s", this.getClass.getName))
    }

    override def abortCheckpointOnBarrier(checkpointId: Long,
                                            cause: Throwable): Unit = {
      throw new UnsupportedOperationException(
        String.format("abortCheckpointOnBarrier not supported by %s", this.getClass.getName))
    }

    override def notifyCheckpointComplete(checkpointId: Long): Unit = {
      throw new UnsupportedOperationException(
        String.format("notifyCheckpointComplete not supported by %s", this.getClass.getName))
    }
  }

  class Forwarder(environment: Environment, taskStateHandles: TaskStateHandles)
    extends AbstractInvokable(environment, taskStateHandles) {

    override def invoke(): Unit = {
      val reader = new RecordReader[IntValue](
        getEnvironment.getInputGate(0),
        classOf[IntValue],
        getEnvironment.getTaskManagerInfo.getTmpDirectories)
      
      val writer = new RecordWriter[IntValue](getEnvironment.getWriter(0))

      try {
        while (true) {
          val record = reader.next()

          if (record == null) {
            return
          }

          writer.emit(record)
        }

        writer.flush()
      } finally {
        writer.clearBuffers()
      }
    }

    override def triggerCheckpoint(checkpointMetaData: CheckpointMetaData,
                                   checkpointOptions: CheckpointOptions): Boolean = {
      throw new UnsupportedOperationException(
        String.format("triggerCheckpoint not supported by %s", this.getClass.getName))
    }

    override def triggerCheckpointOnBarrier(checkpointMetaData: CheckpointMetaData,
                                            checkpointOptions: CheckpointOptions,
                                            checkpointMetrics: CheckpointMetrics): Unit = {
      throw new UnsupportedOperationException(
        String.format("triggerCheckpointOnBarrier not supported by %s", this.getClass.getName))
    }

    override def abortCheckpointOnBarrier(checkpointId: Long,
                                          cause: Throwable): Unit = {
      throw new UnsupportedOperationException(
        String.format("abortCheckpointOnBarrier not supported by %s", this.getClass.getName))
    }

    override def notifyCheckpointComplete(checkpointId: Long): Unit = {
      throw new UnsupportedOperationException(
        String.format("notifyCheckpointComplete not supported by %s", this.getClass.getName))
    }
  }

  class Receiver(environment: Environment, taskStateHandles: TaskStateHandles)
    extends AbstractInvokable(environment, taskStateHandles) {

    override def invoke(): Unit = {
      val reader = new RecordReader[IntValue](
        getEnvironment.getInputGate(0),
        classOf[IntValue],
        getEnvironment.getTaskManagerInfo.getTmpDirectories)

      val i1 = reader.next()
      val i2 = reader.next()
      val i3 = reader.next()

      if(i1.getValue != 42 || i2.getValue != 1337 || i3 != null){
        throw new Exception("Wrong data received.")
      }
    }

    override def triggerCheckpoint(checkpointMetaData: CheckpointMetaData,
                                   checkpointOptions: CheckpointOptions): Boolean = {
      throw new UnsupportedOperationException(
        String.format("triggerCheckpoint not supported by %s", this.getClass.getName))
    }

    override def triggerCheckpointOnBarrier(checkpointMetaData: CheckpointMetaData,
                                            checkpointOptions: CheckpointOptions,
                                            checkpointMetrics: CheckpointMetrics): Unit = {
      throw new UnsupportedOperationException(
        String.format("triggerCheckpointOnBarrier not supported by %s", this.getClass.getName))
    }

    override def abortCheckpointOnBarrier(checkpointId: Long,
                                          cause: Throwable): Unit = {
      throw new UnsupportedOperationException(
        String.format("abortCheckpointOnBarrier not supported by %s", this.getClass.getName))
    }

    override def notifyCheckpointComplete(checkpointId: Long): Unit = {
      throw new UnsupportedOperationException(
        String.format("notifyCheckpointComplete not supported by %s", this.getClass.getName))
    }
  }

  class FailingOnceReceiver(environment: Environment, taskStateHandles: TaskStateHandles)
    extends Receiver(environment, taskStateHandles) {
    import FailingOnceReceiver.failed

    override def invoke(): Unit = {
      if(!failed && getEnvironment.getTaskInfo.getIndexOfThisSubtask == 0){
        failed = true
        throw new Exception("Test exception.")
      }else{
        super.invoke()
      }
    }
  }

  object FailingOnceReceiver{
    var failed = false
  }

  class BlockingOnceReceiver(environment: Environment, taskStateHandles: TaskStateHandles)
    extends Receiver(environment, taskStateHandles) {
    import BlockingOnceReceiver.blocking

    override def invoke(): Unit = {
      if(blocking) {
        val o = new Object
        o.synchronized{
          o.wait()
        }
      } else {
        super.invoke()
      }
    }

  }

  object BlockingOnceReceiver{
    var blocking = true
  }

  class AgnosticReceiver(environment: Environment, taskStateHandles: TaskStateHandles)
    extends AbstractInvokable(environment, taskStateHandles) {

    override def invoke(): Unit = {
      val reader= new RecordReader[IntValue](
        getEnvironment.getInputGate(0),
        classOf[IntValue],
        getEnvironment.getTaskManagerInfo.getTmpDirectories)

      while(reader.next() != null){}
    }

    override def triggerCheckpoint(checkpointMetaData: CheckpointMetaData,
                                   checkpointOptions: CheckpointOptions): Boolean = {
      throw new UnsupportedOperationException(
        String.format("triggerCheckpoint not supported by %s", this.getClass.getName))
    }

    override def triggerCheckpointOnBarrier(checkpointMetaData: CheckpointMetaData,
                                            checkpointOptions: CheckpointOptions,
                                            checkpointMetrics: CheckpointMetrics): Unit = {
      throw new UnsupportedOperationException(
        String.format("triggerCheckpointOnBarrier not supported by %s", this.getClass.getName))
    }

    override def abortCheckpointOnBarrier(checkpointId: Long,
                                          cause: Throwable): Unit = {
      throw new UnsupportedOperationException(
        String.format("abortCheckpointOnBarrier not supported by %s", this.getClass.getName))
    }

    override def notifyCheckpointComplete(checkpointId: Long): Unit = {
      throw new UnsupportedOperationException(
        String.format("notifyCheckpointComplete not supported by %s", this.getClass.getName))
    }
  }

  class AgnosticBinaryReceiver(environment: Environment, taskStateHandles: TaskStateHandles)
    extends AbstractInvokable(environment, taskStateHandles) {

    override def invoke(): Unit = {
      val reader1 = new RecordReader[IntValue](
        getEnvironment.getInputGate(0),
        classOf[IntValue],
        getEnvironment.getTaskManagerInfo.getTmpDirectories)
      
      val reader2 = new RecordReader[IntValue](
        getEnvironment.getInputGate(1),
        classOf[IntValue],
        getEnvironment.getTaskManagerInfo.getTmpDirectories)

      while(reader1.next() != null){}
      while(reader2.next() != null){}
    }

    override def triggerCheckpoint(checkpointMetaData: CheckpointMetaData,
                                   checkpointOptions: CheckpointOptions): Boolean = {
      throw new UnsupportedOperationException(
        String.format("triggerCheckpoint not supported by %s", this.getClass.getName))
    }

    override def triggerCheckpointOnBarrier(checkpointMetaData: CheckpointMetaData,
                                            checkpointOptions: CheckpointOptions,
                                            checkpointMetrics: CheckpointMetrics): Unit = {
      throw new UnsupportedOperationException(
        String.format("triggerCheckpointOnBarrier not supported by %s", this.getClass.getName))
    }

    override def abortCheckpointOnBarrier(checkpointId: Long,
                                          cause: Throwable): Unit = {
      throw new UnsupportedOperationException(
        String.format("abortCheckpointOnBarrier not supported by %s", this.getClass.getName))
    }

    override def notifyCheckpointComplete(checkpointId: Long): Unit = {
      throw new UnsupportedOperationException(
        String.format("notifyCheckpointComplete not supported by %s", this.getClass.getName))
    }
  }

  class AgnosticTertiaryReceiver(environment: Environment, taskStateHandles: TaskStateHandles)
    extends AbstractInvokable(environment, taskStateHandles) {

    override def invoke(): Unit = {
      val env = getEnvironment

      val reader1 = new RecordReader[IntValue](
        env.getInputGate(0),
        classOf[IntValue],
        getEnvironment.getTaskManagerInfo.getTmpDirectories)
      
      val reader2 = new RecordReader[IntValue](
        env.getInputGate(1),
        classOf[IntValue],
        getEnvironment.getTaskManagerInfo.getTmpDirectories)
      
      val reader3 = new RecordReader[IntValue](
        env.getInputGate(2),
        classOf[IntValue],
        getEnvironment.getTaskManagerInfo.getTmpDirectories)

      while(reader1.next() != null){}
      while(reader2.next() != null){}
      while(reader3.next() != null){}
    }

    override def triggerCheckpoint(checkpointMetaData: CheckpointMetaData,
                                   checkpointOptions: CheckpointOptions): Boolean = {
      throw new UnsupportedOperationException(
        String.format("triggerCheckpoint not supported by %s", this.getClass.getName))
    }

    override def triggerCheckpointOnBarrier(checkpointMetaData: CheckpointMetaData,
                                            checkpointOptions: CheckpointOptions,
                                            checkpointMetrics: CheckpointMetrics): Unit = {
      throw new UnsupportedOperationException(
        String.format("triggerCheckpointOnBarrier not supported by %s", this.getClass.getName))
    }

    override def abortCheckpointOnBarrier(checkpointId: Long,
                                          cause: Throwable): Unit = {
      throw new UnsupportedOperationException(
        String.format("abortCheckpointOnBarrier not supported by %s", this.getClass.getName))
    }

    override def notifyCheckpointComplete(checkpointId: Long): Unit = {
      throw new UnsupportedOperationException(
        String.format("notifyCheckpointComplete not supported by %s", this.getClass.getName))
    }
  }

  class ExceptionSender(environment: Environment, taskStateHandles: TaskStateHandles)
    extends AbstractInvokable(environment, taskStateHandles) {

    override def invoke(): Unit = {
      throw new Exception("Test exception")
    }

    override def triggerCheckpoint(checkpointMetaData: CheckpointMetaData,
                                   checkpointOptions: CheckpointOptions): Boolean = {
      throw new UnsupportedOperationException(
        String.format("triggerCheckpoint not supported by %s", this.getClass.getName))
    }

    override def triggerCheckpointOnBarrier(checkpointMetaData: CheckpointMetaData,
                                            checkpointOptions: CheckpointOptions,
                                            checkpointMetrics: CheckpointMetrics): Unit = {
      throw new UnsupportedOperationException(
        String.format("triggerCheckpointOnBarrier not supported by %s", this.getClass.getName))
    }

    override def abortCheckpointOnBarrier(checkpointId: Long,
                                          cause: Throwable): Unit = {
      throw new UnsupportedOperationException(
        String.format("abortCheckpointOnBarrier not supported by %s", this.getClass.getName))
    }

    override def notifyCheckpointComplete(checkpointId: Long): Unit = {
      throw new UnsupportedOperationException(
        String.format("notifyCheckpointComplete not supported by %s", this.getClass.getName))
    }
  }

  class SometimesExceptionSender(environment: Environment, taskStateHandles: TaskStateHandles)
    extends AbstractInvokable(environment, taskStateHandles) {

    override def invoke(): Unit = {
      // this only works if the TaskManager runs in the same JVM as the test case
      if(SometimesExceptionSender.failingSenders.contains(this.getIndexInSubtaskGroup)){
        throw new Exception("Test exception")
      }else{
        val o = new Object()
        o.synchronized(o.wait())
      }
    }

    override def triggerCheckpoint(checkpointMetaData: CheckpointMetaData,
                                   checkpointOptions: CheckpointOptions): Boolean = {
      throw new UnsupportedOperationException(
        String.format("triggerCheckpoint not supported by %s", this.getClass.getName))
    }

    override def triggerCheckpointOnBarrier(checkpointMetaData: CheckpointMetaData,
                                            checkpointOptions: CheckpointOptions,
                                            checkpointMetrics: CheckpointMetrics): Unit = {
      throw new UnsupportedOperationException(
        String.format("triggerCheckpointOnBarrier not supported by %s", this.getClass.getName))
    }

    override def abortCheckpointOnBarrier(checkpointId: Long,
                                          cause: Throwable): Unit = {
      throw new UnsupportedOperationException(
        String.format("abortCheckpointOnBarrier not supported by %s", this.getClass.getName))
    }

    override def notifyCheckpointComplete(checkpointId: Long): Unit = {
      throw new UnsupportedOperationException(
        String.format("notifyCheckpointComplete not supported by %s", this.getClass.getName))
    }
  }

  object SometimesExceptionSender {
    var failingSenders = Set[Int](0)
  }

  class ExceptionReceiver(environment: Environment, taskStateHandles: TaskStateHandles)
    extends AbstractInvokable(environment, taskStateHandles) {

    override def invoke(): Unit = {
      throw new Exception("Test exception")
    }

    override def triggerCheckpoint(checkpointMetaData: CheckpointMetaData,
                                   checkpointOptions: CheckpointOptions): Boolean = {
      throw new UnsupportedOperationException(
        String.format("triggerCheckpoint not supported by %s", this.getClass.getName))
    }

    override def triggerCheckpointOnBarrier(checkpointMetaData: CheckpointMetaData,
                                            checkpointOptions: CheckpointOptions,
                                            checkpointMetrics: CheckpointMetrics): Unit = {
      throw new UnsupportedOperationException(
        String.format("triggerCheckpointOnBarrier not supported by %s", this.getClass.getName))
    }

    override def abortCheckpointOnBarrier(checkpointId: Long,
                                          cause: Throwable): Unit = {
      throw new UnsupportedOperationException(
        String.format("abortCheckpointOnBarrier not supported by %s", this.getClass.getName))
    }

    override def notifyCheckpointComplete(checkpointId: Long): Unit = {
      throw new UnsupportedOperationException(
        String.format("notifyCheckpointComplete not supported by %s", this.getClass.getName))
    }
  }

  class InstantiationErrorSender(environment: Environment, taskStateHandles: TaskStateHandles)
    extends AbstractInvokable(environment, taskStateHandles) {
    throw new RuntimeException("Test exception in constructor")

    override def invoke(): Unit = {
    }

    override def triggerCheckpoint(checkpointMetaData: CheckpointMetaData,
                                   checkpointOptions: CheckpointOptions): Boolean = {
      throw new UnsupportedOperationException(
        String.format("triggerCheckpoint not supported by %s", this.getClass.getName))
    }

    override def triggerCheckpointOnBarrier(checkpointMetaData: CheckpointMetaData,
                                            checkpointOptions: CheckpointOptions,
                                            checkpointMetrics: CheckpointMetrics): Unit = {
      throw new UnsupportedOperationException(
        String.format("triggerCheckpointOnBarrier not supported by %s", this.getClass.getName))
    }

    override def abortCheckpointOnBarrier(checkpointId: Long,
                                          cause: Throwable): Unit = {
      throw new UnsupportedOperationException(
        String.format("abortCheckpointOnBarrier not supported by %s", this.getClass.getName))
    }

    override def notifyCheckpointComplete(checkpointId: Long): Unit = {
      throw new UnsupportedOperationException(
        String.format("notifyCheckpointComplete not supported by %s", this.getClass.getName))
    }
  }

  class SometimesInstantiationErrorSender(environment: Environment,
                                          taskStateHandles: TaskStateHandles)
    extends AbstractInvokable(environment, taskStateHandles) {

    // this only works if the TaskManager runs in the same JVM as the test case
    if(SometimesInstantiationErrorSender.failingSenders.contains(this.getIndexInSubtaskGroup)){
      throw new RuntimeException("Test exception in constructor")
    }

    override def invoke(): Unit = {
      val o = new Object()
      o.synchronized(o.wait())
    }

    override def triggerCheckpoint(checkpointMetaData: CheckpointMetaData,
                                   checkpointOptions: CheckpointOptions): Boolean = {
      throw new UnsupportedOperationException(
        String.format("triggerCheckpoint not supported by %s", this.getClass.getName))
    }

    override def triggerCheckpointOnBarrier(checkpointMetaData: CheckpointMetaData,
                                            checkpointOptions: CheckpointOptions,
                                            checkpointMetrics: CheckpointMetrics): Unit = {
      throw new UnsupportedOperationException(
        String.format("triggerCheckpointOnBarrier not supported by %s", this.getClass.getName))
    }

    override def abortCheckpointOnBarrier(checkpointId: Long,
                                          cause: Throwable): Unit = {
      throw new UnsupportedOperationException(
        String.format("abortCheckpointOnBarrier not supported by %s", this.getClass.getName))
    }

    override def notifyCheckpointComplete(checkpointId: Long): Unit = {
      throw new UnsupportedOperationException(
        String.format("notifyCheckpointComplete not supported by %s", this.getClass.getName))
    }
  }

  object SometimesInstantiationErrorSender {
    var failingSenders = Set[Int](0)
  }

  class BlockingReceiver(environment: Environment, taskStateHandles: TaskStateHandles)
    extends AbstractInvokable(environment, taskStateHandles) {
    override def invoke(): Unit = {
      val o = new Object
      o.synchronized(
        o.wait()
      )
    }

    override def triggerCheckpoint(checkpointMetaData: CheckpointMetaData,
                                   checkpointOptions: CheckpointOptions): Boolean = {
      throw new UnsupportedOperationException(
        String.format("triggerCheckpoint not supported by %s", this.getClass.getName))
    }

    override def triggerCheckpointOnBarrier(checkpointMetaData: CheckpointMetaData,
                                            checkpointOptions: CheckpointOptions,
                                            checkpointMetrics: CheckpointMetrics): Unit = {
      throw new UnsupportedOperationException(
        String.format("triggerCheckpointOnBarrier not supported by %s", this.getClass.getName))
    }

    override def abortCheckpointOnBarrier(checkpointId: Long,
                                          cause: Throwable): Unit = {
      throw new UnsupportedOperationException(
        String.format("abortCheckpointOnBarrier not supported by %s", this.getClass.getName))
    }

    override def notifyCheckpointComplete(checkpointId: Long): Unit = {
      throw new UnsupportedOperationException(
        String.format("notifyCheckpointComplete not supported by %s", this.getClass.getName))
    }
  }
}
