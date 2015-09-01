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

package org.apache.flink.yarn

import java.util.UUID

import akka.actor._
import grizzled.slf4j.Logger
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.leaderretrieval.{LeaderRetrievalListener, LeaderRetrievalService}
import org.apache.flink.runtime.{LeaderSessionMessageFilter, FlinkActor, LogMessages}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.yarn.FlinkYarnClusterStatus
import org.apache.flink.yarn.Messages._
import scala.collection.mutable
import scala.concurrent.duration._

import scala.language.postfixOps

class ApplicationClient(
    val flinkConfig: Configuration,
    val leaderRetrievalService: LeaderRetrievalService)
  extends FlinkActor
  with LeaderSessionMessageFilter
  with LogMessages
  with LeaderRetrievalListener{
  import context._

  val log = Logger(getClass)

  val INITIAL_POLLING_DELAY = 0 seconds
  val WAIT_FOR_YARN_INTERVAL = 2 seconds
  val POLLING_INTERVAL = 3 seconds

  var yarnJobManager: Option[ActorRef] = None
  var pollingTimer: Option[Cancellable] = None
  implicit val timeout: FiniteDuration = AkkaUtils.getTimeout(flinkConfig)
  var running = false
  var messagesQueue : mutable.Queue[YarnMessage] = mutable.Queue[YarnMessage]()
  var latestClusterStatus : Option[FlinkYarnClusterStatus] = None
  var stopMessageReceiver : Option[ActorRef] = None

  var leaderSessionID: Option[UUID] = None

  override def preStart(): Unit = {
    super.preStart()

    try {
      leaderRetrievalService.start(this)
    } catch {
      case e: Exception =>
        log.error("Could not start the leader retrieval service.", e)
        throw new RuntimeException("Could not start the leader retrieval service.", e)
    }
  }

  override def postStop(): Unit = {
    log.info("Stopped Application client.")

    disconnectFromJobManager()

    try {
      leaderRetrievalService.stop()
    } catch {
      case e: Exception => log.error("Leader retrieval service did not shout down properly.")
    }

    // Terminate the whole actor system because there is only the application client running
    context.system.shutdown()
  }

  override def handleMessage: Receive = {
    // ----------------------------- Registration -> Status updates -> shutdown ----------------

    case TriggerApplicationClientRegistration(jobManagerAkkaURL, timeout, deadline) =>
      if (isConnected) {
        // we are already connected to the job manager
        log.debug("ApplicationClient is already registered to the " +
          s"JobManager ${yarnJobManager.get}.")
      } else {
        if (deadline.forall(_.isOverdue())) {
          // we failed to register in time. That means we should quit
          log.error(s"Failed to register at the JobManager with address ${jobManagerAkkaURL}. " +
            s"Shutting down...")

          self ! decorateMessage(PoisonPill)
        } else {
          log.info(s"Trying to register at JobManager $jobManagerAkkaURL.")

          val jobManager = context.actorSelection(jobManagerAkkaURL)

          jobManager ! decorateMessage(
            RegisterApplicationClient
          )

          val nextTimeout = (timeout * 2).min(ApplicationClient.MAX_REGISTRATION_TIMEOUT)

          context.system.scheduler.scheduleOnce(
            timeout,
            self,
            decorateMessage(
              TriggerApplicationClientRegistration(
                jobManagerAkkaURL,
                nextTimeout,
                deadline
              )
            )
          )(context.dispatcher)
        }
      }

    case AcknowledgeApplicationClientRegistration =>
      val jm = sender

      log.info(s"Successfully registered at the JobManager ${jm}")

      yarnJobManager = Some(jm)

      // schedule a periodic status report from the JobManager
      // request the number of task managers and slots from the job manager
      pollingTimer = Some(
        context.system.scheduler.schedule(
          INITIAL_POLLING_DELAY,
          WAIT_FOR_YARN_INTERVAL,
          yarnJobManager.get,
          decorateMessage(PollYarnClusterStatus))
      )

    case JobManagerLeaderAddress(jobManagerAkkaURL, newLeaderSessionID) =>
      log.info(s"Received address of new leader $jobManagerAkkaURL with session ID" +
        s" $newLeaderSessionID.")
      disconnectFromJobManager()

      leaderSessionID = Some(newLeaderSessionID)

      val maxRegistrationDuration = ApplicationClient.MAX_REGISTRATION_DURATION

      val deadline = if (maxRegistrationDuration.isFinite()) {
        Some(maxRegistrationDuration.fromNow)
      } else {
        None
      }

      // trigger registration at new leader
      self ! decorateMessage(
        TriggerApplicationClientRegistration(
          jobManagerAkkaURL,
          ApplicationClient.INITIAL_REGISTRATION_TIMEOUT,
          deadline))

    case LocalStopYarnSession(status, diagnostics) =>
      log.info("Sending StopYarnSession request to ApplicationMaster.")
      stopMessageReceiver = Some(sender)
      yarnJobManager foreach {
        _ ! decorateMessage(StopYarnSession(status, diagnostics))
      }

    case JobManagerStopped =>
      log.info("Remote JobManager has been stopped successfully. " +
        "Stopping local application client")
      stopMessageReceiver foreach {
        _ ! decorateMessage(JobManagerStopped)
      }
      // poison ourselves
      self ! decorateMessage(PoisonPill)

    // handle the responses from the PollYarnClusterStatus messages to the yarn job mgr
    case status: FlinkYarnClusterStatus =>
      latestClusterStatus = Some(status)


    // locally get cluster status
    case LocalGetYarnClusterStatus =>
      sender() ! decorateMessage(latestClusterStatus)

    // Forward message to Application Master
    case LocalStopAMAfterJob(jobID) =>
      yarnJobManager foreach {
        _ forward decorateMessage(StopAMAfterJob(jobID))
      }

    // -----------------  handle messages from the cluster -------------------
    // receive remote messages
    case msg: YarnMessage =>
      log.debug(s"Received new YarnMessage $msg. Now ${messagesQueue.size} messages in queue")
      messagesQueue.enqueue(msg)

    // locally forward messages
    case LocalGetYarnMessage =>
      if(messagesQueue.size > 0) {
        sender() ! decorateMessage(Option(messagesQueue.dequeue))
      } else {
        sender() ! decorateMessage(None)
      }
  }

  def disconnectFromJobManager(): Unit = {
    log.info(s"Disconnect from JobManager ${yarnJobManager.getOrElse(ActorRef.noSender)}.")

    yarnJobManager foreach {
      _ ! decorateMessage(UnregisterClient)
    }

    pollingTimer foreach {
      _.cancel()
    }

    yarnJobManager = None
    leaderSessionID = None
    pollingTimer = None
  }

  def isConnected: Boolean = {
    yarnJobManager.isDefined
  }

  /**
   * Handle unmatched messages with an exception.
   */
  override def unhandled(message: Any): Unit = {
    // let the actor crash
    throw new RuntimeException("Received unknown message " + message)
  }

  override def notifyLeaderAddress(leaderAddress: String, leaderSessionID: UUID): Unit = {
    log.info(s"Notification about new leader address $leaderAddress with " +
      s"session ID $leaderSessionID.")
    self ! JobManagerLeaderAddress(leaderAddress, leaderSessionID)
  }

  override def handleError(exception: Exception): Unit = {
    log.error("Error in leader retrieval service.", exception)

    self ! decorateMessage(PoisonPill)
  }
}

object ApplicationClient {
  val INITIAL_REGISTRATION_TIMEOUT: FiniteDuration = 500 milliseconds
  val MAX_REGISTRATION_DURATION: FiniteDuration = 5 minutes
  val MAX_REGISTRATION_TIMEOUT = 5 minutes
}
