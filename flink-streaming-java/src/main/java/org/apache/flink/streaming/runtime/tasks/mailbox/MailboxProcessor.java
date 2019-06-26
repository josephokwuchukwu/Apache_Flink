/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks.mailbox;

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * This class encapsulates the logic of the mailbox-based execution model. At the core of this model
 * {@link #runMailboxLoop()} that continuously executes the provided {@link MailboxDefaultAction} in a loop. On each
 * iteration, the method also checks if there are pending actions in the mailbox and executes such actions. This model
 * ensures single-threaded execution between the default action (e.g. record processing) and mailbox actions (e.g.
 * checkpoint trigger, timer firing, ...).
 *
 * <p>The {@link MailboxDefaultAction} interacts with this class through the {@link MailboxDefaultActionContext} to
 * communicate control flow changes to the mailbox loop, e.g. that invocations of the default action are temporarily
 * or permanently exhausted.
 *
 * <p>The design of {@link #runMailboxLoop()} is centered around the idea of keeping the expected hot path
 * (default action, no mail) as fast as possible, with just a single volatile read per iteration in
 * {@link Mailbox#hasMail}. This means that all checking of mail and other control flags (mailboxLoopRunning,
 * suspendedDefaultAction) are always connected to #hasMail indicating true. This means that control flag changes in
 * the mailbox thread can be done directly, but we must ensure that there is at least one action in the mailbox so that
 * the change is picked up. For control flag changes by all other threads, that must happen through mailbox actions,
 * this is automatically the case.
 *
 * <p>This class has a open-prepareClose-close lifecycle that is connected with and maps to the lifecycle of the
 * encapsulated {@link Mailbox} (which is open-quiesce-close).
 *
 * <p>The method {@link #switchToLegacySourceCompatibilityMailboxLoop(Object)} exists to run the current sources
 * (see {@link org.apache.flink.streaming.runtime.tasks.SourceStreamTask} with the mailbox model in a compatibility
 * mode. Once we drop the old source interface for the new one (FLIP-27) this method can eventually go away.
 */
public class MailboxProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(MailboxProcessor.class);

	/** The mailbox data-structure that manages request for special actions, like timers, checkpoints, ... */
	private final Mailbox mailbox;

	/** Executor-style facade for client code to submit actions to the mailbox. */
	private final TaskMailboxExecutorService taskMailboxExecutor;

	/** Action that is repeatedly executed if no action request is in the mailbox. Typically record processing. */
	private final MailboxDefaultAction mailboxDefaultAction;

	/** Control flag to terminate the mailbox loop. Must only be accessed from mailbox thread. */
	private boolean mailboxLoopRunning;

	/**
	 * Remembers a currently active suspension of the default action. Serves as flag to indicate a suspended
	 * default action (suspended if not-null) and to reuse the object as return value in consecutive suspend attempts.
	 * Must only be accessed from mailbox thread.
	 */
	private MailboxDefaultAction.SuspendedDefaultAction suspendedDefaultAction;

	/** Special action that is used to terminate the mailbox loop. */
	private final Runnable mailboxPoisonLetter;

	public MailboxProcessor(MailboxDefaultAction mailboxDefaultAction) {
		this.mailboxDefaultAction = Preconditions.checkNotNull(mailboxDefaultAction);
		this.mailbox = new MailboxImpl();
		this.taskMailboxExecutor = new TaskMailboxExecutorServiceImpl(mailbox);
		this.mailboxPoisonLetter = () -> mailboxLoopRunning = false;
		this.mailboxLoopRunning = true;
		this.suspendedDefaultAction = null;
	}

	/**
	 * Returns an executor service facade to submit actions to the mailbox.
	 */
	public TaskMailboxExecutorService getTaskMailboxExecutor() {
		return taskMailboxExecutor;
	}

	/**
	 * Lifecycle method to open the mailbox for action submission.
	 */
	public void open() {
		mailbox.open();
	}

	/**
	 * Lifecycle method to close the mailbox for action submission.
	 */
	public void prepareClose() {
		taskMailboxExecutor.shutdown();
	}

	/**
	 * Lifecycle method to close the mailbox for action submission/retrieval. This will cancel all instances of
	 * {@link java.util.concurrent.RunnableFuture} that are still contained in the mailbox.
	 */
	public void close() {
		FutureUtils.cancelRunnableFutures(taskMailboxExecutor.shutdownNow());
	}

	/**
	 * Runs the mailbox processing loop. This is where the main work is done.
	 */
	public void runMailboxLoop() throws Exception {

		assert taskMailboxExecutor.isMailboxThread() :
			"StreamTask::run must be executed by declared mailbox thread!";

		final Mailbox localMailbox = mailbox;

		assert localMailbox.getState() == Mailbox.State.OPEN : "Mailbox must be opened!";

		final MailboxDefaultActionContext defaultActionContext = new MailboxDefaultActionContext(this);

		while (processMail(localMailbox)) {
			mailboxDefaultAction.runDefaultAction(defaultActionContext);
		}
	}

	/**
	 * This method exists to run the current sources with the mailbox model in a compatibility mode. Once we drop the
	 * old source interface for the new one (FLIP-27) this method can eventually go away.
	 *
	 * @param checkpointLock the task's checkpointing lock.
	 * @throws MailboxStateException if mailbox is closed.
	 * @throws InterruptedException on interruption.
	 */
	public void switchToLegacySourceCompatibilityMailboxLoop(
		final Object checkpointLock) throws MailboxStateException, InterruptedException {

		assert taskMailboxExecutor.isMailboxThread() :
			"Legacy source compatibility mailbox loop must run in mailbox thread!";

		while (isMailboxLoopRunning()) {

			Runnable letter = mailbox.takeMail();

			synchronized (checkpointLock) {
				letter.run();
			}
		}
	}

	/**
	 * Cancels the mailbox loop execution. All pending mailbox actions will not be executed anymore, if they are
	 * instance of {@link java.util.concurrent.RunnableFuture}, they will be cancelled.
	 */
	public void cancelMailboxExecution() {
		try {
			List<Runnable> droppedRunnables = mailbox.clearAndPut(mailboxPoisonLetter);
			FutureUtils.cancelRunnableFutures(droppedRunnables);
		} catch (MailboxStateException msex) {
			LOG.debug("Mailbox already closed in cancel().", msex);
		}
	}

	/**
	 * This method must be called to end the stream task when all actions for the tasks have been performed.
	 */
	public void allActionsCompleted() {
		try {
			if (taskMailboxExecutor.isMailboxThread()) {
				mailboxLoopRunning = false;
				ensureControlFlowSignalCheck();
			} else {
				mailbox.putFirst(mailboxPoisonLetter);
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (MailboxStateException me) {
			LOG.debug("Action context could not submit poison letter to mailbox.", me);
		}
	}

	/**
	 * This helper method handles all special actions from the mailbox. It returns true if the mailbox loop should
	 * continue running, false if it should stop. In the current design, this method also evaluates all control flag
	 * changes. This keeps the hot path in {@link #runMailboxLoop()} free from any other flag checking, at the cost
	 * that all flag changes must make sure that the mailbox signals mailbox#hasMail.
	 */
	private boolean processMail(Mailbox mailbox) throws MailboxStateException, InterruptedException {

		if (!mailbox.hasMail()) {
			// We can directly return true because all changes to #isMailboxLoopRunning must be connected to
			// mailbox.hasMail() == true.
			return true;
		}

		// TODO consider batched draining into list and/or limit number of executed letters
		// Take letters in a non-blockingly and execute them.
		Optional<Runnable> maybeLetter;
		while (isMailboxLoopRunning() && (maybeLetter = mailbox.tryTakeMail()).isPresent()) {
			maybeLetter.get().run();
		}

		// If the default action is currently not available, we can run a blocking mailbox execution until the default
		// action becomes available again.
		while (isDefaultActionUnavailable() && isMailboxLoopRunning()) {
			mailbox.takeMail().run();
		}

		return isMailboxLoopRunning();
	}

	/**
	 * Calling this method signals that the mailbox-thread should (temporarily) stop invoking the default action,
	 * e.g. because there is currently no input available.
	 */
	private MailboxDefaultAction.SuspendedDefaultAction suspendDefaultAction() {

		assert taskMailboxExecutor.isMailboxThread();

		if (suspendedDefaultAction == null) {
			suspendedDefaultAction = new SuspendDefaultActionRunnable();
			ensureControlFlowSignalCheck();
		}

		return suspendedDefaultAction;
	}

	private boolean isDefaultActionUnavailable() {
		return suspendedDefaultAction != null;
	}

	private boolean isMailboxLoopRunning() {
		return mailboxLoopRunning;
	}

	/**
	 * Helper method to make sure that the mailbox loop will check the control flow flags in the next iteration.
	 */
	private void ensureControlFlowSignalCheck() {
		// Make sure that mailbox#hasMail is true via a dummy letter so that the flag change is noticed.
		if (!mailbox.hasMail()) {
			try {
				mailbox.tryPutMail(() -> {});
			} catch (MailboxStateException me) {
				LOG.debug("Mailbox closed when trying to submit letter for control flow signal.", me);
			}
		}
	}

	/**
	 * Implementation of {@link MailboxDefaultAction.ActionContext} that is connected to a {@link MailboxProcessor}
	 * instance.
	 */
	private final class MailboxDefaultActionContext implements MailboxDefaultAction.ActionContext {

		private final MailboxProcessor mailboxProcessor;

		private MailboxDefaultActionContext(MailboxProcessor mailboxProcessor) {
			this.mailboxProcessor = mailboxProcessor;
		}

		@Override
		public void allActionsCompleted() {
			mailboxProcessor.allActionsCompleted();
		}

		@Override
		public MailboxDefaultAction.SuspendedDefaultAction suspendDefaultAction() {
			return mailboxProcessor.suspendDefaultAction();
		}
	}

	/**
	 * Represents the suspended state of the default action and offers an idempotent method to resume execution.
	 */
	private final class SuspendDefaultActionRunnable implements MailboxDefaultAction.SuspendedDefaultAction {

		/** Ensuring idempotent behavior, we ensure this is only accessed from the main thread. */
		private boolean valid;

		SuspendDefaultActionRunnable() {
			this.valid = true;
		}

		@Override
		public void resume() {
			try {
				if (taskMailboxExecutor.isMailboxThread()) {
					resumeInternal();
				} else {
					mailbox.putMail(this::resumeInternal);
				}
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
			} catch (MailboxStateException me) {
				LOG.debug("Action context could not submit letter to mailbox.", me);
			}
		}

		private void resumeInternal() {
			if (valid) {
				valid = false;
				suspendedDefaultAction = null;
			}
		}
	}
}
