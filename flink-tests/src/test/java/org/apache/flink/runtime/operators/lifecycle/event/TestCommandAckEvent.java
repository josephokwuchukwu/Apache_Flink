/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.operators.lifecycle.event;

import org.apache.flink.runtime.operators.lifecycle.command.TestCommand;

/** An event indicating that a {@link TestCommand} was received and either executed or dropped. */
public class TestCommandAckEvent extends TestEvent {
    private final TestCommand command;
    private final int attemptNumber;

    public TestCommandAckEvent(
            String operatorId, int subtaskIndex, int attemptNumber, TestCommand command) {
        super(operatorId, subtaskIndex);
        this.command = command;
        this.attemptNumber = attemptNumber;
    }

    public TestCommand getCommand() {
        return command;
    }

    public int getAttemptNumber() {
        return attemptNumber;
    }

    @Override
    public String toString() {
        return super.toString() + ", attemptNumber=" + attemptNumber + ", command=" + command;
    }
}
