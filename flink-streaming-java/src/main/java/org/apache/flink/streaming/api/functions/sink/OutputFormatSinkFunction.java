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

package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.OutputFormat.InitializationContext;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Simple implementation of the SinkFunction writing tuples in the specified OutputFormat format.
 *
 * @param <IN> Input type
 * @deprecated Please use the {@link
 *     org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink StreamingFileSink}
 *     for writing to files from a streaming program.
 */
@PublicEvolving
@Deprecated
public class OutputFormatSinkFunction<IN> extends RichSinkFunction<IN>
        implements InputTypeConfigurable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(OutputFormatSinkFunction.class);

    private OutputFormat<IN> format;
    private boolean cleanupCalled = false;

    public OutputFormatSinkFunction(OutputFormat<IN> format) {
        this.format = format;
    }

    /**
     * Initialization method for the {@link OutputFormatSinkFunction}.
     *
     * @param parameters The configuration containing the parameters attached to the contract.
     * @throws Exception if an error happens.
     * @deprecated This method is deprecated since Flink 1.19. The users are recommended to
     *     implement {@code open(OpenContext openContext)} and override {@code open(Configuration
     *     parameters)} with an empty body instead. 1. If you implement {@code open(OpenContext
     *     openContext)}, the {@code open(OpenContext openContext)} will be invoked and the {@code
     *     open(Configuration parameters)} won't be invoked. 2. If you don't implement {@code
     *     open(OpenContext openContext)}, the {@code open(Configuration parameters)} will be
     *     invoked in the default implementation of the {@code open(OpenContext openContext)}.
     * @see <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=263425231">
     *     FLIP-344: Remove parameter in RichFunction#open </a>
     */
    @Deprecated
    @Override
    public void open(Configuration parameters) throws Exception {
        RuntimeContext context = getRuntimeContext();
        format.configure(parameters);
        int indexInSubtaskGroup = context.getTaskInfo().getIndexOfThisSubtask();
        int currentNumberOfSubtasks = context.getTaskInfo().getNumberOfParallelSubtasks();
        format.open(
                new InitializationContext() {
                    @Override
                    public int getNumTasks() {
                        return currentNumberOfSubtasks;
                    }

                    @Override
                    public int getTaskNumber() {
                        return indexInSubtaskGroup;
                    }

                    @Override
                    public int getAttemptNumber() {
                        return context.getTaskInfo().getAttemptNumber();
                    }
                });
    }

    @Override
    public void setRuntimeContext(RuntimeContext context) {
        super.setRuntimeContext(context);
        if (format instanceof RichOutputFormat) {
            ((RichOutputFormat) format).setRuntimeContext(context);
        }
    }

    @Override
    public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
        if (format instanceof InputTypeConfigurable) {
            InputTypeConfigurable itc = (InputTypeConfigurable) format;
            itc.setInputType(type, executionConfig);
        }
    }

    @Override
    public void invoke(IN record) throws Exception {
        try {
            format.writeRecord(record);
        } catch (Exception ex) {
            cleanup();
            throw ex;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            format.close();
        } catch (Exception ex) {
            cleanup();
            throw ex;
        }
    }

    private void cleanup() {
        try {
            if (!cleanupCalled && format instanceof CleanupWhenUnsuccessful) {
                cleanupCalled = true;
                ((CleanupWhenUnsuccessful) format).tryCleanupOnError();
            }
        } catch (Throwable t) {
            LOG.error("Cleanup on error failed.", t);
        }
    }

    public OutputFormat<IN> getFormat() {
        return format;
    }
}
