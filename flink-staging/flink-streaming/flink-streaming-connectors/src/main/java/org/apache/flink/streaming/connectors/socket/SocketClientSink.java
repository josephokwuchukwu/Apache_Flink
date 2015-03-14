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

package org.apache.flink.streaming.connectors.socket;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.function.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.util.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SocketClientSink<IN> extends RichSinkFunction<IN> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(SocketClientSink.class);

    private final String hostName;
    private final int port;
    private final SerializationSchema<IN, byte[]> scheme;
    private transient Socket client;
    private transient DataOutputStream dataOutputStream;

    public SocketClientSink(String hostName, int port, SerializationSchema<IN, byte[]> schema) {
        this.hostName = hostName;
        this.port = port;
        this.scheme = schema;
    }

    /**
     * Initializes the connection to Socket.
     */
    public void initialize() {
        OutputStream outputStream;
        try {
            client = new Socket(hostName, port);
            outputStream = client.getOutputStream();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        dataOutputStream = new DataOutputStream(outputStream);
    }

    /**
     * Called when new data arrives to the sink, and forwards it to Socket.
     *
     * @param value
     *            The incoming data
     */
    @Override
    public void invoke(IN value) {
        byte[] msg = scheme.serialize(value);
        try {
            dataOutputStream.write(msg);
        } catch (IOException e) {
            if(LOG.isErrorEnabled()){
                LOG.error("Cannot send message to socket server at {}:{}", hostName, port);
            }
        }
    }

    /**
     * Closes the connection.
     */
    private void closeConnection(){
        try {
            client.close();
        } catch (IOException e) {
            throw new RuntimeException("Error while closing connection with socket server at "
                    + hostName + ":" + port, e);
        }
    }

    @Override
    public void open(Configuration parameters) {
        initialize();
    }

    @Override
    public void close() {
        closeConnection();
    }

    @Override
    public void cancel() {
        close();
    }

}
