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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.TestingConnectionManager;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferListener;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyMessage.AddCredit;
import org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import org.apache.flink.runtime.io.network.netty.NettyMessage.CloseRequest;
import org.apache.flink.runtime.io.network.netty.NettyMessage.ErrorResponse;
import org.apache.flink.runtime.io.network.netty.NettyMessage.PartitionRequest;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;

import static org.apache.flink.runtime.io.network.netty.PartitionRequestQueueTest.blockChannel;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createRemoteInputChannel;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createSingleInputGate;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CreditBasedPartitionRequestClientHandlerTest {

    /**
     * Tests a fix for FLINK-1627.
     *
     * <p>FLINK-1627 discovered a race condition, which could lead to an infinite loop when a
     * receiver was cancelled during a certain time of decoding a message. The test reproduces the
     * input, which lead to the infinite loop: when the handler gets a reference to the buffer
     * provider of the receiving input channel, but the respective input channel is released (and
     * the corresponding buffer provider destroyed), the handler did not notice this.
     *
     * @see <a href="https://issues.apache.org/jira/browse/FLINK-1627">FLINK-1627</a>
     */
    @Test
    @Timeout(60)
    @SuppressWarnings("unchecked")
    public void testReleaseInputChannelDuringDecode() throws Exception {
        // Mocks an input channel in a state as it was released during a decode.
        final BufferProvider bufferProvider = mock(BufferProvider.class);
        when(bufferProvider.requestBuffer()).thenReturn(null);
        when(bufferProvider.isDestroyed()).thenReturn(true);
        when(bufferProvider.addBufferListener(any(BufferListener.class))).thenReturn(false);

        final RemoteInputChannel inputChannel = mock(RemoteInputChannel.class);
        when(inputChannel.getInputChannelId()).thenReturn(new InputChannelID());
        when(inputChannel.getBufferProvider()).thenReturn(bufferProvider);

        final CreditBasedPartitionRequestClientHandler client =
                new CreditBasedPartitionRequestClientHandler();
        client.addInputChannel(inputChannel);

        final BufferResponse receivedBuffer =
                createBufferResponse(
                        TestBufferFactory.createBuffer(TestBufferFactory.BUFFER_SIZE),
                        0,
                        inputChannel.getInputChannelId(),
                        2,
                        new NetworkBufferAllocator(client));

        client.channelRead(mock(ChannelHandlerContext.class), receivedBuffer);
    }

    /**
     * Tests a fix for FLINK-1761.
     *
     * <p>FLINK-1761 discovered an IndexOutOfBoundsException, when receiving buffers of size 0.
     */
    @Test
    public void testReceiveEmptyBuffer() throws Exception {
        // Minimal mock of a remote input channel
        final BufferProvider bufferProvider = mock(BufferProvider.class);
        when(bufferProvider.requestBuffer()).thenReturn(TestBufferFactory.createBuffer(0));

        final RemoteInputChannel inputChannel = mock(RemoteInputChannel.class);
        when(inputChannel.getInputChannelId()).thenReturn(new InputChannelID());
        when(inputChannel.getBufferProvider()).thenReturn(bufferProvider);

        // An empty buffer of size 0
        final Buffer emptyBuffer = TestBufferFactory.createBuffer(0);

        final CreditBasedPartitionRequestClientHandler client =
                new CreditBasedPartitionRequestClientHandler();
        client.addInputChannel(inputChannel);

        final int backlog = 2;
        final BufferResponse receivedBuffer =
                createBufferResponse(
                        emptyBuffer,
                        0,
                        inputChannel.getInputChannelId(),
                        backlog,
                        new NetworkBufferAllocator(client));

        // Read the empty buffer
        client.channelRead(mock(ChannelHandlerContext.class), receivedBuffer);

        // This should not throw an exception
        verify(inputChannel, never()).onError(any(Throwable.class));
        verify(inputChannel, times(1)).onEmptyBuffer(0, backlog);
    }

    /**
     * Verifies that {@link RemoteInputChannel#onBuffer(Buffer, int, int)} is called when a {@link
     * BufferResponse} is received.
     */
    @Test
    public void testReceiveBuffer() throws Exception {
        final NetworkBufferPool networkBufferPool = new NetworkBufferPool(10, 32);
        final SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
        final RemoteInputChannel inputChannel =
                InputChannelBuilder.newBuilder().buildRemoteChannel(inputGate);
        try {
            inputGate.setInputChannels(inputChannel);
            final BufferPool bufferPool = networkBufferPool.createBufferPool(8, 8);
            inputGate.setBufferPool(bufferPool);
            inputGate.setupChannels();

            final CreditBasedPartitionRequestClientHandler handler =
                    new CreditBasedPartitionRequestClientHandler();
            handler.addInputChannel(inputChannel);

            final int backlog = 2;
            final BufferResponse bufferResponse =
                    createBufferResponse(
                            TestBufferFactory.createBuffer(32),
                            0,
                            inputChannel.getInputChannelId(),
                            backlog,
                            new NetworkBufferAllocator(handler));
            handler.channelRead(mock(ChannelHandlerContext.class), bufferResponse);

            assertEquals(1, inputChannel.getNumberOfQueuedBuffers());
            assertEquals(2, inputChannel.getSenderBacklog());
        } finally {
            releaseResource(inputGate, networkBufferPool);
        }
    }

    /**
     * Verifies that {@link BufferResponse} of compressed {@link Buffer} can be handled correctly.
     */
    @Test
    public void testReceiveCompressedBuffer() throws Exception {
        int bufferSize = 1024;
        String compressionCodec = "LZ4";
        BufferCompressor compressor = new BufferCompressor(bufferSize, compressionCodec);
        BufferDecompressor decompressor = new BufferDecompressor(bufferSize, compressionCodec);
        NetworkBufferPool networkBufferPool = new NetworkBufferPool(10, bufferSize);
        SingleInputGate inputGate =
                new SingleInputGateBuilder()
                        .setBufferDecompressor(decompressor)
                        .setSegmentProvider(networkBufferPool)
                        .build();
        RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate, null);
        inputGate.setInputChannels(inputChannel);

        try {
            BufferPool bufferPool = networkBufferPool.createBufferPool(8, 8);
            inputGate.setBufferPool(bufferPool);
            inputGate.setupChannels();

            CreditBasedPartitionRequestClientHandler handler =
                    new CreditBasedPartitionRequestClientHandler();
            handler.addInputChannel(inputChannel);

            Buffer buffer =
                    compressor.compressToOriginalBuffer(TestBufferFactory.createBuffer(bufferSize));
            BufferResponse bufferResponse =
                    createBufferResponse(
                            buffer,
                            0,
                            inputChannel.getInputChannelId(),
                            2,
                            new NetworkBufferAllocator(handler));
            assertTrue(bufferResponse.isCompressed);
            handler.channelRead(null, bufferResponse);

            Buffer receivedBuffer = inputChannel.getNextReceivedBuffer();
            assertNotNull(receivedBuffer);
            assertTrue(receivedBuffer.isCompressed());
            receivedBuffer.recycleBuffer();
        } finally {
            releaseResource(inputGate, networkBufferPool);
        }
    }

    /**
     * Verifies that {@link RemoteInputChannel#onError(Throwable)} is called when a {@link
     * BufferResponse} is received but no available buffer in input channel.
     */
    @Test
    public void testThrowExceptionForNoAvailableBuffer() throws Exception {
        final SingleInputGate inputGate = createSingleInputGate(1);
        final RemoteInputChannel inputChannel =
                spy(InputChannelBuilder.newBuilder().buildRemoteChannel(inputGate));

        final CreditBasedPartitionRequestClientHandler handler =
                new CreditBasedPartitionRequestClientHandler();
        handler.addInputChannel(inputChannel);

        assertEquals(
                0,
                inputChannel.getNumberOfAvailableBuffers(),
                "There should be no buffers available in the channel.");

        final BufferResponse bufferResponse =
                createBufferResponse(
                        TestBufferFactory.createBuffer(TestBufferFactory.BUFFER_SIZE),
                        0,
                        inputChannel.getInputChannelId(),
                        2,
                        new NetworkBufferAllocator(handler));
        assertNull(bufferResponse.getBuffer());

        handler.channelRead(mock(ChannelHandlerContext.class), bufferResponse);
        verify(inputChannel, times(1)).onError(any(IllegalStateException.class));
    }

    /**
     * Verifies that {@link RemoteInputChannel#onFailedPartitionRequest()} is called when a {@link
     * PartitionNotFoundException} is received.
     */
    @Test
    public void testReceivePartitionNotFoundException() throws Exception {
        // Minimal mock of a remote input channel
        final BufferProvider bufferProvider = mock(BufferProvider.class);
        when(bufferProvider.requestBuffer()).thenReturn(TestBufferFactory.createBuffer(0));

        final RemoteInputChannel inputChannel = mock(RemoteInputChannel.class);
        when(inputChannel.getInputChannelId()).thenReturn(new InputChannelID());
        when(inputChannel.getBufferProvider()).thenReturn(bufferProvider);

        final ErrorResponse partitionNotFound =
                new ErrorResponse(
                        new PartitionNotFoundException(new ResultPartitionID()),
                        inputChannel.getInputChannelId());

        final CreditBasedPartitionRequestClientHandler client =
                new CreditBasedPartitionRequestClientHandler();
        client.addInputChannel(inputChannel);

        // Mock channel context
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(mock(Channel.class));

        client.channelActive(ctx);

        client.channelRead(ctx, partitionNotFound);

        verify(inputChannel, times(1)).onFailedPartitionRequest();
    }

    @Test
    public void testCancelBeforeActive() throws Exception {

        final RemoteInputChannel inputChannel = mock(RemoteInputChannel.class);
        when(inputChannel.getInputChannelId()).thenReturn(new InputChannelID());

        final CreditBasedPartitionRequestClientHandler client =
                new CreditBasedPartitionRequestClientHandler();
        client.addInputChannel(inputChannel);

        // Don't throw NPE
        client.cancelRequestFor(null);

        // Don't throw NPE, because channel is not active yet
        client.cancelRequestFor(inputChannel.getInputChannelId());
    }

    /**
     * Verifies that {@link RemoteInputChannel} is enqueued in the pipeline for notifying credits,
     * and verifies the behaviour of credit notification by triggering channel's writability
     * changed.
     */
    @Test
    public void testNotifyCreditAvailable() throws Exception {
        final CreditBasedPartitionRequestClientHandler handler =
                new CreditBasedPartitionRequestClientHandler();
        final NetworkBufferAllocator allocator = new NetworkBufferAllocator(handler);
        final EmbeddedChannel channel = new EmbeddedChannel(handler);
        final PartitionRequestClient client =
                new NettyPartitionRequestClient(
                        channel,
                        handler,
                        mock(ConnectionID.class),
                        mock(PartitionRequestClientFactory.class));

        final NetworkBufferPool networkBufferPool = new NetworkBufferPool(10, 32);
        final SingleInputGate inputGate = createSingleInputGate(2, networkBufferPool);
        final RemoteInputChannel[] inputChannels = new RemoteInputChannel[2];
        inputChannels[0] = createRemoteInputChannel(inputGate, client);
        inputChannels[1] = createRemoteInputChannel(inputGate, client);
        try {
            inputGate.setInputChannels(inputChannels);
            final BufferPool bufferPool = networkBufferPool.createBufferPool(6, 6);
            inputGate.setBufferPool(bufferPool);
            inputGate.setupChannels();

            inputChannels[0].requestSubpartition(0);
            inputChannels[1].requestSubpartition(0);

            // The two input channels should send partition requests
            assertTrue(channel.isWritable());
            Object readFromOutbound = channel.readOutbound();
            assertThat(readFromOutbound, instanceOf(PartitionRequest.class));
            assertEquals(
                    inputChannels[0].getInputChannelId(),
                    ((PartitionRequest) readFromOutbound).receiverId);
            assertEquals(2, ((PartitionRequest) readFromOutbound).credit);

            readFromOutbound = channel.readOutbound();
            assertThat(readFromOutbound, instanceOf(PartitionRequest.class));
            assertEquals(
                    inputChannels[1].getInputChannelId(),
                    ((PartitionRequest) readFromOutbound).receiverId);
            assertEquals(2, ((PartitionRequest) readFromOutbound).credit);

            // The buffer response will take one available buffer from input channel, and it will
            // trigger
            // requesting (backlog + numExclusiveBuffers - numAvailableBuffers) floating buffers
            final BufferResponse bufferResponse1 =
                    createBufferResponse(
                            TestBufferFactory.createBuffer(32),
                            0,
                            inputChannels[0].getInputChannelId(),
                            1,
                            allocator);
            final BufferResponse bufferResponse2 =
                    createBufferResponse(
                            TestBufferFactory.createBuffer(32),
                            0,
                            inputChannels[1].getInputChannelId(),
                            1,
                            allocator);
            handler.channelRead(mock(ChannelHandlerContext.class), bufferResponse1);
            handler.channelRead(mock(ChannelHandlerContext.class), bufferResponse2);

            assertEquals(2, inputChannels[0].getUnannouncedCredit());
            assertEquals(2, inputChannels[1].getUnannouncedCredit());

            channel.runPendingTasks();

            // The two input channels should notify credits availability via the writable channel
            readFromOutbound = channel.readOutbound();
            assertThat(readFromOutbound, instanceOf(AddCredit.class));
            assertEquals(
                    inputChannels[0].getInputChannelId(),
                    ((AddCredit) readFromOutbound).receiverId);
            assertEquals(2, ((AddCredit) readFromOutbound).credit);

            readFromOutbound = channel.readOutbound();
            assertThat(readFromOutbound, instanceOf(AddCredit.class));
            assertEquals(
                    inputChannels[1].getInputChannelId(),
                    ((AddCredit) readFromOutbound).receiverId);
            assertEquals(2, ((AddCredit) readFromOutbound).credit);
            assertNull(channel.readOutbound());

            ByteBuf channelBlockingBuffer = blockChannel(channel);

            // Trigger notify credits availability via buffer response on the condition of an
            // un-writable channel
            final BufferResponse bufferResponse3 =
                    createBufferResponse(
                            TestBufferFactory.createBuffer(32),
                            1,
                            inputChannels[0].getInputChannelId(),
                            1,
                            allocator);
            handler.channelRead(mock(ChannelHandlerContext.class), bufferResponse3);

            assertEquals(1, inputChannels[0].getUnannouncedCredit());
            assertEquals(0, inputChannels[1].getUnannouncedCredit());

            channel.runPendingTasks();

            // The input channel will not notify credits via un-writable channel
            assertFalse(channel.isWritable());
            assertNull(channel.readOutbound());

            // Flush the buffer to make the channel writable again
            channel.flush();
            assertSame(channelBlockingBuffer, channel.readOutbound());

            // The input channel should notify credits via channel's writability changed event
            assertTrue(channel.isWritable());
            readFromOutbound = channel.readOutbound();
            assertThat(readFromOutbound, instanceOf(AddCredit.class));
            assertEquals(1, ((AddCredit) readFromOutbound).credit);
            assertEquals(0, inputChannels[0].getUnannouncedCredit());
            assertEquals(0, inputChannels[1].getUnannouncedCredit());

            // no more messages
            assertNull(channel.readOutbound());
        } finally {
            releaseResource(inputGate, networkBufferPool);
            channel.close();
        }
    }

    /**
     * Verifies that {@link RemoteInputChannel} is enqueued in the pipeline, but {@link AddCredit}
     * message is not sent actually when this input channel is released.
     */
    @Test
    public void testNotifyCreditAvailableAfterReleased() throws Exception {
        final CreditBasedPartitionRequestClientHandler handler =
                new CreditBasedPartitionRequestClientHandler();
        final EmbeddedChannel channel = new EmbeddedChannel(handler);
        final PartitionRequestClient client =
                new NettyPartitionRequestClient(
                        channel,
                        handler,
                        mock(ConnectionID.class),
                        mock(PartitionRequestClientFactory.class));

        final NetworkBufferPool networkBufferPool = new NetworkBufferPool(10, 32);
        final SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
        final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate, client);
        try {
            inputGate.setInputChannels(inputChannel);
            final BufferPool bufferPool = networkBufferPool.createBufferPool(6, 6);
            inputGate.setBufferPool(bufferPool);
            inputGate.setupChannels();

            inputChannel.requestSubpartition(0);

            // This should send the partition request
            Object readFromOutbound = channel.readOutbound();
            assertThat(readFromOutbound, instanceOf(PartitionRequest.class));
            assertEquals(2, ((PartitionRequest) readFromOutbound).credit);

            // Trigger request floating buffers via buffer response to notify credits available
            final BufferResponse bufferResponse =
                    createBufferResponse(
                            TestBufferFactory.createBuffer(32),
                            0,
                            inputChannel.getInputChannelId(),
                            1,
                            new NetworkBufferAllocator(handler));
            handler.channelRead(mock(ChannelHandlerContext.class), bufferResponse);

            assertEquals(2, inputChannel.getUnannouncedCredit());

            // Release the input channel
            inputGate.close();

            // it should send a close request after releasing the input channel,
            // but will not notify credits for a released input channel.
            readFromOutbound = channel.readOutbound();
            assertThat(readFromOutbound, instanceOf(CloseRequest.class));

            channel.runPendingTasks();

            assertNull(channel.readOutbound());
        } finally {
            releaseResource(inputGate, networkBufferPool);
            channel.close();
        }
    }

    @Test
    public void testReadBufferResponseBeforeReleasingChannel() throws Exception {
        testReadBufferResponseWithReleasingOrRemovingChannel(false, true);
    }

    @Test
    public void testReadBufferResponseBeforeRemovingChannel() throws Exception {
        testReadBufferResponseWithReleasingOrRemovingChannel(true, true);
    }

    @Test
    public void testReadBufferResponseAfterReleasingChannel() throws Exception {
        testReadBufferResponseWithReleasingOrRemovingChannel(false, false);
    }

    @Test
    public void testReadBufferResponseAfterRemovingChannel() throws Exception {
        testReadBufferResponseWithReleasingOrRemovingChannel(true, false);
    }

    @Test
    public void testDoNotFailHandlerOnSingleChannelFailure() throws Exception {
        // Setup
        final int bufferSize = 1024;
        final String expectedMessage = "test exception on buffer";
        final NetworkBufferPool networkBufferPool = new NetworkBufferPool(10, bufferSize);
        final SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
        final RemoteInputChannel inputChannel =
                new TestRemoteInputChannelForError(inputGate, expectedMessage);
        final CreditBasedPartitionRequestClientHandler handler =
                new CreditBasedPartitionRequestClientHandler();

        try {
            inputGate.setInputChannels(inputChannel);
            inputGate.setupChannels();
            inputGate.requestPartitions();
            handler.addInputChannel(inputChannel);

            final BufferResponse bufferResponse =
                    createBufferResponse(
                            TestBufferFactory.createBuffer(bufferSize),
                            0,
                            inputChannel.getInputChannelId(),
                            1,
                            new NetworkBufferAllocator(handler));

            // It will trigger an expected exception from TestRemoteInputChannelForError#onBuffer
            handler.channelRead(null, bufferResponse);

            // The handler should not be tagged as error for above excepted exception
            handler.checkError();

            try {
                // The input channel should be tagged as error and the respective exception is
                // thrown via #getNext
                inputGate.getNext();
            } catch (IOException ignored) {
                assertEquals(expectedMessage, ignored.getMessage());
            }
        } finally {
            // Cleanup
            releaseResource(inputGate, networkBufferPool);
        }
    }

    private void testReadBufferResponseWithReleasingOrRemovingChannel(
            boolean isRemoved, boolean readBeforeReleasingOrRemoving) throws Exception {

        int bufferSize = 1024;

        NetworkBufferPool networkBufferPool = new NetworkBufferPool(10, bufferSize);
        SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
        RemoteInputChannel inputChannel = new InputChannelBuilder().buildRemoteChannel(inputGate);
        inputGate.setInputChannels(inputChannel);
        inputGate.setupChannels();

        CreditBasedPartitionRequestClientHandler handler =
                new CreditBasedPartitionRequestClientHandler();
        EmbeddedChannel embeddedChannel = new EmbeddedChannel(handler);
        handler.addInputChannel(inputChannel);

        try {
            if (!readBeforeReleasingOrRemoving) {
                // Release the channel.
                inputGate.close();
                if (isRemoved) {
                    handler.removeInputChannel(inputChannel);
                }
            }

            BufferResponse bufferResponse =
                    createBufferResponse(
                            TestBufferFactory.createBuffer(bufferSize),
                            0,
                            inputChannel.getInputChannelId(),
                            1,
                            new NetworkBufferAllocator(handler));

            if (readBeforeReleasingOrRemoving) {
                // Release the channel.
                inputGate.close();
                if (isRemoved) {
                    handler.removeInputChannel(inputChannel);
                }
            }

            handler.channelRead(null, bufferResponse);

            assertEquals(0, inputChannel.getNumberOfQueuedBuffers());
            if (!readBeforeReleasingOrRemoving) {
                assertNull(bufferResponse.getBuffer());
            } else {
                assertNotNull(bufferResponse.getBuffer());
                assertTrue(bufferResponse.getBuffer().isRecycled());
            }

            embeddedChannel.runScheduledPendingTasks();
            NettyMessage.CancelPartitionRequest cancelPartitionRequest =
                    embeddedChannel.readOutbound();
            assertNotNull(cancelPartitionRequest);
            assertEquals(inputChannel.getInputChannelId(), cancelPartitionRequest.receiverId);
        } finally {
            releaseResource(inputGate, networkBufferPool);
            embeddedChannel.close();
        }
    }

    private static void releaseResource(
            SingleInputGate inputGate, NetworkBufferPool networkBufferPool) throws IOException {
        // Release all the buffer resources
        inputGate.close();

        networkBufferPool.destroyAllBufferPools();
        networkBufferPool.destroy();
    }

    /** Returns a deserialized buffer message as it would be received during runtime. */
    private static BufferResponse createBufferResponse(
            Buffer buffer,
            int sequenceNumber,
            InputChannelID receivingChannelId,
            int backlog,
            NetworkBufferAllocator allocator)
            throws IOException {
        // Mock buffer to serialize
        BufferResponse resp =
                new BufferResponse(buffer, sequenceNumber, receivingChannelId, backlog);

        ByteBuf serialized = resp.write(UnpooledByteBufAllocator.DEFAULT);

        // Skip general header bytes
        serialized.readBytes(NettyMessage.FRAME_HEADER_LENGTH);

        // Deserialize the bytes to construct the BufferResponse.
        return BufferResponse.readFrom(serialized, allocator);
    }

    /**
     * The test remote input channel to throw expected exception while calling {@link
     * RemoteInputChannel#onBuffer(Buffer, int, int)}.
     */
    private static class TestRemoteInputChannelForError extends RemoteInputChannel {
        private final String expectedMessage;

        TestRemoteInputChannelForError(SingleInputGate inputGate, String expectedMessage) {
            super(
                    inputGate,
                    0,
                    new ResultPartitionID(),
                    InputChannelBuilder.STUB_CONNECTION_ID,
                    new TestingConnectionManager(),
                    0,
                    100,
                    2,
                    new SimpleCounter(),
                    new SimpleCounter(),
                    ChannelStateWriter.NO_OP);
            this.expectedMessage = expectedMessage;
        }

        @Override
        public void onBuffer(Buffer buffer, int sequenceNumber, int backlog) throws IOException {
            buffer.recycleBuffer();
            throw new IOException(expectedMessage);
        }
    }
}
