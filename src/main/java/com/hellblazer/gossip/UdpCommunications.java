/** (C) Copyright 2010 Hal Hildebrand, All Rights Reserved
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *     
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 */
package com.hellblazer.gossip;

import static com.hellblazer.gossip.GossipMessages.DATA_POSITION;
import static com.hellblazer.gossip.GossipMessages.DIGEST_BYTE_SIZE;
import static com.hellblazer.gossip.GossipMessages.GOSSIP;
import static com.hellblazer.gossip.GossipMessages.MAX_SEG_SIZE;
import static com.hellblazer.gossip.GossipMessages.REPLY;
import static com.hellblazer.gossip.GossipMessages.RING;
import static com.hellblazer.gossip.GossipMessages.UPDATE;
import static java.lang.Math.min;
import static java.lang.String.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.utils.ByteBufferPool;
import com.hellblazer.utils.HexDump;

/**
 * A UDP message protocol implementation of the gossip communications
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public class UdpCommunications implements GossipCommunications {
    private class GossipHandler implements GossipMessages {
        private final InetSocketAddress gossipper;

        GossipHandler(InetSocketAddress target) {
            assert target.getPort() != 0 : "Invalid port";
            this.gossipper = target;
        }

        @Override
        public void close() {
            // no op
        }

        @Override
        public void gossip(List<Digest> digests) {
            sendDigests(digests, GOSSIP);
        }

        @Override
        public void reply(List<Digest> digests, List<Update> states) {
            sendDigests(digests, REPLY);
            update(states);
        }

        @Override
        public void update(List<Update> deltaState) {
            ByteBuffer buffer = bufferPool.allocate(MAX_SEG_SIZE);
            buffer.order(ByteOrder.BIG_ENDIAN);
            for (Update state : deltaState) {
                UdpCommunications.this.update(UPDATE, state, gossipper, buffer);
                buffer.clear();
            }
            bufferPool.free(buffer);
        }

        private void sendDigests(List<Digest> digests, byte messageType) {
            ByteBuffer buffer = bufferPool.allocate(MAX_SEG_SIZE);
            buffer.order(ByteOrder.BIG_ENDIAN);
            for (int i = 0; i < digests.size();) {
                byte count = (byte) min(MAX_DIGESTS, digests.size() - i);
                buffer.position(DATA_POSITION);
                buffer.put(messageType);
                buffer.put(count);
                int position;
                for (Digest digest : digests.subList(i, i + count)) {
                    digest.writeTo(buffer);
                    position = buffer.position();
                    Integer.toString(position);
                }
                send(buffer, gossipper);
                i += count;
                buffer.clear();
            }
            bufferPool.free(buffer);
        }

        @Override
        public SocketAddress getGossipper() {
            return gossipper;
        }

    }

    private static final int    DEFAULT_RECEIVE_BUFFER_MULTIPLIER = 4;
    private static final int    DEFAULT_SEND_BUFFER_MULTIPLIER    = 4;
    private static final Logger log                               = LoggerFactory.getLogger(UdpCommunications.class);
    private static final int    MAGIC_NUMBER                      = 0xCAFEBABE;
    private static final int    MAX_DIGESTS                       = (MAX_SEG_SIZE
                                                                     - DATA_POSITION - 1)
                                                                    / DIGEST_BYTE_SIZE;

    private static String prettyPrint(SocketAddress sender,
                                      SocketAddress target, byte[] bytes,
                                      int length) {
        final StringBuilder sb = new StringBuilder(length * 2);
        sb.append('\n');
        sb.append(new SimpleDateFormat().format(new Date()));
        sb.append(" sender: ");
        sb.append(sender);
        sb.append(" target: ");
        sb.append(target);
        sb.append(" length: ");
        sb.append(length);
        sb.append('\n');
        sb.append(toHex(bytes, 0, length));
        return sb.toString();
    }

    private static String toHex(byte[] data, int offset, int length) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        PrintStream stream = new PrintStream(baos);
        HexDump.hexdump(stream, data, offset, length);
        stream.close();
        return baos.toString();
    }

    private final ByteBufferPool  bufferPool = new ByteBufferPool("UDP Comms",
                                                                  100);
    private final ExecutorService dispatcher;
    private Gossip                gossip;
    private final AtomicBoolean   running    = new AtomicBoolean();

    private final DatagramSocket  socket;
    private InetSocketAddress     localAddress;

    public UdpCommunications(InetSocketAddress endpoint,
                             ExecutorService executor) {
        this(endpoint, executor, DEFAULT_RECEIVE_BUFFER_MULTIPLIER,
             DEFAULT_SEND_BUFFER_MULTIPLIER);
    }

    public UdpCommunications(InetSocketAddress endpoint,
                             ExecutorService executor,
                             int receiveBufferMultiplier,
                             int sendBufferMultiplier) {
        dispatcher = executor;
        try {
            socket = new DatagramSocket(endpoint.getPort(),
                                        endpoint.getAddress());
        } catch (SocketException e) {
            log.error(format("Unable to bind to: %s", endpoint));
            throw new IllegalStateException(format("Unable to bind to: %s",
                                                   endpoint), e);
        }
        localAddress = new InetSocketAddress(socket.getLocalAddress(),
                                             socket.getLocalPort());
        try {
            socket.setReceiveBufferSize(MAX_SEG_SIZE * receiveBufferMultiplier);
            socket.setSendBufferSize(MAX_SEG_SIZE * sendBufferMultiplier);
        } catch (SocketException e) {
            log.error(format("Unable to configure endpoint: %s", socket));
            throw new IllegalStateException(
                                            format("Unable to configure endpoint: %s",
                                                   socket), e);
        }
    }

    @Override
    public void connect(InetSocketAddress address, Endpoint endpoint,
                        Runnable connectAction) throws IOException {
        endpoint.setCommunications(new GossipHandler(address));
        connectAction.run();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return localAddress;
    }

    @Override
    public void setGossip(Gossip gossip) {
        this.gossip = gossip;
    }

    /**
     * Start the service
     */
    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            dispatcher.execute(serviceTask());
        }
    }

    /**
     * Stop the service
     */
    @Override
    public void terminate() {
        if (running.compareAndSet(true, false)) {
            if (log.isInfoEnabled()) {
                log.info(String.format("Terminating UDP Communications on %s",
                                       socket.getLocalSocketAddress()));
            }
            socket.close();
            log.info(bufferPool.toString());
        }
    }

    @Override
    public void update(Update state, InetSocketAddress left) {
        ByteBuffer buffer = bufferPool.allocate(MAX_SEG_SIZE);
        update(RING, state, left, buffer);
        bufferPool.free(buffer);
    }

    /**
     * @param msg
     * @return
     */
    private Digest[] extractDigests(SocketAddress sender, ByteBuffer msg) {
        int count = msg.get();
        final Digest[] digests = new Digest[count];
        for (int i = 0; i < count; i++) {
            Digest digest;
            try {
                digest = new Digest(msg);
            } catch (Throwable e) {
                if (log.isWarnEnabled()) {
                    log.warn(String.format("Cannot deserialize digest. Ignoring the digest: %s\n%s",
                                           i,
                                           prettyPrint(sender,
                                                       getLocalAddress(),
                                                       msg.array(), msg.limit())),
                             e);
                }
                continue;
            }
            digests[i] = digest;
        }
        return digests;
    }

    private void handleGossip(final InetSocketAddress gossiper, ByteBuffer msg) {
        gossip.gossip(extractDigests(gossiper, msg),
                      new GossipHandler(gossiper));
    }

    private void handleReply(final InetSocketAddress target, ByteBuffer msg) {
        gossip.reply(extractDigests(target, msg), new GossipHandler(target));
    }

    /**
     * @param msg
     */
    private void handleRing(InetSocketAddress gossiper, ByteBuffer msg) {
        final Update state;
        try {
            state = new Update(msg);
        } catch (Throwable e) {
            if (log.isWarnEnabled()) {
                log.warn("Cannot deserialize state. Ignoring the state.", e);
            }
            return;
        }
        if (log.isTraceEnabled()) {
            log.trace(format("Heartbeat state from %s is : %s", this, state));
        }
        gossip.ringUpdate(state, gossiper);
    }

    private void handleUpdate(InetSocketAddress gossiper, ByteBuffer msg) {
        final Update state;
        try {
            state = new Update(msg);
        } catch (Throwable e) {
            if (log.isWarnEnabled()) {
                log.warn("Cannot deserialize state. Ignoring the state.", e);
            }
            return;
        }
        if (log.isTraceEnabled()) {
            log.trace(format("Heartbeat state from %s is : %s", this, state));
        }
        gossip.update(state, gossiper);
    }

    /**
     * Process the inbound message
     * 
     * @param buffer
     *            - the message bytes
     */
    private void processInbound(InetSocketAddress sender, ByteBuffer buffer) {
        byte msgType = buffer.get();
        switch (msgType) {
            case GOSSIP: {
                handleGossip(sender, buffer);
                break;
            }
            case REPLY: {
                handleReply(sender, buffer);
                break;
            }
            case UPDATE: {
                handleUpdate(sender, buffer);
                break;
            }
            case RING: {
                handleRing(sender, buffer);
                break;
            }
            default: {
                if (log.isInfoEnabled()) {
                    log.info(format("invalid message type: %s from: %s",
                                    msgType, this));
                }
            }
        }
    }

    /**
     * Send the datagram across the net
     * 
     * @param buffer
     * @param target
     * @throws IOException
     */
    private void send(ByteBuffer buffer, SocketAddress target) {
        if (socket.isClosed()) {
            log.trace(String.format("Sending on a closed socket"));
            return;
        }
        int length = buffer.position();
        if (length == 6)
            System.out.println("Ruh Roh");
        buffer.putInt(0, MAGIC_NUMBER);
        try {
            byte[] bytes = buffer.array();
            DatagramPacket packet = new DatagramPacket(bytes, length, target);
            if (log.isTraceEnabled()) {
                log.trace(String.format("sending packet %s",
                                        prettyPrint(getLocalAddress(), target,
                                                    buffer.array(), length)));
            }
            socket.send(packet);
        } catch (SocketException e) {
            if (!"Socket is closed".equals(e.getMessage())
                && !"Bad file descriptor".equals(e.getMessage())) {
                if (log.isWarnEnabled()) {
                    log.warn("Error sending packet", e);
                }
            }
        } catch (IOException e) {
            if (log.isWarnEnabled()) {
                log.warn("Error sending packet", e);
            }
        }
    }

    /**
     * Service the next inbound datagram
     * 
     * @param buffer
     *            - the buffer to use to receive the datagram
     * @throws IOException
     */
    private void service() throws IOException {
        final ByteBuffer buffer = bufferPool.allocate(MAX_SEG_SIZE);
        buffer.order(ByteOrder.BIG_ENDIAN);
        final DatagramPacket packet = new DatagramPacket(buffer.array(),
                                                         buffer.array().length);
        socket.receive(packet);
        buffer.limit(packet.getLength());
        dispatcher.execute(new Runnable() {
            @Override
            public void run() {
                if (log.isTraceEnabled()) {
                    log.trace(String.format("Received packet %s",
                                            prettyPrint(packet.getSocketAddress(),
                                                        getLocalAddress(),
                                                        buffer.array(),
                                                        packet.getLength())));
                } else if (log.isTraceEnabled()) {
                    log.trace("Received packet from: "
                              + packet.getSocketAddress());
                }
                int magic = buffer.getInt();
                if (MAGIC_NUMBER == magic) {
                    try {
                        processInbound((InetSocketAddress) packet.getSocketAddress(),
                                       buffer);
                    } catch (Throwable e) {
                        if (log.isWarnEnabled()) {
                            log.warn(format("Error processing inbound message on: %s",
                                            getLocalAddress()), e);
                        }
                    }
                } else {
                    if (log.isWarnEnabled()) {
                        log.warn(format("Msg with invalid MAGIC header [%s] discarded %s",
                                        magic,
                                        prettyPrint(packet.getSocketAddress(),
                                                    getLocalAddress(),
                                                    buffer.array(),
                                                    packet.getLength())));
                    }
                }
                bufferPool.free(buffer);
            }
        });
    }

    /**
     * The service loop.
     * 
     * @return the Runnable action implementing the service loop.
     */
    private Runnable serviceTask() {
        return new Runnable() {
            @Override
            public void run() {
                if (log.isInfoEnabled()) {
                    log.info(String.format("UDP Gossip communications started on %s",
                                           socket.getLocalSocketAddress()));
                }
                while (running.get()) {
                    try {
                        service();
                    } catch (SocketException e) {
                        if ("Socket closed".equals(e.getMessage())) {
                            if (log.isTraceEnabled()) {
                                log.trace("Socket closed, shutting down");
                                terminate();
                                return;
                            }
                        }
                    } catch (Throwable e) {
                        if (log.isWarnEnabled()) {
                            log.warn("Exception processing inbound message", e);
                        }
                    }
                }
            }
        };
    }

    /**
     * @param state
     * @param address
     * @param buffer
     */
    private void update(byte msg, Update state, InetSocketAddress address,
                        ByteBuffer buffer) {
        buffer.clear();
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.position(DATA_POSITION);
        buffer.put(msg);
        state.writeTo(buffer);
        send(buffer, address);
    }
}
