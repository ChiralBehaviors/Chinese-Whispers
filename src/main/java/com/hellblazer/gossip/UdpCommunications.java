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

import static com.hellblazer.gossip.GossipMessages.BYTE_SIZE;
import static com.hellblazer.gossip.GossipMessages.DATA_POSITION;
import static com.hellblazer.gossip.GossipMessages.DIGEST_BYTE_SIZE;
import static com.hellblazer.gossip.GossipMessages.GOSSIP;
import static com.hellblazer.gossip.GossipMessages.MAGIC;
import static com.hellblazer.gossip.GossipMessages.MAGIC_BYTE_SIZE;
import static com.hellblazer.gossip.GossipMessages.MAX_SEG_SIZE;
import static com.hellblazer.gossip.GossipMessages.MESSAGE_HEADER_BYTE_SIZE;
import static com.hellblazer.gossip.GossipMessages.REPLY;
import static com.hellblazer.gossip.GossipMessages.RING;
import static com.hellblazer.gossip.GossipMessages.UPDATE;
import static com.hellblazer.gossip.GossipMessages.UPDATE_HEADER_BYTE_SIZE;
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
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.crypto.Mac;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.SecretKeySpec;

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
            gossipper = target;
        }

        @Override
        public InetSocketAddress getGossipper() {
            return gossipper;
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
                if (state.node.equals(gossipper)) {
                    if (log.isTraceEnabled()) {
                        log.trace(String.format("Not sending % to the node that owns it",
                                                state));
                    }
                } else {
                    UdpCommunications.this.update(UPDATE, state, gossipper,
                                                  buffer);
                    buffer.clear();
                }
            }
            bufferPool.free(buffer);
        }

        private void sendDigests(List<Digest> digests, byte messageType) {
            ByteBuffer buffer = bufferPool.allocate(MAX_SEG_SIZE);
            buffer.order(ByteOrder.BIG_ENDIAN);
            for (int i = 0; i < digests.size();) {
                byte count = (byte) min(maxDigests, digests.size() - i);
                buffer.position(DATA_POSITION);
                buffer.put(count);
                int position;
                for (Digest digest : digests.subList(i, i + count)) {
                    digest.writeTo(buffer);
                    position = buffer.position();
                    Integer.toString(position);
                }
                send(messageType, buffer, gossipper);
                i += count;
                buffer.clear();
            }
            bufferPool.free(buffer);
        }

    }

    public static final int    DEFAULT_RECEIVE_BUFFER_MULTIPLIER = 4;
    public static final int    DEFAULT_SEND_BUFFER_MULTIPLIER    = 4;

    // Default MAC key used strictly for message integrity
    private static byte[]      DEFAULT_KEY_DATA                  = {
            (byte) 0x23, (byte) 0x45, (byte) 0x83, (byte) 0xad, (byte) 0x23,
            (byte) 0x46, (byte) 0x83, (byte) 0xad, (byte) 0x23, (byte) 0x45,
            (byte) 0x83, (byte) 0xad, (byte) 0x23, (byte) 0x45, (byte) 0x83,
            (byte) 0xad                                         };
    // Default MAC used strictly for message integrity
    private static String      DEFAULT_MAC_TYPE                  = "HmacMD5";
    public static final Logger log                               = LoggerFactory.getLogger(UdpCommunications.class);

    public static DatagramSocket connect(InetSocketAddress endpoint)
                                                                    throws SocketException {
        try {
            return new DatagramSocket(endpoint);
        } catch (SocketException e) {
            log.error(format("Unable to bind to: %s", endpoint));
            throw e;
        }
    }

    /**
     * @return a default mac, with a fixed key. Used for validation only, no
     *         authentication
     */
    public static Mac defaultMac() {
        Mac mac;
        try {
            mac = Mac.getInstance(DEFAULT_MAC_TYPE);
            mac.init(new SecretKeySpec(DEFAULT_KEY_DATA, DEFAULT_MAC_TYPE));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(
                                            String.format("Unable to create default mac %s",
                                                          DEFAULT_MAC_TYPE));
        } catch (InvalidKeyException e) {
            throw new IllegalStateException(
                                            String.format("Invalid default key %s for default mac %s",
                                                          Arrays.toString(DEFAULT_KEY_DATA),
                                                          DEFAULT_MAC_TYPE));
        }
        return mac;
    }

    public static String prettyPrint(SocketAddress sender,
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

    public static String toHex(byte[] data, int offset, int length) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        PrintStream stream = new PrintStream(baos);
        HexDump.hexdump(stream, data, offset, length);
        stream.close();
        return baos.toString();
    }

    private final ByteBufferPool    bufferPool = new ByteBufferPool(
                                                                    "UDP Comms",
                                                                    100);
    private final ExecutorService   dispatcher;
    private Gossip                  gossip;
    private final Mac               hmac;
    private final InetSocketAddress localAddress;
    private final int               maxDigests;
    private final AtomicBoolean     running    = new AtomicBoolean();
    private final DatagramSocket    socket;

    public UdpCommunications(DatagramSocket socket, ExecutorService executor,
                             int receiveBufferMultiplier,
                             int sendBufferMultiplier, Mac mac)
                                                               throws SocketException {
        if (socket.getLocalAddress().isAnyLocalAddress()) {
            throw new IllegalArgumentException(
                                               String.format("UDP Communications do not work with a socket bound to %s address",
                                                             socket.getLocalAddress()));
        }
        hmac = mac;
        dispatcher = executor;
        this.socket = socket;
        InetSocketAddress bound = new InetSocketAddress(
                                                        socket.getLocalAddress(),
                                                        socket.getLocalPort());
        localAddress = bound;
        try {
            socket.setReceiveBufferSize(MAX_SEG_SIZE * receiveBufferMultiplier);
            socket.setSendBufferSize(MAX_SEG_SIZE * sendBufferMultiplier);
        } catch (SocketException e) {
            log.error(format("Unable to configure endpoint: %s", socket));
            throw e;
        }
        int payloadByteSize = MAX_SEG_SIZE - MESSAGE_HEADER_BYTE_SIZE
                              - mac.getMacLength();
        maxDigests = (payloadByteSize - BYTE_SIZE) // 1 byte for #digests
                     / DIGEST_BYTE_SIZE;
    }

    public UdpCommunications(InetSocketAddress endpoint,
                             ExecutorService executor) throws SocketException {
        this(endpoint, executor, DEFAULT_RECEIVE_BUFFER_MULTIPLIER,
             DEFAULT_SEND_BUFFER_MULTIPLIER, defaultMac());
    }

    public UdpCommunications(InetSocketAddress endpoint,
                             ExecutorService executor,
                             int receiveBufferMultiplier,
                             int sendBufferMultiplier, Mac mac)
                                                               throws SocketException {
        this(connect(endpoint), executor, receiveBufferMultiplier,
             sendBufferMultiplier, mac);
    }

    public UdpCommunications(InetSocketAddress endpoint,
                             ExecutorService executor, Mac mac)
                                                               throws SocketException {
        this(endpoint, executor, DEFAULT_RECEIVE_BUFFER_MULTIPLIER,
             DEFAULT_SEND_BUFFER_MULTIPLIER, mac);
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return localAddress;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.hellblazer.gossip.GossipCommunications#getMaxStateSize()
     */
    @Override
    public int getMaxStateSize() {
        return MAX_SEG_SIZE - hmac.getMacLength() - UPDATE_HEADER_BYTE_SIZE;
    }

    @Override
    public GossipHandler handlerFor(InetSocketAddress address) {
        return new GossipHandler(address);
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
            Executors.newSingleThreadExecutor().execute(serviceTask());
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
    public String toString() {
        return String.format("UdpComms[%s]", getLocalAddress());
    }

    @Override
    public void update(Update state, InetSocketAddress left) {
        ByteBuffer buffer = bufferPool.allocate(MAX_SEG_SIZE);
        update(RING, state, left, buffer);
        bufferPool.free(buffer);
    }

    private synchronized void addMac(byte[] data, int offset, int length)
                                                                         throws ShortBufferException {
        hmac.reset();
        hmac.update(data, offset, length);
        hmac.doFinal(data, offset + length);
    }

    private synchronized boolean checkMac(byte[] data, int start, int length) {
        hmac.reset();
        hmac.update(data, start, length);
        byte[] checkMAC = hmac.doFinal();
        int len = checkMAC.length;
        assert len == hmac.getMacLength();

        for (int i = 0; i < len; i++) {
            if (checkMAC[i] != data[start + length + i]) {
                return false;
            }
        }
        return true;
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
        Digest[] digests = extractDigests(gossiper, msg);
        if (log.isTraceEnabled()) {
            log.trace(String.format("Gossip from %s is %s", gossiper,
                                    Arrays.toString(digests)));
        }
        gossip.gossip(digests, new GossipHandler(gossiper));
    }

    private void handleReply(final InetSocketAddress gossiper, ByteBuffer msg) {
        Digest[] digests = extractDigests(gossiper, msg);
        if (log.isTraceEnabled()) {
            log.trace(String.format("Reply from %s is %s", gossiper,
                                    Arrays.toString(digests)));
        }
        gossip.reply(digests, new GossipHandler(gossiper));
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
            log.trace(format("Ring state from %s is : %s", gossiper, state));
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
            log.trace(format("Update state from %s is : %s", gossiper, state));
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
    private void send(byte msgType, ByteBuffer buffer, SocketAddress target) {
        if (socket.isClosed()) {
            log.trace(String.format("Sending on a closed socket"));
            return;
        }
        int msgLength = buffer.position();
        int totalLength = msgLength + hmac.getMacLength();
        buffer.putInt(0, MAGIC);
        buffer.put(MAGIC_BYTE_SIZE, msgType);
        byte[] bytes = buffer.array();
        try {
            addMac(bytes, 0, msgLength);
        } catch (ShortBufferException e) {
            log.error("Invalid message %s",
                      prettyPrint(getLocalAddress(), target, buffer.array(),
                                  msgLength));
            return;
        } catch (SecurityException e) {
            log.error("No key provided for HMAC");
            return;
        }
        try {
            DatagramPacket packet = new DatagramPacket(bytes, totalLength,
                                                       target);
            if (log.isTraceEnabled()) {
                log.trace(String.format("sending packet mac start: %s %s",
                                        msgLength,
                                        prettyPrint(getLocalAddress(), target,
                                                    buffer.array(), totalLength)));
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
        if (log.isTraceEnabled()) {
            log.trace(String.format("listening for packet on %s", localAddress));
        }
        socket.receive(packet);
        buffer.limit(packet.getLength());
        if (log.isTraceEnabled()) {
            log.trace(String.format("Received packet %s",
                                    prettyPrint(packet.getSocketAddress(),
                                                getLocalAddress(),
                                                buffer.array(),
                                                packet.getLength())));
        } else if (log.isTraceEnabled()) {
            log.trace("Received packet from: " + packet.getSocketAddress());
        }
        dispatcher.execute(new Runnable() {
            @Override
            public void run() {
                int magic = buffer.getInt();
                if (MAGIC == magic) {
                    try {
                        if (!checkMac(buffer.array(), 0, packet.getLength()
                                                         - hmac.getMacLength())) {
                            if (log.isWarnEnabled()) {
                                log.warn(format("Error processing inbound message on: %s, HMAC does not check",
                                                getLocalAddress()));
                            }
                            return;
                        }
                    } catch (SecurityException e) {
                        if (log.isWarnEnabled()) {
                            log.warn(format("Error processing inbound message on: %s, HMAC does not check",
                                            getLocalAddress()), e);
                        }
                        return;
                    }
                    buffer.limit(packet.getLength() - hmac.getMacLength());
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
                                           localAddress));
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
        state.writeTo(buffer);
        send(msg, buffer, address);
    }
}
