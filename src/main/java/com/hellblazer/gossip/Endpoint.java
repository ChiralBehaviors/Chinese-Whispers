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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.utils.fd.FailureDetector;
import com.hellblazer.utils.fd.FailureDetectorFactory;

/**
 * The Endpoint keeps track of the replicated state and the failure detector for
 * remote clients
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */

public class Endpoint implements Comparable<Endpoint> {
    public static enum State {
        CONNECTING, ALIVE, DEAD;
    }

    protected static Logger logger = LoggerFactory.getLogger(Endpoint.class);

    public static int compare(InetSocketAddress o1, InetSocketAddress o2) {
        if (o1 == o2) {
            return 0;
        } else if (o1.isUnresolved() || o2.isUnresolved()) {
            return o1.toString().compareTo(o2.toString());
        } else {
            int compare = getIp(o1).compareTo(getIp(o2));
            if (compare == 0) {
                compare = Integer.valueOf(o1.getPort()).compareTo(o2.getPort());
            }
            return compare;
        }
    }

    public static Integer getIp(InetSocketAddress addr) {
        byte[] a = addr.getAddress().getAddress();
        return (a[0] & 0xff) << 24 | (a[1] & 0xff) << 16 | (a[2] & 0xff) << 8
               | a[3] & 0xff;
    }

    public static InetSocketAddress readInetAddress(ByteBuffer msg)
                                                                   throws UnknownHostException {
        byte[] address = new byte[4];
        msg.get(address);
        int port = msg.getInt();

        InetAddress inetAddress = InetAddress.getByAddress(address);
        return new InetSocketAddress(inetAddress, port);
    }

    public static void writeInetAddress(InetSocketAddress ipaddress,
                                        ByteBuffer bytes) {
        byte[] address = ipaddress.getAddress().getAddress();
        bytes.put(address);
        bytes.putInt(ipaddress.getPort());
    }

    private final InetSocketAddress          address;
    private FailureDetector                  fd;
    private volatile GossipMessages          handler;
    private volatile State                   state     = State.CONNECTING;
    private final ReentrantLock              synch     = new ReentrantLock();
    private final Map<UUID, ReplicatedState> states    = new HashMap<UUID, ReplicatedState>();
    private final AtomicInteger              suspected = new AtomicInteger(0);

    public Endpoint(InetSocketAddress address) {
        this(address, null);
    }

    /**
     * @param address2
     * @param create
     * @param handlerFor
     */
    public Endpoint(InetSocketAddress address, GossipMessages handler) {
        this.address = address;
        this.handler = handler;
    }

    public Endpoint(InetSocketAddress address, ReplicatedState replicatedState,
                    GossipMessages handler) {
        this(address, handler);
        states.put(replicatedState.getId(), replicatedState);
    }

    /**
     * @param digests
     */
    public void addDigestsTo(ArrayList<Digest> digests) {
        final ReentrantLock myLock = synch;
        myLock.lock();
        try {
            for (ReplicatedState state : states.values()) {
                digests.add(new Digest(address, state));
            }
        } finally {
            myLock.unlock();
        }

    }

    /* (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(Endpoint o) {
        return compare(address, o.address);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Endpoint)) {
            return false;
        }
        return address.equals(((Endpoint) o).address);
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public GossipMessages getHandler() {
        return handler;
    }

    public ReplicatedState getState(UUID id) {
        final ReentrantLock myLock = synch;
        myLock.lock();
        try {
            return states.get(id);
        } finally {
            myLock.unlock();
        }
    }

    /**
     * @return
     */
    public Collection<ReplicatedState> getStates() {
        final ReentrantLock myLock = synch;
        myLock.lock();
        try {
            return new ArrayList<ReplicatedState>(states.values());
        } finally {
            myLock.unlock();
        }
    }

    public long getTime(UUID id) {
        ReplicatedState state = getState(id);
        if (state == null) {
            return -1L;
        }
        return state.getTime();
    }

    @Override
    public int hashCode() {
        return address.hashCode();
    }

    public boolean isAlive() {
        final ReentrantLock myLock = synch;
        myLock.lock();
        try {
            return state == State.ALIVE;
        } finally {
            myLock.unlock();
        }
    }

    public void markAlive(Runnable action, FailureDetectorFactory fdFactory) {
        final ReentrantLock myLock = synch;
        myLock.lock();
        try {
            switch (state) {
                case ALIVE:
                    return;
                default:
                    state = State.ALIVE;
                    fd = fdFactory.create();
                    action.run();
            }
        } finally {
            myLock.unlock();
        }
    }

    public void markDead() {
        final ReentrantLock myLock = synch;
        myLock.lock();
        try {
            state = State.DEAD;
            fd = null;
        } finally {
            myLock.unlock();
        }
    }

    public void setCommunications(GossipMessages communications) {
        handler = communications;
    }

    /**
     * Answer true if the suspicion level of the failure detector is greater
     * than the conviction threshold.
     * 
     * @param now
     *            - the time at which to base the measurement
     * @param cleanUp
     *            - the number of cycles of #fail required before conviction
     * @return true if the suspicion level of the failure detector is greater
     *         than the conviction threshold
     */
    public boolean shouldConvict(long now, int cleanUp) {
        if (fd.shouldConvict(now)) {
            return suspected.incrementAndGet() >= cleanUp;
        }
        suspected.set(0);
        return false;
    }

    @Override
    public String toString() {
        return String.format("Endpoint[%s:%S]", address, state);
    }

    public void updateState(ReplicatedState newState, Gossip gossip) {
        assert newState != null : "updated state cannot be null";
        final ReentrantLock myLock = synch;
        myLock.lock();
        try {
            ReplicatedState prev = states.get(newState.getId());
            if (prev == null) {
                states.put(newState.getId(), newState);
                if (state == State.ALIVE && newState.isNotifiable()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("Notifiying registration of %s for %s",
                                                   newState.getId(), address));
                    }
                    gossip.notifyRegister(newState);
                }
            } else {
                if (prev.getTime() < newState.getTime()) {
                    states.put(newState.getId(), newState);
                    if (state == State.ALIVE) {
                        if (newState.isDeleted()) {
                            gossip.notifyDeregister(newState);
                        } else if (newState.isNotifiable()) {
                            gossip.notifyUpdate(newState);
                        }
                    }
                }
            }
            if (fd != null && newState.isHeartbeat()) {
                fd.record(newState.getTime(), System.currentTimeMillis()
                                              - newState.getTime());
            }
        } finally {
            myLock.unlock();
        }
        if (logger.isTraceEnabled()) {
            logger.trace(String.format("new replicated state time: %s",
                                       newState.getTime()));
        }
    }
}
