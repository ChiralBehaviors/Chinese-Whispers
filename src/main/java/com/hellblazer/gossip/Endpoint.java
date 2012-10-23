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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Endpoint keeps track of the replicated state and the failure detector for
 * remote clients
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */

public class Endpoint implements Comparable<Endpoint> {
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

    private final FailureDetector            fd;
    private volatile GossipMessages          handler;
    private final Map<UUID, ReplicatedState> states  = new HashMap<UUID, ReplicatedState>();
    private volatile boolean                 isAlive = true;
    private final InetSocketAddress          address;

    public Endpoint(InetSocketAddress address, FailureDetector failureDetector) {
        this.address = address;
        fd = failureDetector;
    }

    public Endpoint(InetSocketAddress address, ReplicatedState replicatedState,
                    FailureDetector failureDetector) {
        this(address, failureDetector);
        states.put(replicatedState.getId(), replicatedState);
    }

    /**
     * @param digests
     */
    public void addDigestsTo(ArrayList<Digest> digests) {
        synchronized (states) {
            for (ReplicatedState state : states.values()) {
                digests.add(new Digest(address, state));
            }
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
        synchronized (states) {
            return states.get(id);
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
        return isAlive;
    }

    public void markAlive() {
        isAlive = true;
    }

    public void markDead() {
        isAlive = false;
    }

    public void setCommunications(GossipMessages communications) {
        handler = communications;
    }

    /**
     * Answer true if the suspicion level of the failure detector is greater
     * than the conviction threshold
     * 
     * @param now
     *            - the time at which to base the measurement
     * @return true if the suspicion level of the failure detector is greater
     *         than the conviction threshold
     */
    public boolean shouldConvict(long now) {
        return fd.shouldConvict(now);
    }

    @Override
    public String toString() {
        return String.format("Endpoint[%s]", address);
    }

    public void updateState(ReplicatedState newState) {
        synchronized (states) {
            states.put(newState.getId(), newState);
        }
        if (fd != null) {
            fd.record(newState.getTime(),
                      System.currentTimeMillis() - newState.getTime());
        }
        if (logger.isTraceEnabled()) {
            logger.trace(String.format("new replicated state time: %s",
                                       newState.getTime()));
        }
    }

    /**
     * @return
     */
    public Collection<ReplicatedState> getStates() {
        synchronized (states) {
            return new ArrayList<ReplicatedState>(states.values());
        }
    }
}
