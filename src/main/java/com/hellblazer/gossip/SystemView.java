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

import static java.lang.String.format;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a view on the known endpoint state for the system. The primary
 * responsibility of the system view is to provide random members from the
 * various subsets of the member endpoints the view tracks. The system endpoint
 * view is composed of live endpoints (endpoints that are considered up and
 * functioning normally) and unreachable endpoints (endpoints that are
 * considered down and non functional). A subset of the endpoints in the system
 * serve as seeds that form the kernel set of endpoints used to construct the
 * system view. The system view also tracks members that are considered
 * quarantined. Quarantined members are members that have been marked dead and
 * are prohibited from rejoining the set of live endpoints until the quarantine
 * period has elapsed.
 * 
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public class SystemView {
    private static final Logger                log         = LoggerFactory.getLogger(SystemView.class);
    private final Random                       entropy;
    private final InetSocketAddress            localAddress;
    private final Map<InetSocketAddress, Long> quarantined = new HashMap<InetSocketAddress, Long>();
    private final long                         quarantineInterval;
    private final List<InetSocketAddress>      seeds       = new ArrayList<InetSocketAddress>();
    private final Map<InetSocketAddress, Long> unreachable = new HashMap<InetSocketAddress, Long>();
    private final long                         unreachableInterval;

    /**
     * 
     * @param random
     *            - a source of entropy
     * @param localAddress
     *            - the local address the system
     * @param seedHosts
     *            - the kernel set of endpoints used to construct the system
     *            view
     * @param quarantineDelay
     *            - the interval a failing member must remain quarantined before
     *            rejoining the view a a live member
     * @param unreachableDelay
     *            - the interval it takes before the system finally considers a
     *            member really and truly dead
     */
    public SystemView(Random random, InetSocketAddress localAddress,
                      List<InetSocketAddress> seedHosts, long quarantineDelay,
                      long unreachableDelay) {
        assert validAddresses(seedHosts);
        entropy = random;
        this.localAddress = localAddress;
        quarantineInterval = quarantineDelay;
        unreachableInterval = unreachableDelay;
        for (InetSocketAddress seed : seedHosts) {
            if (!seed.equals(localAddress)) {
                seeds.add(seed);
            }
        }
        log.info(format("System view initialized for: %s, seeds: %s",
                        localAddress, seeds));
    }

    /**
     * Reconsider endpoints that have been quarantined for a sufficient time.
     * 
     * @param now
     *            - the time to determine the interval the endpoint has been
     *            quarantined
     */
    public synchronized void cullQuarantined(long now) {
        for (Iterator<Map.Entry<InetSocketAddress, Long>> iterator = quarantined.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry<InetSocketAddress, Long> entry = iterator.next();
            if (now - entry.getValue() > quarantineInterval) {
                if (log.isTraceEnabled()) {
                    log.trace(format("%s elapsed, %s gossip quarantine over",
                                     quarantineInterval, entry.getKey()));
                }
                iterator.remove();
                unreachable.put(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * Remove endpoints that have been unreachable for a long time
     * 
     * @param now
     *            - the time to determine the interval the endpoint has been
     *            unreachable
     */
    public synchronized void cullUnreachable(long now) {
        for (Iterator<Map.Entry<InetSocketAddress, Long>> iterator = quarantined.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry<InetSocketAddress, Long> entry = iterator.next();
            if (now - entry.getValue() > unreachableInterval) {
                if (log.isTraceEnabled()) {
                    log.trace(format("%s elapsed, %s is now considered truly dead",
                                     unreachableInterval, entry.getKey()));
                }
                iterator.remove();
                unreachable.put(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * Answer how long, in millseconds, the endpoint has been unreachable
     * 
     * @param endpoint
     * @return the number of milliseconds the endpoint has been unreachable, or
     *         0, if the endpoint is not in the set of unreachable endpoints
     */
    public synchronized long getEndpointDowntime(InetSocketAddress endpoint) {
        Long downtime;
        downtime = unreachable.get(endpoint);
        if (downtime != null) {
            return System.currentTimeMillis() - downtime;
        }
        return 0L;
    }

    /**
     * Answer a random member of the endpoint collection.
     * 
     * @param endpoints
     *            - the endpoints to sample
     * @return the selected member
     */
    public synchronized InetSocketAddress getRandomMember(Collection<InetSocketAddress> endpoints) {
        if (endpoints.isEmpty()) {
            return null;
        }
        int size = endpoints.size();
        int index = size == 1 ? 0 : entropy.nextInt(size);
        int i = 0;
        for (InetSocketAddress address : endpoints) {
            if (i == index) {
                return address;
            }
            i++;
        }
        log.warn(format("We should have found the selected random member of the supplied endpoint set: %s",
                        index));
        return null;
    }

    /**
     * Answer a random member of the seed set. We only return a member of the
     * seed set if the member supplied is null, or if the size of the live
     * endpoint set is smaller than the size of the seed set, a member is
     * selected with the probleability defined by the ratio of the cardinality
     * of the seed set dived by the sum of the cardinalities of the live and
     * unreachable endpoint sets
     * 
     * @param member
     *            - the members that have been gossiped with
     * 
     * @return a random member of the seed set, if appropriate, or null
     */
    public synchronized InetSocketAddress getRandomSeedMember(List<InetSocketAddress> members) {
        if (members.isEmpty()) {
            return getRandomMember(seeds);
        } else if (seeds.containsAll(members)) {
            return null;
        }
        if (seeds.size() == 0 || seeds.size() == 1
            && seeds.contains(localAddress)) {
            return null;
        }

        InetSocketAddress seed = null;
        int attempts = 0;
        do {
            if (attempts++ > seeds.size()) {
                return null;
            }
            seed = getRandomMember(seeds);
        } while (localAddress.equals(seed) || members.contains(seed));
        return seed;
    }

    /**
     * Answer a random member of the unreachable set. The unreachable set will
     * be sampled with a probability of the cardinality of the unreachable set
     * divided by the cardinality of the live set of endpionts + 1
     * 
     * @return the unreachable member selected, or null if none selected or
     *         available
     */
    public synchronized InetSocketAddress getRandomUnreachableMember(double liveSetSize) {
        if (entropy.nextDouble() < unreachable.size() / (liveSetSize + 1.0)) {
            return getRandomMember(unreachable.keySet());
        }
        return null;
    }

    /**
     * Answer the set of unreachable members in the view
     * 
     * @return the set of unreachable endpoints.
     */
    public synchronized Collection<InetSocketAddress> getUnreachableMembers() {
        return Collections.unmodifiableCollection(unreachable.keySet());
    }

    /**
     * Answer true if the endpoint is quarantined.
     * 
     * @param ep
     *            - the endpoint to query
     * @return true if the endpoint is currently quarantined
     */
    public synchronized boolean isQuarantined(InetSocketAddress ep) {
        return quarantined.containsKey(ep);
    }

    /**
     * Mark the endpoint as live.
     * 
     * @param endpoint
     *            - the endpoint to mark as live
     */
    public synchronized void markAlive(InetSocketAddress endpoint) {
        unreachable.remove(endpoint);
    }

    /**
     * Mark the endpoint as dead
     * 
     * @param endpoint
     *            - the endpoint to mark as dead
     */
    public synchronized void markDead(InetSocketAddress endpoint, long now) {
        quarantined.put(endpoint, now);
    }

    private boolean validAddresses(Collection<InetSocketAddress> hosts) {
        for (InetSocketAddress address : hosts) {
            assert address.getPort() != 0 : String.format("Invalid host address: %s",
                                                          address);
        }
        return true;
    }
}
