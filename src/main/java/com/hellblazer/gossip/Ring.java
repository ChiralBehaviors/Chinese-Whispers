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

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Ring {
    private final GossipCommunications               comms;
    private final AtomicReference<InetSocketAddress> neighbor = new AtomicReference<InetSocketAddress>();
    private final Endpoint                           endpoint;
    private static final Logger                      log      = LoggerFactory.getLogger(Ring.class.getCanonicalName());

    public Ring(InetSocketAddress address, GossipCommunications comms) {
        endpoint = new Endpoint(address);
        this.comms = comms;
    }

    /**
     * Send the heartbeat around the ring in both directions.
     * 
     * @param state
     */
    public void send(Update state) {
        InetSocketAddress target = neighbor.get();
        if (target != null) {
            if (target.equals(state.node)) {
                if (log.isTraceEnabled()) {
                    log.trace(String.format("Not forwarding state %s to the node that owns it",
                                            state));
                }
            } else {
                comms.update(state, target);
            }
        } else {
            if (log.isTraceEnabled()) {
                log.trace(String.format("Ring has not been formed, not forwarding state"));
            }
        }
    }

    /**
     * Update the neighboring members of the id on the ring represented by the
     * members.
     * 
     * @param members
     * @param endpoints
     */
    public void update(Collection<Endpoint> endpoints) {
        SortedSet<Endpoint> members = new TreeSet<Endpoint>();
        members.addAll(endpoints);
        members.remove(endpoint);
        if (members.size() < 3) {
            if (log.isTraceEnabled()) {
                log.trace(String.format("Ring has not been formed"));
            }
            return;
        }
        SortedSet<Endpoint> head = members.headSet(endpoint);
        if (!head.isEmpty()) {
            neighbor.set(head.last().getAddress());
        } else {
            neighbor.set(members.last().getAddress());
        }
    }
}
