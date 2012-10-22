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

import java.util.List;

/**
 * The communications interface used by the gossip protocol
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public interface GossipMessages {
    /**
     * MAX_SEG_SIZE is a default maximum packet size. This may be small, but any
     * network will be capable of handling this size so the packet transfer
     * semantics are atomic (no fragmentation in the network).
     */
    int  MAX_SEG_SIZE               = 1500;
    int  DATA_POSITION              = 4;
    byte GOSSIP                     = 1;
    byte REPLY                      = 2;
    byte UPDATE                     = 3;
    byte RING                       = 4;
    int  INET_ADDRESS_MAX_BYTE_SIZE = 4 // address 
                                    + 4;   // port 
    int  DIGEST_BYTE_SIZE           = INET_ADDRESS_MAX_BYTE_SIZE // address
                                    + 8;   // timestamp

    /**
     * Close the communications connection
     */
    void close();

    /**
     * The first message of the gossip protocol. Send a list of the shuffled
     * digests of the receiver's view of the endpoint state
     * 
     * @param digests
     *            - the list of heartbeat digests the receiver knows about
     */
    void gossip(List<Digest> digests);

    /**
     * The second message in the gossip protocol. Send a list of digests the
     * node this handler represents, that would like replicated state updates
     * for, along with the list of replicated state this node believes is out of
     * date on the node this handler represents.
     * 
     * @param digests
     *            - the digests representing desired state updates
     * @param states
     *            - the updates for the node which are believed to be out of
     *            date
     */
    void reply(List<Digest> digests, List<Update> states);

    /**
     * The third message of the gossip protocol. Send a list of updated states
     * to the node this handler represents, which is requesting the updates.
     * 
     * @param deltaState
     *            - the list of replicated states requested.
     */
    void update(List<Update> deltaState);

}