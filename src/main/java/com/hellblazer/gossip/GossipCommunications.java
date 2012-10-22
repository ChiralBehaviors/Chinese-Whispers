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

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * The service interface for connecting to new members
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 */

public interface GossipCommunications {

    /**
     * Asynchronously create a new connection to the indicated address. When the
     * connection is established, run the connect action.
     * <p>
     * Note that this is an asynchronous operation, and the handler will not be
     * ready for communications unless and until the connectAction is run.
     * 
     * @param address
     *            - the address to create a connection to
     * @param endpoint
     *            - the endpoint to connect
     * @param connectAction
     *            - the action to run when the new connection is fully
     *            established.
     * @throws IOException
     *             - if there is a problem creating a connection to the address
     */
    void connect(InetSocketAddress address, Endpoint endpoint,
                 Runnable connectAction) throws IOException;

    /**
     * Answer the local address of the communcations endpoint
     * 
     * @return the socket address
     */
    InetSocketAddress getLocalAddress();

    /**
     * Send the replicated state around the ring via the left members
     * 
     * @param state
     * @param inetSocketAddress
     */
    void update(Update state, InetSocketAddress inetSocketAddress);

    /**
     * Set the gossip service
     * 
     * @param gossip
     */
    void setGossip(Gossip gossip);

    /**
     * Start the communications service
     */
    void start();

    /**
     * Tereminate the communications service
     */
    void terminate();
}
