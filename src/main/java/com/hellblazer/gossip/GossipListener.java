/**
 * Copyright (c) 2012, salesforce.com, inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided
 * that the following conditions are met:
 *
 *    Redistributions of source code must retain the above copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 *    Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
 *    the following disclaimer in the documentation and/or other materials provided with the distribution.
 *
 *    Neither the name of salesforce.com, inc. nor the names of its contributors may be used to endorse or
 *    promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

package com.hellblazer.gossip;

import static com.hellblazer.gossip.GossipMessages.MAX_SEG_SIZE;
import static com.hellblazer.gossip.GossipMessages.UPDATE_HEADER_BYTE_SIZE;
import static com.hellblazer.gossip.HMAC.MAC_BYTE_SIZE;

import java.util.UUID;

/**
 * @author hhildebrand
 * 
 */
public interface GossipListener {

    int MAX_STATE_SIZE = MAX_SEG_SIZE - MAC_BYTE_SIZE - UPDATE_HEADER_BYTE_SIZE;

    /**
     * Previously known state has been abandoned
     * 
     * @param id
     *            - the id of the state that has been aba
     */
    void deregister(UUID id);

    /**
     * The state is newly discovered
     * 
     * @param id
     *            - the id assigned to this state
     * @param state
     *            - the content of the state
     */
    void register(UUID id, byte[] state);

    /**
     * Previously known state has been updated
     * 
     * @param id
     *            - the id assigned to this state
     * @param state
     *            - the updated content of the state
     */
    void update(UUID id, byte[] state);
}
