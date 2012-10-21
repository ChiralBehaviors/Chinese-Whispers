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
import java.nio.ByteBuffer;

import junit.framework.TestCase;

import com.hellblazer.gossip.Digest;
import com.hellblazer.gossip.GossipMessages;
import com.hellblazer.gossip.Digest.DigestComparator;

/**
 * Basic testing of the digest state
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public class DigestTest extends TestCase {
    public void testBasic() throws Exception {
        InetSocketAddress address = new InetSocketAddress("localhost", 80);
        Digest s = new Digest(address, 667);
        assertEquals(667, s.getTime());
        assertEquals(address, s.getAddress());
        byte[] bytes = new byte[GossipMessages.DIGEST_BYTE_SIZE];
        ByteBuffer msg = ByteBuffer.wrap(bytes);
        s.writeTo(msg);
        msg.flip();
        Digest d = new Digest(msg);
        assertEquals(667, d.getTime());
        assertEquals(address, d.getAddress());
        DigestComparator comparator = new DigestComparator();
        assertEquals(0, comparator.compare(s, d));

        Digest l2 = new Digest(address, 666);
        assertEquals(-1, comparator.compare(l2, d));
        Digest g2 = new Digest(address, 668);
        assertEquals(1, comparator.compare(g2, d));
    }
}
