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
package com.hellblazer.gossip.util;

import static junit.framework.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.hellblazer.gossip.util.ByteBufferPool;

/**
 * @author hhildebrand
 * 
 */
public class ByteBufferPoolTest {

    @Test
    public void testFree() {
        ByteBufferPool test = new ByteBufferPool("test", 100);
        for (int i = 0; i < 100; i++) {
            test.free(ByteBuffer.allocate(i));
        }
        assertEquals(0, test.getCreated());
        assertEquals(100, test.getPooled());

        test.free(ByteBuffer.allocate(100));

        assertEquals(100, test.getPooled());
        assertEquals(100, test.size());
        assertEquals(1, test.getDiscarded());

        for (int i = 0; i < 100; i++) {
            assertEquals(i, test.allocate(i).capacity());
        }

        assertEquals(0, test.size());
        assertEquals(0, test.getCreated());

        assertEquals(100, test.allocate(100).capacity());

        assertEquals(1, test.getCreated());
        assertEquals(0, test.size());
    }

    @Test
    public void testFreeMatch() {
        ByteBufferPool test = new ByteBufferPool("test", 100);
        for (int i = 0; i < 100; i++) {
            test.free(ByteBuffer.allocate(i));
        }
        assertEquals(0, test.getCreated());
        assertEquals(100, test.getPooled());

        test.free(ByteBuffer.allocate(100));

        assertEquals(100, test.getPooled());
        assertEquals(100, test.size());
        assertEquals(1, test.getDiscarded());

        for (int i = 99; i >= 0; i--) {
            assertEquals(i, test.allocate(i).capacity());
        }

        assertEquals(0, test.size());
        assertEquals(0, test.getCreated());

        assertEquals(100, test.allocate(100).capacity());

        assertEquals(1, test.getCreated());
        assertEquals(0, test.size());
    }
}
