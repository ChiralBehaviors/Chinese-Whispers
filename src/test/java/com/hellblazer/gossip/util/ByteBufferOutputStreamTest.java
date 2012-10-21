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

import com.hellblazer.gossip.util.ByteBufferOutputStream;
import com.hellblazer.gossip.util.ByteBufferPool;

/**
 * @author hhildebrand
 * 
 */
public class ByteBufferOutputStreamTest {
    @Test
    public void testGrow() {
        ByteBufferPool pool = new ByteBufferPool("test", 100);
        @SuppressWarnings("resource")
        ByteBufferOutputStream test = new ByteBufferOutputStream(pool);
        for (int i = 0; i < 1024 * 1024; i++) {
            test.write(5);
        }
        ByteBuffer produced = test.toByteBuffer();
        assertEquals("Invalid buffer limit", 1024 * 1024, produced.limit());
        assertEquals("Invalid capacity", 1048576, produced.capacity());
        assertEquals("Invalid bytes allocated", 2097120,
                     pool.getBytesAllocated());
        assertEquals("Invalid buffer allocations", 16, pool.getCreated());
    }
}
