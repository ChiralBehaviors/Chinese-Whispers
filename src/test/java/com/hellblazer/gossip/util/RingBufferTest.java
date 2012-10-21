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
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;

import java.util.Random;

import org.junit.Test;

import com.hellblazer.gossip.util.RingBuffer;

/**
 * @author hhildebrand
 * 
 */
public class RingBufferTest {
    @Test
    public void testOffer() {
        RingBuffer<String> test = new RingBuffer<String>(1000);
        for (int i = 0; i < 1000; i++) {
            assertEquals("Invalid size", i, test.size());
            assertTrue(test.offer(String.format("Offer: %s", i)));
        }
        assertEquals("Invalid size", 1000, test.size());
        assertFalse((test.offer(String.format("Offer: %s", 1001))));
    }

    @Test
    public void testPoll() {
        RingBuffer<String> test = new RingBuffer<String>(1000);
        for (int i = 0; i < 1000; i++) {
            test.add(String.format("Add: %s", i));
        }
        for (int i = 0; i < 1000; i++) {
            assertEquals("Invalid size", 1000 - i, test.size());
            assertEquals(String.format("Add: %s", i), test.poll());
        }
        assertNull(test.poll());
    }

    @Test
    public void testAccordian() {
        Random r = new Random(0x666);
        RingBuffer<Integer> test = new RingBuffer<Integer>(1000);
        for (int i = 0; i < 500; i++) {
            test.add(i);
        }
        int count = test.size();
        for (int i = 0; i < 10000; i++) {
            if (r.nextBoolean()) {
                assertTrue(test.offer(i));
                count++;
                assertEquals(count, test.size());
            } else {
                assertNotNull(test.poll());
                count--;
                assertEquals(count, test.size());
            }
        }
        assertEquals(count, test.size());
    }

    @Test
    public void testIteration() {
        RingBuffer<String> test = new RingBuffer<String>(1000);
        for (int i = 0; i < 1000; i++) {
            test.add(String.format("Offer: %s", i));
        }
        int i = 0;
        for (String element : test) {
            assertEquals(String.format("Offer: %s", i++), element);
        }
    }

    @Test
    public void testPollOrder() {
        RingBuffer<String> test = new RingBuffer<String>(1000);
        for (int i = 0; i < 1000; i++) {
            test.add(String.format("Offer: %s", i));
        }
        for (int i = 0; i < 1000; i++) {
            String element = test.poll();
            assertEquals(String.format("Offer: %s", i), element);
        }
    }
}
