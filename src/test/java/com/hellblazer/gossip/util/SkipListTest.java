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

import java.util.Arrays;
import java.util.Random;

import junit.framework.TestCase;

import com.hellblazer.gossip.util.SkipList;

public class SkipListTest extends TestCase {
    public void testCounting() {
        SkipList list = new SkipList();
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }

        assertEquals(6, list.countLessThanEqualTo(5.0));
    }

    public void testDuplicates() {
        SkipList list = new SkipList();
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }
        assertEquals(10, list.size());

        assertTrue(list.add(5));

        assertEquals(11, list.size());

        list.remove(Integer.valueOf(5));

        assertEquals(10, list.size());

        assertTrue(list.contains(Integer.valueOf(5)));

        list.remove(Integer.valueOf(5));

        assertEquals(9, list.size());

        assertFalse(list.contains(Integer.valueOf(5)));
    }

    public void testLargeRandom() {
        Random r = new Random(666);
        SkipList list = new SkipList();
        int count = 10000;
        double[] original = new double[count];
        double[] sorted = new double[count];
        for (int i = 0; i < count; i++) {
            double data = r.nextLong();
            sorted[i] = data;
            original[i] = data;
            list.add(data);
        }
        Arrays.sort(sorted);
        assertEquals(count, list.size());
        int i = 0;
        for (double d : sorted) {
            assertEquals(d, list.get(i++));
        }
        for (i = 0; i < count; i++) {
            assertTrue(list.remove(original[i]));
            assertEquals(count - (i + 1), list.size());
        }
    }
}
