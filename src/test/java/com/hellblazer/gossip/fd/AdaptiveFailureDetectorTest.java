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
package com.hellblazer.gossip.fd;

import java.util.Random;

import junit.framework.TestCase;

import com.hellblazer.gossip.FailureDetector;

public class AdaptiveFailureDetectorTest extends TestCase {

    public void testDetector() throws Exception {

        FailureDetector detector = new AdaptiveFailureDetectorFactory(0.95,
                                                                      1000,
                                                                      0.95,
                                                                      500, 0,
                                                                      0.0).create();
        Random random = new Random(666);

        long average = 500;
        int variance = 100;
        long now = System.currentTimeMillis();

        for (int i = 0; i < 950; i++) {
            now += average;
            now += variance / 2 - random.nextInt(variance);
            detector.record(now, 0L);
        }

        assertEquals(false, detector.shouldConvict(now + variance));

        now += 573;
        assertFalse(detector.shouldConvict(now));

        assertTrue(detector.shouldConvict(now + 30000));
    }
}
