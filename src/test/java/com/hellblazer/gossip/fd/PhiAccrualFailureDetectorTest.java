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

import junit.framework.TestCase;

import com.hellblazer.gossip.FailureDetector;
import com.hellblazer.gossip.fd.PhiAccrualFailureDetector;

/**
 * Basic testing of the failure detector
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public class PhiAccrualFailureDetectorTest extends TestCase {

    public void testDetector() throws Exception {

        FailureDetector detector = new PhiAccrualFailureDetector(11, false,
                                                                 1000, 500, 0,
                                                                 1.0);
        long inc = 500;

        long now = System.currentTimeMillis();

        detector.record(now, 0L);
        now += inc;

        detector.record(now, 0L);
        now += inc;

        detector.record(now, 0L);
        now += inc;

        detector.record(now, 0L);
        now += inc;

        detector.record(now, 0L);

        now += inc;

        assertFalse(detector.shouldConvict(now));

        assertTrue(detector.shouldConvict(now + 30000));
    }
}
