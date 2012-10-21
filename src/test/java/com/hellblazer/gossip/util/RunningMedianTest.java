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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import com.hellblazer.gossip.util.RunningMedian;

public class RunningMedianTest extends TestCase {
    public void testMedian() {
        Random random = new Random(666);
        RunningMedian median = new RunningMedian(1000);
        List<Double> input = new ArrayList<Double>();

        for (int i = 0; i < 300; i++) {
            double sample = random.nextDouble();
            input.add(sample);
            median.sample(sample);
        }
        Collections.sort(input);
        assertEquals(input.get(input.size() / 2), median.value());

        median = new RunningMedian(1000);
        input = new ArrayList<Double>();
        for (int i = 0; i < 1500; i++) {
            double sample = random.nextDouble();
            input.add(sample);
            median.sample(sample);
        }
        input = input.subList(500, 1500);
        assertEquals(1000, input.size());
        Collections.sort(input);
        assertEquals(input.get(input.size() / 2), median.value());
    }
}
