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

/**
 * Provide the median value for a windowed set of samples.
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public class RunningMedian extends Window implements SampledWindow {
    private final SkipList sorted = new SkipList();

    public RunningMedian(int windowSize) {
        super(windowSize);
    }

    @Override
    public void sample(double sample) {
        sorted.add(sample);
        if (count == samples.length) {
            sorted.remove(removeFirst());
        }
        addLast(sample);
    }

    @Override
    public double value() {
        if (count == 0) {
            throw new IllegalStateException(
                                            "Must have at least one sample to calculate the median");
        }
        return sorted.get(sorted.size() / 2);
    }
}
