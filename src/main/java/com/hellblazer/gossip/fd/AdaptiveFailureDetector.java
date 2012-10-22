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

import com.hellblazer.gossip.FailureDetector;
import com.hellblazer.utils.collections.SkipList;
import com.hellblazer.utils.windows.MultiWindow;

/**
 * An adaptive accural failure detector based on the paper:
 * "A New Adaptive Accrual Failure Detector for Dependable Distributed Systems"
 * by Benjamin Satzger, Andreas Pietzowski, Wolfgang Trumler, Theo Ungerer
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public class AdaptiveFailureDetector extends MultiWindow implements
        FailureDetector {

    private double         last        = -1.0;
    private final double   minInterval;
    private final double   scale;
    private final SkipList sorted      = new SkipList();
    private final double   threshold;
    private double         sumOfDelays = 0.0;

    public AdaptiveFailureDetector(double convictionThreshold, int windowSize,
                                   double scale, long expectedSampleInterval,
                                   int initialSamples, double minimumInterval) {
        super(windowSize, 2);
        threshold = convictionThreshold;
        minInterval = minimumInterval;
        this.scale = scale;

        long now = System.currentTimeMillis();
        last = now - initialSamples * expectedSampleInterval;
        for (int i = 0; i < initialSamples; i++) {
            record((long) (last + expectedSampleInterval), 0L);
        }
        assert last == now;
    }

    @Override
    public synchronized void record(long timeStamp, long delay) {
        if (last >= 0.0) {
            double sample = timeStamp - last;
            if (sample < minInterval) {
                return;
            }
            sorted.add(sample);
            sumOfDelays += delay;
            if (count == samples.length) {
                double[] removed = removeFirst();
                sorted.remove(removed[0]);
                sumOfDelays -= removed[1];
            }
            addLast(sample, delay);
        }
        last = timeStamp + sumOfDelays / count;
    }

    @Override
    public synchronized boolean shouldConvict(long now) {
        double delta = (now - last) * scale;
        double countLessThanEqualTo = sorted.countLessThanEqualTo(delta);
        boolean convict = countLessThanEqualTo / count >= threshold;
        return convict;
    }
}
