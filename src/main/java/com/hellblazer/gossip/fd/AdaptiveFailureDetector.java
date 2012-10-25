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

    private double       last        = -1.0;
    private final double minInterval;
    private final double scale;
    private final double threshold;
    private double       sumOfDelays = 0.0;

    /**
     * 
     * @param convictionThreshold
     *            - the level of certainty that must be met before conviction.
     *            This value must be <= 1.0
     * @param windowSize
     *            - the number of samples in the window
     * @param scale
     *            - a scale factor to accomidate the real world
     * @param expectedSampleInterval
     *            - the expected sample interval, used to prime the detector
     * @param initialSamples
     *            - the number of initial samples to prime the detector
     * @param minimumInterval
     *            - the minimum inter arival interval
     */
    public AdaptiveFailureDetector(double convictionThreshold, int windowSize,
                                   double scale, long expectedSampleInterval,
                                   int initialSamples, double minimumInterval) {
        super(windowSize, 2);
        if (convictionThreshold > 1.0) {
            throw new IllegalArgumentException(
                                               String.format("Conviction threshold %s must be <= 1.0",
                                                             convictionThreshold));
        }
        threshold = convictionThreshold;
        minInterval = minimumInterval;
        this.scale = scale;

        long now = System.currentTimeMillis();
        last = now - initialSamples * expectedSampleInterval;
        for (int i = 0; i < initialSamples; i++) {
            record((long) (last + expectedSampleInterval), 0L);
        }
        last = -1.0;
    }

    @Override
    public synchronized void record(long timeStamp, long delay) {
        if (last >= 0.0) {
            double sample = timeStamp - last;
            if (sample > minInterval) {
                sumOfDelays += delay;
                if (count == samples.length) {
                    double[] removed = removeFirst();
                    sumOfDelays -= removed[1];
                }
                addLast(sample, delay);
            }
        }
        double averageDelay = count == 0 ? 0.0 : sumOfDelays / count;
        last = timeStamp + averageDelay;
    }

    @Override
    public synchronized boolean shouldConvict(long now) {
        if (last < 0) {
            return false;
        }

        double delta = (now - last - sumOfDelays / count) * scale;
        double countLessThanEqualTo = countLessThanEqualTo(delta);
        double probability = countLessThanEqualTo / count;
        boolean convict = probability >= threshold;
        /*
        if (convict) {
            System.out.println(String.format("delta: %s, count: %s, size: %s, probability: %s, window: %s",
                                             delta, countLessThanEqualTo,
                                             count, probability, sorted));
        }*/
        return convict;
    }

    /**
     * @param delta
     * @return
     */
    private double countLessThanEqualTo(double delta) {
        int deltaCount = 0;
        for (int i = 0; i < count; i++) {

            if (samples[(i + head) % samples.length][0] <= delta) {
                deltaCount++;
            }
        }
        return deltaCount;
    }
}
