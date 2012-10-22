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

import java.util.concurrent.locks.ReentrantLock;

import com.hellblazer.gossip.FailureDetector;
import com.hellblazer.utils.windows.RunningAverage;
import com.hellblazer.utils.windows.RunningMedian;
import com.hellblazer.utils.windows.SampledWindow;

/**
 * Instead of providing information of a boolean nature (trust vs. suspect),
 * this failure detector outputs a suspicion level on a continuous scale. The
 * protocol samples the arrival time of heartbeats and maintains a sliding
 * window of the most recent samples. This window is used to estimate the
 * arrival time of the next heartbeat, similarly to conventional adaptive
 * failure detectors. The distribution of past samples is used as an
 * approximation for the probabilistic distribution of future heartbeat
 * messages. With this information, it is possible to compute a value phi with a
 * scale that changes dynamically to match recent network conditions.
 * 
 * Based on the paper, "The phi Accrual Failure Detector", by Naohiro
 * Hayashibara, Xavier Defago, Rami Yared, and Takuya Katayama
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public class PhiAccrualFailureDetector implements FailureDetector {
    private double              last;
    private final double        minInterval;
    private final ReentrantLock stateLock = new ReentrantLock();
    private final double        threshold;
    private SampledWindow       window;

    public PhiAccrualFailureDetector(double convictThreshold,
                                     boolean useMedian, int windowSize,
                                     long expectedSampleInterval,
                                     int initialSamples, double minimumInterval) {
        threshold = convictThreshold;
        minInterval = minimumInterval;
        if (useMedian) {
            window = new RunningMedian(windowSize);
        } else {
            window = new RunningAverage(windowSize);
        }
        long now = System.currentTimeMillis();
        last = now - initialSamples * expectedSampleInterval;
        for (int i = 0; i < initialSamples; i++) {
            record((long) (last + expectedSampleInterval), 0L);
        }
        assert last == now;
    }

    /* (non-Javadoc)
     * @see com.hellblazer.jackal.gossip.FailureDetector#record(long)
     */
    @Override
    public void record(long now, long delay) {
        final ReentrantLock myLock = stateLock;
        try {
            myLock.lockInterruptibly();
        } catch (InterruptedException e) {
            return;
        }
        try {
            double interArrivalTime = now - last;
            if (interArrivalTime < minInterval) {
                return;
            }
            window.sample(interArrivalTime);
            last = now;
        } finally {
            myLock.unlock();
        }
    }

    /**
     * 
     * Given the programmed conviction threshold sigma, and assuming that we
     * decide to suspect when phi >= sigma, when sigma = 1 then the likeliness
     * that we will make a mistake (i.e., the decision will be contradicted in
     * the future by the reception of a late heartbeat) is about 10%. The
     * likeliness is about 1% with sigma = 2, 0.1% with sigma = 3, and so on.
     * <p>
     * Although the original paper suggests that the distribution is
     * approximated by the Gaussian distribution the Cassandra group has
     * reported that the Exponential Distribution to be a better approximation,
     * because of the nature of the gossip channel and its impact on latency
     * 
     * @see com.hellblazer.gossip.FailureDetector#shouldConvict(long)
     */
    @Override
    public boolean shouldConvict(long now) {
        final ReentrantLock myLock = stateLock;
        try {
            myLock.lockInterruptibly();
        } catch (InterruptedException e) {
            return false;
        }
        try {
            if (window.size() == 0) {
                return false;
            }
            double delta = now - last;
            double phi = -1
                         * Math.log10(Math.pow(Math.E,
                                               -1 * delta / window.value()));
            boolean shouldConvict = phi > threshold;
            /*
            if (shouldConvict) {
                System.out.println("delta: " + delta);
            }
            */
            return shouldConvict;
        } finally {
            myLock.unlock();
        }
    }
}
