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
import com.hellblazer.gossip.FailureDetectorFactory;

/**
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public class AdaptiveFailureDetectorFactory implements FailureDetectorFactory {
    private final double convictionThreshold;
    private final int    windowSize;
    private final long   expectedSampleInterval;
    private final int    initialSamples;
    private final double minimumInterval;
    private final double scale;

    public AdaptiveFailureDetectorFactory(double convictionThreshold,
                                          int windowSize, double scale,
                                          long expectedSampleInterval,
                                          int initialSamples,
                                          double minimumInterval) {
        this.convictionThreshold = convictionThreshold;
        this.windowSize = windowSize;
        this.expectedSampleInterval = expectedSampleInterval;
        this.initialSamples = initialSamples;
        this.minimumInterval = minimumInterval;
        this.scale = scale;
    }

    @Override
    public FailureDetector create() {
        return new AdaptiveFailureDetector(convictionThreshold, windowSize,
                                           scale, expectedSampleInterval,
                                           initialSamples, minimumInterval);
    }

}
