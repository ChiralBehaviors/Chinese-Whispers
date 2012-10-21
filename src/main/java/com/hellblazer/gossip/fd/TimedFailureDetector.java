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

/**
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public class TimedFailureDetector implements FailureDetector {
    private volatile long last = -1;
    private final long    maxInterval;

    public TimedFailureDetector(long maxInterval) {
        this.maxInterval = maxInterval;
    }

    @Override
    public void record(long now, long delay) {
        last = now;
    }

    @Override
    public boolean shouldConvict(long now) {
        if (last < 0) {
            return false;
        }
        return now - last > maxInterval;
    }

}
