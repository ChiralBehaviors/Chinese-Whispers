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
package com.hellblazer.gossip;

/**
 * 
 * The failure detector contract
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public interface FailureDetector {

    /**
     * Record the arrival time of a heartbeat.
     * 
     * @param now
     *            - the timestamp of the heartbeat
     * @param delay
     *            - the local delay perceived in receiving the heartbeat
     */
    public abstract void record(long now, long delay);

    /**
     * Answer true if the suspicion level of the detector has exceeded the
     * conviction threshold.
     * 
     * @param now
     *            - the the time to calculate conviction
     * @return - true if the conviction threshold has been exceeded.
     */
    public abstract boolean shouldConvict(long now);

}