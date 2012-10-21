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
 * A simple ring buffer for storing windows of samples of multiple variables.
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public class MultiWindow {

    protected int              count = 0;
    private int                head  = 0;
    protected final double[][] samples;
    private int                tail  = 0;

    public MultiWindow(int windowSize, int numVariables) {
        samples = new double[windowSize][numVariables];
    }

    public void addLast(double... value) {
        samples[tail] = value;
        tail = (tail + 1) % samples.length;
        count++;
    }

    public double[] removeFirst() {
        double[] items = samples[head];
        count--;
        head = (head + 1) % samples.length;
        return items;
    }

    public int size() {
        return count;
    }

    @Override
    public String toString() {
        final StringBuilder buf = new StringBuilder();
        buf.append("[ ");
        for (int i = 0; i < count; i++) {
            buf.append(samples[(i + head) % samples.length]);
            buf.append(", ");
        }
        buf.append("]");
        return buf.toString();
    }
}
