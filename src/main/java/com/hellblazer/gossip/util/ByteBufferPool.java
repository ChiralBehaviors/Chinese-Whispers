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

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A thread safe pool for byte buffers
 * 
 * @author hhildebrand
 * 
 */
public class ByteBufferPool {

    private int                          bytesAllocated = 0;
    private int                          created        = 0;
    private int                          discarded      = 0;
    private final ReentrantLock          lock           = new ReentrantLock();
    private final String                 name;
    private final RingBuffer<ByteBuffer> pool;
    private int                          pooled         = 0;
    private int                          reused         = 0;

    public ByteBufferPool(String name, int limit) {
        this.name = name;
        pool = new RingBuffer<ByteBuffer>(limit);
    }

    public ByteBuffer allocate(int capacity) {
        final ReentrantLock myLock = lock;
        myLock.lock();
        try {
            if (pool.isEmpty()) {
                created++;
                bytesAllocated += capacity;
                return ByteBuffer.allocate(capacity);
            }
            int remaining = pool.size();
            while (remaining != 0) {
                ByteBuffer allocated = pool.poll();
                if (allocated.capacity() >= capacity) {
                    reused++;
                    allocated.rewind();
                    allocated.limit(capacity);
                    return allocated;
                }
                pool.add(allocated);
                remaining--;
            }
            created++;
            bytesAllocated += capacity;
            return ByteBuffer.allocate(capacity);
        } finally {
            myLock.unlock();
        }
    }

    public void free(ByteBuffer free) {
        final ReentrantLock myLock = lock;
        myLock.lock();
        try {
            if (!pool.offer(free)) {
                discarded++;
            } else {
                free.clear();
                pooled++;
            }
        } finally {
            myLock.unlock();
        }
    }

    /**
     * @return the bytesAllocated
     */
    public int getBytesAllocated() {
        return bytesAllocated;
    }

    /**
     * @return the created
     */
    public int getCreated() {
        return created;
    }

    /**
     * @return the discarded
     */
    public int getDiscarded() {
        return discarded;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    public int getPooled() {
        return pooled;
    }

    /**
     * @return the reused
     */
    public int getReused() {
        return reused;
    }

    public int size() {
        return pool.size();
    }

    @Override
    public String toString() {
        return String.format("Pool[%s] bytes allocated: %s size: %s reused: %s created: %s pooled: %s discarded: %s",
                             name, bytesAllocated, size(), reused, created,
                             pooled, discarded);
    }
}
