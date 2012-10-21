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

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Provides a fixed size Queue implementation. This class is not thread safe.
 * 
 * @author hhildebrand
 * 
 * @param <T>
 */
public class RingBuffer<T> extends AbstractQueue<T> {

    private int         size = 0;
    private int         head = 0;
    protected final T[] items;
    private int         tail = 0;
    private int         capacity;

    @SuppressWarnings("unchecked")
    public RingBuffer(int capacity) {
        this.capacity = capacity;
        items = (T[]) new Object[capacity];
    }

    /* (non-Javadoc)
     * @see java.util.AbstractCollection#iterator()
     */
    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            int current = 0;

            @Override
            public boolean hasNext() {
                return current < size;
            }

            @Override
            public T next() {
                if (current == size) {
                    throw new NoSuchElementException();
                }
                return items[(current++ + head) % items.length];
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /* (non-Javadoc)
     * @see java.util.Queue#offer(java.lang.Object)
     */
    @Override
    public boolean offer(T value) {
        if (size == capacity) {
            return false;
        }
        items[tail] = value;
        tail = (tail + 1) % items.length;
        size++;
        return true;
    }

    /* (non-Javadoc)
     * @see java.util.Queue#peek()
     */
    @Override
    public T peek() {
        if (size == 0) {
            return null;
        }
        return items[head];
    }

    /* (non-Javadoc)
     * @see java.util.Queue#poll()
     */
    @Override
    public T poll() {
        if (size == 0) {
            return null;
        }
        T item = items[head];
        items[head] = null;
        size--;
        head = (head + 1) % items.length;
        return item;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public String toString() {
        final StringBuilder buf = new StringBuilder();
        buf.append("[ ");
        for (int i = 0; i < size; i++) {
            buf.append(items[(i + head) % items.length]);
            buf.append(", ");
        }
        buf.append("]");
        return buf.toString();
    }
}