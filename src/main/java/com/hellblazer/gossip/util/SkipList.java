/*
* Copyright (C) 2010 Zhenya Leonov
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.hellblazer.gossip.util;

import java.util.Random;

public final class SkipList {

    private static class Node {
        private final int[]  dist;
        private double       element;
        private final Node[] next;
        private Node         prev;

        private Node(final double element, final int size) {
            this.element = element;
            next = new Node[size];
            dist = new int[size];
        }

        private Node next() {
            return next[0];
        }
    }

    private static final int MAX_LEVEL = 32;
    private Node             head;
    private int              level;
    private Random           random    = new Random();
    private int              size;

    public SkipList() {
        reset();
    }

    public boolean add(double e) {
        Node[] update = new Node[MAX_LEVEL];
        int[] index = new int[MAX_LEVEL];
        final int newLevel = randomLevel();
        Node x = head;
        Node y = head;
        int i;
        int idx = 0;
        for (i = level - 1; i >= 0; i--) {
            while (x.next[i] != y && x.next[i].element < e) {
                idx += x.dist[i];
                x = x.next[i];
            }
            y = x.next[i];
            update[i] = x;
            index[i] = idx;
        }
        if (newLevel > level) {
            for (i = level; i < newLevel; i++) {
                head.dist[i] = size + 1;
                update[i] = head;
            }
            level = newLevel;
        }
        x = new Node(e, newLevel);
        for (i = 0; i < level; i++) {
            if (i > newLevel - 1) {
                update[i].dist[i]++;
            } else {
                x.next[i] = update[i].next[i];
                update[i].next[i] = x;
                x.dist[i] = index[i] + update[i].dist[i] - idx;
                update[i].dist[i] = idx + 1 - index[i];

            }
        }
        x.prev = update[0];
        x.next().prev = x;
        size++;
        return true;
    }

    public boolean contains(double o) {
        return search(o) != null;
    }

    /**
     * count the number of elements that are <= the supplied value
     * 
     * @param value
     * @return
     */
    public int countLessThanEqualTo(double value) {
        Node node = head.next();
        Node last = null;
        int offset = 0;
        int index = 0;
        int count = 0;

        while (index + offset < size()) {
            index++;
            last = node;
            node = node.next();
            if (last.element <= value) {
                count++;
            } else {
                return count;
            }
        }
        return count;
    }

    public double get(int index) {
        assert index < size;
        return search(index).element;
    }

    public boolean remove(double o) {
        Node[] update = new Node[MAX_LEVEL];
        Node curr = head;
        for (int i = level - 1; i >= 0; i--) {
            while (curr.next[i] != head && curr.next[i].element < o) {
                curr = curr.next[i];
            }
            update[i] = curr;
        }
        curr = curr.next();
        if (curr == head || curr.element != o) {
            return false;
        }
        delete(curr, update);
        return true;
    }

    /**
     * reset the receiver's state
     */
    public void reset() {
        head = new Node(Double.NaN, MAX_LEVEL);
        level = 1;
        size = 0;
        for (int i = 0; i < MAX_LEVEL; i++) {
            head.next[i] = head;
            head.dist[i] = 1;
        }
        head.prev = head;
    }

    public int size() {
        return size;
    }

    private void delete(final Node node, final Node[] update) {
        for (int i = 0; i < level; i++) {
            if (update[i].next[i] == node) {
                update[i].next[i] = node.next[i];
                update[i].dist[i] += node.dist[i] - 1;
            } else {
                update[i].dist[i]--;
            }
        }
        node.next().prev = node.prev;
        while (head.next[level - 1] == head && level > 1) {
            level--;
        }
        size--;
    }

    private int randomLevel() {
        return Math.max(1, (int) ((MAX_LEVEL - 1) * random.nextDouble()));
    }

    private Node search(final double element) {
        Node curr = head;
        for (int i = level - 1; i >= 0; i--) {
            while (curr.next[i] != head && curr.next[i].element < element) {
                curr = curr.next[i];
            }
        }
        curr = curr.next();
        if (curr != head && curr.element == element) {
            return curr;
        }
        return null;
    }

    private Node search(final int index) {
        Node curr = head;
        int idx = -1;
        for (int i = level - 1; i >= 0; i--) {
            while (idx + curr.dist[i] <= index) {
                idx += curr.dist[i];
                curr = curr.next[i];
            }
        }
        return curr;
    }

}