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

import java.util.Comparator;

public interface SortedCollection<E> extends Iterable<E> {
    void add(E element);

    void addAll(SortedCollection<? extends E> col);

    Comparator<? super E> comparator();

    boolean contains(E element);

    E find(E element);

    E first();

    boolean isEmpty();

    E last();

    void remove(E element);

    int size();

    Iterable<E> subset(E from, E to);

}