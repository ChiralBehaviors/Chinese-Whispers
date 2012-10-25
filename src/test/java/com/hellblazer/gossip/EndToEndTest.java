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

import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import com.hellblazer.gossip.fd.AdaptiveFailureDetectorFactory;

/**
 * Basic end to end testing
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public class EndToEndTest extends TestCase {
    private class Receiver implements GossipListener {

        private final CountDownLatch[] latches;

        Receiver(int members, int id) {
            latches = new CountDownLatch[members];
            setLatches(id);
        }

        public boolean await(int timeout, TimeUnit unit)
                                                        throws InterruptedException {
            for (CountDownLatch latche : latches) {
                if (!latche.await(timeout, unit)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void deregister(UUID id) {
            System.err.println(String.format("Sould never have abandoned state %s",
                                             id));
            deregistered.set(true);
        }

        @Override
        public void register(UUID id, byte[] state) {
            int currentCount = count.incrementAndGet();
            if (currentCount % 100 == 0) {
                System.out.print('.');
            } else if (currentCount % 1000 == 0) {
                System.out.println();
            }

            ByteBuffer buffer = ByteBuffer.wrap(state);
            latches[buffer.getInt()].countDown();
        }

        @Override
        public void update(UUID id, byte[] state) {
            assert state != null;
            // System.out.println("Heartbeat received: " + hb);
            int currentCount = count.incrementAndGet();
            if (currentCount % 100 == 0) {
                System.out.print('.');
            } else if (currentCount % 1000 == 0) {
                System.out.println();
            }

            ByteBuffer buffer = ByteBuffer.wrap(state);
            latches[buffer.getInt()].countDown();
        }

        void setLatches(int id) {
            for (int i = 0; i < latches.length; i++) {
                int count = i == id ? 0 : 1;
                latches[i] = new CountDownLatch(count);
            }
        }
    }

    private static final AtomicInteger count        = new AtomicInteger();

    private static final AtomicBoolean deregistered = new AtomicBoolean(false);

    private UUID[]                     stateIds;

    public void testEnd2End() throws Exception {
        int membership = 64;
        int maxSeeds = 1;
        Random entropy = new Random();
        stateIds = new UUID[membership];

        Receiver[] receivers = new Receiver[membership];
        for (int i = 0; i < membership; i++) {
            receivers[i] = new Receiver(membership, i);
        }
        List<Gossip> members = new ArrayList<Gossip>();
        Collection<InetSocketAddress> seedHosts = new ArrayList<InetSocketAddress>();
        for (int i = 0; i < membership; i++) {
            members.add(createCommunications(receivers[i], seedHosts, i));
            if (i == 0) { // always add first member
                seedHosts.add(members.get(0).getLocalAddress());
            } else if (seedHosts.size() < maxSeeds) {
                // add the new member with probability of 25%
                if (entropy.nextDouble() < 0.25D) {
                    seedHosts.add(members.get(i).getLocalAddress());
                }
            }
        }
        System.out.println("Using " + seedHosts.size() + " seed hosts");
        try {
            int id = 0;
            for (Gossip member : members) {
                byte[] state = new byte[4];
                ByteBuffer buffer = ByteBuffer.wrap(state);
                buffer.putInt(id);
                member.start();
                stateIds[id] = member.register(state);
                id++;
            }
            for (int i = 0; i < membership; i++) {
                assertTrue("did not receive all notifications for initial iteration",
                           receivers[i].await(20, TimeUnit.SECONDS));
            }
            System.out.println();
            System.out.println("Initial iteration completed");
            for (int i = 1; i < 5; i++) {
                updateAndAwait(i, membership, receivers, members);
                System.out.println();
                System.out.println("Iteration " + (i + 1) + " completed");
            }
        } finally {
            System.out.println();
            for (Gossip member : members) {
                member.terminate();
            }
        }
        assertFalse("state was deregistered", deregistered.get());
    }

    protected Gossip createCommunications(GossipListener receiver,
                                          Collection<InetSocketAddress> seedHosts,
                                          int i) throws SocketException {
        ThreadFactory threadFactory = new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        e.printStackTrace();
                    }
                });
                return t;
            }
        };
        UdpCommunications communications = new UdpCommunications(
                                                                 new InetSocketAddress(
                                                                                       "127.0.0.1",
                                                                                       0),
                                                                 Executors.newCachedThreadPool(threadFactory));

        SystemView view = new SystemView(new Random(),
                                         communications.getLocalAddress(),
                                         seedHosts, 5000, 500000);
        FailureDetectorFactory fdFactory = new AdaptiveFailureDetectorFactory(
                                                                              0.9,
                                                                              100,
                                                                              0.8,
                                                                              12000,
                                                                              10,
                                                                              3000);
        Gossip gossip = new Gossip(receiver, communications, view, fdFactory,
                                   new Random(), 1, TimeUnit.SECONDS, 3);
        return gossip;
    }

    protected void updateAndAwait(int iteration, int membership,
                                  Receiver[] receivers, List<Gossip> members)
                                                                             throws InterruptedException {
        int id = 0;
        for (Receiver receiver : receivers) {
            receiver.setLatches(id++);
        }
        id = 0;
        for (Gossip member : members) {
            ByteBuffer state = ByteBuffer.wrap(new byte[4]);
            state.putInt(id);
            member.update(stateIds[id], state.array());
            id++;
        }
        for (int i = 0; i < membership; i++) {
            assertTrue("did not receive all notifications for iteration " + i,
                       receivers[i].await(20, TimeUnit.SECONDS));
        }
    }
}
