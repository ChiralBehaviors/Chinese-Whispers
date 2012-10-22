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

import static java.util.Arrays.asList;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.mockito.internal.verification.Times;

public class GossipTest extends TestCase {

    public void testApplyDiscover() throws Exception {
        GossipCommunications communications = mock(GossipCommunications.class);
        FailureDetectorFactory fdFactory = mock(FailureDetectorFactory.class);
        SystemView view = mock(SystemView.class);
        Random random = mock(Random.class);
        InetSocketAddress localAddress = new InetSocketAddress("127.0.0.1", 0);
        when(view.getLocalAddress()).thenReturn(localAddress);
        when(communications.getLocalAddress()).thenReturn(localAddress);
        GossipListener receiver = mock(GossipListener.class);

        InetSocketAddress address1 = new InetSocketAddress("127.0.0.1", 1);
        InetSocketAddress address2 = new InetSocketAddress("127.0.0.1", 2);
        InetSocketAddress address3 = new InetSocketAddress("127.0.0.1", 3);
        InetSocketAddress address4 = new InetSocketAddress("127.0.0.1", 4);

        ReplicatedState state1 = new ReplicatedState(address1,
                                                     new UUID(666, 1),
                                                     new byte[0]);
        ReplicatedState state2 = new ReplicatedState(address2,
                                                     new UUID(666, 2),
                                                     new byte[0]);
        ReplicatedState state3 = new ReplicatedState(address3,
                                                     new UUID(666, 3),
                                                     new byte[0]);
        ReplicatedState state4 = new ReplicatedState(address4,
                                                     new UUID(666, 4),
                                                     new byte[0]);

        Gossip gossip = new Gossip(receiver, communications, view, fdFactory,
                                   random, 4, TimeUnit.DAYS);

        gossip.update(state1);
        gossip.update(state2);
        gossip.update(state3);
        gossip.update(state4);

        verify(communications).connect(eq(address1), isA(Endpoint.class),
                                       isA(Runnable.class));
        verify(communications).connect(eq(address2), isA(Endpoint.class),
                                       isA(Runnable.class));
        verify(communications).connect(eq(address3), isA(Endpoint.class),
                                       isA(Runnable.class));
        verify(communications).connect(eq(address4), isA(Endpoint.class),
                                       isA(Runnable.class));

        verify(communications).setGossip(gossip);

        verifyNoMoreInteractions(communications);
    }

    public void testApplyUpdate() throws Exception {
        GossipCommunications communications = mock(GossipCommunications.class);
        final GossipListener receiver = mock(GossipListener.class);
        FailureDetectorFactory fdFactory = mock(FailureDetectorFactory.class);
        SystemView view = mock(SystemView.class);
        Random random = mock(Random.class);
        InetSocketAddress localAddress = new InetSocketAddress("127.0.0.1", 0);
        when(view.getLocalAddress()).thenReturn(localAddress);
        when(communications.getLocalAddress()).thenReturn(localAddress);

        Endpoint ep1 = mock(Endpoint.class);
        Endpoint ep2 = mock(Endpoint.class);
        Endpoint ep3 = mock(Endpoint.class);
        Endpoint ep4 = mock(Endpoint.class);

        InetSocketAddress address1 = new InetSocketAddress("127.0.0.1", 1);
        InetSocketAddress address2 = new InetSocketAddress("127.0.0.1", 2);
        InetSocketAddress address3 = new InetSocketAddress("127.0.0.1", 3);
        InetSocketAddress address4 = new InetSocketAddress("127.0.0.1", 4);

        ReplicatedState state1 = new ReplicatedState(address1,
                                                     new UUID(666, 1),
                                                     new byte[] { 1 });
        state1.setTime(1);

        ReplicatedState state2 = new ReplicatedState(address2,
                                                     new UUID(666, 2),
                                                     new byte[] { 2 });
        state2.setTime(1);

        ReplicatedState state3 = new ReplicatedState(address3,
                                                     new UUID(666, 3),
                                                     new byte[] { 3 });
        state3.setTime(3);

        ReplicatedState state4 = new ReplicatedState(address4,
                                                     new UUID(666, 4),
                                                     new byte[] { 4 });
        state4.setTime(1);

        when(ep1.getTime()).thenReturn(0L);
        when(ep1.getState()).thenReturn(state1);

        when(ep2.getTime()).thenReturn(1L);

        when(ep3.getTime()).thenReturn(0L);
        when(ep3.getState()).thenReturn(state3);

        when(ep4.getTime()).thenReturn(5L);

        Gossip gossip = new Gossip(receiver, communications, view, fdFactory,
                                   random, 4, TimeUnit.DAYS) {

            @Override
            protected void notifyUpdate(ReplicatedState state) {
                receiver.update(state.getId(), state.getState());
            }
        };

        Field ep = Gossip.class.getDeclaredField("endpoints");
        ep.setAccessible(true);

        @SuppressWarnings("unchecked")
        ConcurrentMap<InetSocketAddress, Endpoint> endpoints = (ConcurrentMap<InetSocketAddress, Endpoint>) ep.get(gossip);

        endpoints.put(address1, ep1);
        endpoints.put(address2, ep2);
        endpoints.put(address3, ep3);
        endpoints.put(address4, ep4);

        gossip.update(state1);
        gossip.update(state2);
        gossip.update(state3);
        gossip.update(state4);

        verify(ep1, new Times(2)).getTime();
        verify(ep1).record(state1);
        verify(ep1).getState();
        verifyNoMoreInteractions(ep1);

        verify(ep2).getTime();
        verifyNoMoreInteractions(ep2);

        verify(ep3, new Times(2)).getTime();
        verify(ep3).record(state3);
        verify(ep3).getState();
        verifyNoMoreInteractions(ep3);

        verify(ep4).getTime();
        verifyNoMoreInteractions(ep4);

        verify(communications).setGossip(gossip);

        verify(receiver).update(state1.getId(), state1.getState());
        verify(receiver).update(state3.getId(), state3.getState());
        verifyNoMoreInteractions(communications);
    }

    public void testExamineAllNew() throws Exception {
        GossipListener listener = mock(GossipListener.class);
        GossipCommunications communications = mock(GossipCommunications.class);
        GossipMessages gossipHandler = mock(GossipMessages.class);
        FailureDetectorFactory fdFactory = mock(FailureDetectorFactory.class);
        SystemView view = mock(SystemView.class);
        Random random = mock(Random.class);
        InetSocketAddress localAddress = new InetSocketAddress("127.0.0.1", 0);
        when(view.getLocalAddress()).thenReturn(localAddress);

        Digest digest1 = new Digest(new InetSocketAddress("google.com", 1), 3);
        Digest digest2 = new Digest(new InetSocketAddress("google.com", 2), 1);
        Digest digest3 = new Digest(new InetSocketAddress("google.com", 3), 1);
        Digest digest4 = new Digest(new InetSocketAddress("google.com", 4), 3);
        Digest digest1a = new Digest(new InetSocketAddress("google.com", 1), -1);
        Digest digest2a = new Digest(new InetSocketAddress("google.com", 2), -1);
        Digest digest3a = new Digest(new InetSocketAddress("google.com", 3), -1);
        Digest digest4a = new Digest(new InetSocketAddress("google.com", 4), -1);

        Gossip gossip = new Gossip(listener, communications, view, fdFactory,
                                   random, 4, TimeUnit.DAYS);

        gossip.examine(asList(digest1, digest2, digest3, digest4),
                       gossipHandler);

        verify(gossipHandler).reply(asList(digest1a, digest2a, digest3a,
                                           digest4a),
                                    new ArrayList<ReplicatedState>());
        verifyNoMoreInteractions(gossipHandler);
    }

    public void testExamineMixed() throws Exception {
        GossipListener listener = mock(GossipListener.class);
        GossipCommunications communications = mock(GossipCommunications.class);
        GossipMessages gossipHandler = mock(GossipMessages.class);
        FailureDetectorFactory fdFactory = mock(FailureDetectorFactory.class);
        FailureDetector fd = mock(FailureDetector.class);
        SystemView view = mock(SystemView.class);
        Random random = mock(Random.class);
        InetSocketAddress localAddress = new InetSocketAddress("127.0.0.1", 0);
        when(view.getLocalAddress()).thenReturn(localAddress);

        InetSocketAddress address1 = new InetSocketAddress("127.0.0.1", 1);
        InetSocketAddress address2 = new InetSocketAddress("127.0.0.1", 2);
        InetSocketAddress address3 = new InetSocketAddress("127.0.0.1", 3);
        InetSocketAddress address4 = new InetSocketAddress("127.0.0.1", 4);

        Digest digest1 = new Digest(address1, 2);
        Digest digest2 = new Digest(address2, 1);
        Digest digest3 = new Digest(address3, 4);
        Digest digest4 = new Digest(address4, 3);

        Digest digest1a = new Digest(address1, 1);
        Digest digest3a = new Digest(address3, 3);

        ReplicatedState state1 = new ReplicatedState(address1,
                                                     new UUID(666, 1),
                                                     new byte[0]);
        state1.setTime(1);

        ReplicatedState state2 = new ReplicatedState(address2,
                                                     new UUID(666, 2),
                                                     new byte[0]);
        state2.setTime(2);

        ReplicatedState state3 = new ReplicatedState(address3,
                                                     new UUID(666, 3),
                                                     new byte[0]);
        state3.setTime(3);

        ReplicatedState state4 = new ReplicatedState(address4,
                                                     new UUID(666, 4),
                                                     new byte[0]);
        state4.setTime(4);

        Gossip gossip = new Gossip(listener, communications, view, fdFactory,
                                   random, 4, TimeUnit.DAYS);

        Field ep = Gossip.class.getDeclaredField("endpoints");
        ep.setAccessible(true);

        @SuppressWarnings("unchecked")
        ConcurrentMap<InetSocketAddress, Endpoint> endpoints = (ConcurrentMap<InetSocketAddress, Endpoint>) ep.get(gossip);

        endpoints.put(address1, new Endpoint(address1, state1, fd));
        endpoints.put(address2, new Endpoint(address2, state2, fd));
        endpoints.put(address3, new Endpoint(address3, state3, fd));
        endpoints.put(address4, new Endpoint(address4, state4, fd));

        gossip.examine(asList(digest1, digest2, digest3, digest4),
                       gossipHandler);
        verify(gossipHandler).reply(asList(digest1a, digest3a),
                                    asList(state2, state4));
        verifyNoMoreInteractions(gossipHandler);
    }
}
