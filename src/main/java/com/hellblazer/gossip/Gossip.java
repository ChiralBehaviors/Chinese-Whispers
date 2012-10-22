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

import static java.lang.String.format;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.gossip.Digest.DigestComparator;

/**
 * The embodiment of the gossip protocol. This protocol replicates state and
 * forms both a member discovery and failure detection service. Periodically,
 * the protocol chooses a random member from the system view and initiates a
 * round of gossip with it. A round of gossip is push/pull and involves 3
 * messages. For example, if node A wants to initiate a round of gossip with
 * node B it starts off by sending node B a gossip message containing a digest
 * of the view number state of the local view of the replicated state. Node B on
 * receipt of this message sends node A a reply containing a list of digests
 * representing the updated state required, based on the received digests. In
 * addition, the node also sends along a list of updated state that is more
 * recent, based on the initial list of digests. On receipt of this message node
 * A sends node B the requested state that completes a round of gossip. When
 * messages are received, the protocol updates the endpoint's failure detector
 * with the liveness information. If the endpoint's failure detector predicts
 * that the endpoint has failed, the endpoint is marked dead and its replicated
 * state is abandoned.
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public class Gossip {
    private final static Logger                              log        = LoggerFactory.getLogger(Gossip.class);

    private final GossipCommunications                       communications;
    private final Executor                                   dispatcher;
    private final ConcurrentMap<InetSocketAddress, Endpoint> endpoints  = new ConcurrentHashMap<InetSocketAddress, Endpoint>();
    private final Random                                     entropy;
    private final FailureDetectorFactory                     fdFactory;
    private ScheduledFuture<?>                               gossipTask;
    private final UUID                                       id;
    private final int                                        interval;
    private final TimeUnit                                   intervalUnit;
    private final GossipListener                             listener;
    private final AtomicReference<ReplicatedState>           localState = new AtomicReference<ReplicatedState>();
    private final Ring                                       ring;
    private final AtomicBoolean                              running    = new AtomicBoolean();
    private final ScheduledExecutorService                   scheduler;
    private final Set<UUID>                                  theShunned = new ConcurrentSkipListSet<UUID>();
    private final SystemView                                 view;

    /**
     * 
     * @param identity
     *            - the unique identity of this gossip node
     * @param stateListener
     *            - the ultimate listener of available gossip
     * @param systemView
     *            - the system management view of the member state
     * @param failureDetectorFactory
     *            - the factory producing instances of the failure detector
     * @param random
     *            - a source of entropy
     * @param gossipInterval
     *            - the period of the random gossiping
     * @param unit
     *            - time unit for the gossip interval
     */
    public Gossip(UUID identity, GossipListener stateListener,
                  GossipCommunications communicationsService,
                  SystemView systemView,
                  FailureDetectorFactory failureDetectorFactory, Random random,
                  int gossipInterval, TimeUnit unit) {
        communications = communicationsService;
        communications.setGossip(this);
        listener = stateListener;
        entropy = random;
        view = systemView;
        interval = gossipInterval;
        intervalUnit = unit;
        fdFactory = failureDetectorFactory;
        id = identity;
        ring = new Ring(id, communications);
        scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                int count = 0;
                Thread daemon = new Thread(r, "Gossip servicing thread "
                                              + count++);
                daemon.setDaemon(true);
                daemon.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        log.warn("Uncaught exception on the gossip servicing thread",
                                 e);
                    }
                });
                return daemon;
            }
        });
        dispatcher = Executors.newSingleThreadExecutor(new ThreadFactory() {
            volatile int count = 0;

            @Override
            public Thread newThread(Runnable r) {
                Thread daemon = new Thread(r, "Gossip dispatching thread "
                                              + count++);
                daemon.setDaemon(true);
                daemon.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        log.warn("Uncaught exception on the gossip dispatching thread",
                                 e);
                    }
                });
                return daemon;
            }
        });
    }

    public InetSocketAddress getLocalAddress() {
        return view.getLocalAddress();
    }

    public boolean isIgnoring(InetSocketAddress address) {
        Endpoint endpoint = endpoints.get(address);
        if (endpoint == null || endpoint.getState() == null
            || endpoint.getState().getId() == null) {
            return false;
        }
        return isIgnoring(endpoint.getState().getId());
    }

    public boolean isIgnoring(UUID id) {
        return theShunned.contains(id);
    }

    public void setIgnoring(Collection<UUID> ignoringUpdate) {
        theShunned.clear();
        theShunned.addAll(ignoringUpdate);
    }

    public void start(byte[] initialState) {
        if (running.compareAndSet(false, true)) {
            ReplicatedState state = new ReplicatedState(view.getLocalAddress(),
                                                        id, initialState);
            state.setTime(System.currentTimeMillis());
            localState.set(state);
            communications.start();
            gossipTask = scheduler.scheduleWithFixedDelay(gossipTask(),
                                                          interval, interval,
                                                          intervalUnit);
        }
    }

    public void terminate() {
        if (running.compareAndSet(true, false)) {
            communications.terminate();
            scheduler.shutdownNow();
            gossipTask.cancel(true);
            gossipTask = null;
        }
    }

    public void updateLocalState(byte[] replicatedState) {
        ReplicatedState state = new ReplicatedState(view.getLocalAddress(), id,
                                                    replicatedState);
        state.setTime(System.currentTimeMillis());
        localState.set(state);
        if (log.isDebugEnabled()) {
            log.debug(String.format("Member: %s updating replicated state",
                                    getId()));
        }
        ring.update(endpoints.values());
        ring.send(state);
    }

    protected void addUpdatedState(List<ReplicatedState> deltaState,
                                   InetSocketAddress endpoint, long time) {
        Endpoint state = endpoints.get(endpoint);
        if (state != null && state.getTime() > time) {
            if (log.isTraceEnabled()) {
                log.trace(format("local time stamp %s greater than %s for %s ",
                                 state.getTime(), time, endpoint));
            }
            deltaState.add(state.getState());
        } else {
            if (view.getLocalAddress().equals(endpoint)
                && localState.get().getTime() > time) {
                deltaState.add(localState.get());
            }
        }
    }

    protected void apply(List<ReplicatedState> list) {
        for (ReplicatedState remoteState : list) {
            InetSocketAddress endpoint = remoteState.getAddress();
            if (endpoint == null) {
                if (log.isDebugEnabled()) {
                    log.debug(String.format("endpoint address is null: "
                                            + remoteState));
                }
                continue;
            }
            if (view.isQuarantined(endpoint)) {
                if (log.isDebugEnabled()) {
                    log.debug(format("Ignoring gossip for %s because it is a quarantined endpoint",
                                     remoteState));
                }
                continue;
            }
            Endpoint local = endpoints.get(endpoint);
            if (local != null) {
                if (remoteState.getTime() > local.getTime()) {
                    long oldTime = local.getTime();
                    local.record(remoteState);
                    notifyUpdate(local.getState());
                    if (log.isTraceEnabled()) {
                        log.trace(format("Updating state time stamp to %s from %s for %s",
                                         local.getTime(), oldTime, endpoint));
                    }
                }
            } else {
                discover(remoteState);
            }
        }
    }

    protected void checkStatus() {
        long now = System.currentTimeMillis();
        if (log.isTraceEnabled()) {
            log.trace("Checking the status of the living...");
        }
        for (Iterator<Entry<InetSocketAddress, Endpoint>> iterator = endpoints.entrySet().iterator(); iterator.hasNext();) {
            Entry<InetSocketAddress, Endpoint> entry = iterator.next();
            InetSocketAddress address = entry.getKey();
            if (address.equals(view.getLocalAddress())) {
                continue;
            }

            Endpoint endpoint = entry.getValue();
            if (endpoint.isAlive() && endpoint.shouldConvict(now)) {
                iterator.remove();
                endpoint.markDead();
                view.markDead(address, now);
                if (log.isDebugEnabled()) {
                    log.debug(format("Endpoint %s is now DEAD on node: %s",
                                     endpoint.getState().getId(),
                                     localState.get().getId()));
                }
                notifyAbandon(endpoint.getState());
            }
        }

        if (log.isTraceEnabled()) {
            log.trace("Culling the quarantined...");
        }
        view.cullQuarantined(now);

        if (log.isTraceEnabled()) {
            log.trace("Culling the unreachable...");
        }
        view.cullUnreachable(now);
    }

    /**
     * Connect with a member
     * 
     * @param address
     *            - the address of the member
     * @param endpoint
     *            - the endpoint representing the member
     * @param connectAction
     *            - the action to take when the connection with the member is
     *            established
     */
    protected void connect(final InetSocketAddress address,
                           final Endpoint endpoint, Runnable connectAction) {
        try {
            communications.connect(address, endpoint, connectAction);
        } catch (IOException e) {
            if (log.isDebugEnabled()) {
                log.debug(format("Cannot connect to endpoint %s", address), e);
            }
        }
    }

    /**
     * Connect and gossip with a member that isn't currently connected. As we
     * have no idea what state this member is in, we need to add a digest to the
     * list that is manifestly out of date so that the member, if it responds,
     * will update us with its state.
     * 
     * @param address
     *            - the address to connect to
     * @param digests
     *            - the digests in question
     */
    protected void connectAndGossipWith(final InetSocketAddress address,
                                        final List<Digest> digests) {
        final Endpoint newEndpoint = new Endpoint(new ReplicatedState(address),
                                                  fdFactory.create());
        Runnable connectAction = new Runnable() {
            @Override
            public void run() {
                Endpoint previous = endpoints.putIfAbsent(address, newEndpoint);
                if (previous != null) {
                    newEndpoint.getHandler().close();
                    if (log.isDebugEnabled()) {
                        log.debug(format("Endpoint already established for %s",
                                         newEndpoint.getState().getId()));
                    }
                    return;
                }
                view.markAlive(address);
                if (log.isDebugEnabled()) {
                    log.debug(format("Member %s is now CONNECTED",
                                     newEndpoint.getState().getId()));
                }
                List<Digest> newDigests = new ArrayList<Digest>(digests);
                newDigests.add(new Digest(address, -1));
                newEndpoint.getHandler().gossip(newDigests);
            }
        };
        connect(address, newEndpoint, connectAction);
    }

    /**
     * Discover a connection with a previously unconnected member
     * 
     * @param remoteState
     *            - the state from a previously unconnected member of the system
     *            view
     */
    protected void discover(final ReplicatedState remoteState) {
        final InetSocketAddress address = remoteState.getAddress();
        if (view.getLocalAddress().equals(address)) {
            return; // it's our state, dummy
        }
        final Endpoint endpoint = new Endpoint(remoteState, fdFactory.create());
        Runnable connectAction = new Runnable() {
            @Override
            public void run() {
                Endpoint previous = endpoints.putIfAbsent(address, endpoint);
                if (previous != null) {
                    endpoint.getHandler().close();
                    if (log.isDebugEnabled()) {
                        log.debug(format("Endpoint already established for %s",
                                         endpoint.getState().getId()));
                    }
                    return;
                }
                view.markAlive(address);
                if (log.isDebugEnabled()) {
                    log.debug(format("Member %s is now UP",
                                     endpoint.getState().getId()));
                }
                notifyDiscover(endpoint.getState());
            }

        };
        connect(address, endpoint, connectAction);
    }

    protected void examine(List<Digest> digests, GossipMessages gossipHandler) {
        if (log.isTraceEnabled()) {
            log.trace(String.format("Member: %s receiving gossip digests: %s",
                                    getId(), digests));
        }
        List<Digest> deltaDigests = new ArrayList<Digest>();
        List<ReplicatedState> deltaState = new ArrayList<ReplicatedState>();
        for (Digest digest : digests) {
            long remoteTime = digest.getTime();
            Endpoint state = endpoints.get(digest.getAddress());
            if (state != null) {
                long localTime = state.getTime();
                if (remoteTime == localTime) {
                    continue;
                }
                if (remoteTime > localTime) {
                    deltaDigests.add(new Digest(digest.getAddress(), localTime));
                } else if (remoteTime < localTime) {
                    addUpdatedState(deltaState, digest.getAddress(), remoteTime);
                }
            } else {
                if (view.getLocalAddress().equals(digest.getAddress())) {
                    addUpdatedState(deltaState, digest.getAddress(), remoteTime);
                } else {
                    deltaDigests.add(new Digest(digest.getAddress(), -1));
                }
            }
        }
        if (!deltaDigests.isEmpty() || !deltaState.isEmpty()) {
            if (log.isTraceEnabled()) {
                log.trace(String.format("Member: %s replying with digests: %s state: %s",
                                        getId(), deltaDigests, deltaState));
            }
            gossipHandler.reply(deltaDigests, deltaState);
        } else {
            if (log.isTraceEnabled()) {
                log.trace(String.format("Member: %s no state to send", getId()));
            }
        }
    }

    protected UUID getId() {
        return localState.get().getId();
    }

    /**
     * Perform the periodic gossip.
     * 
     * @param communications
     *            - the mechanism to send the gossip message to a peer
     */
    protected void gossip() {
        List<Digest> digests = randomDigests();
        if (digests.size() > 0) {
            InetSocketAddress member = gossipWithTheLiving(digests);
            gossipWithTheDead(digests);
            gossipWithSeeds(digests, member);
        }
        checkStatus();
    }

    /**
     * The first message of the gossip protocol. The gossiping node sends a set
     * of digests of it's view of the replicated state. The receiver replies
     * with a list of digests indicating the state that needs to be updated on
     * the receiver. The receiver of the gossip also sends along any states
     * which are more recent than what the gossiper sent, based on the digests
     * provided by the gossiper.
     * 
     * @param digests
     *            - the list of replicated state digests
     * @param gossipHandler
     *            - the handler to send the reply of digests and states
     */
    protected void gossip(List<Digest> digests, GossipMessages gossipHandler) {
        sort(digests);
        examine(digests, gossipHandler);
    }

    protected Runnable gossipTask() {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    gossip();
                } catch (Throwable e) {
                    log.warn("Exception while performing gossip", e);
                }
            }
        };
    }

    /**
     * Gossip with one of the kernel members of the system view with some
     * probability. If the live member that we gossiped with is a seed member,
     * then don't worry about it.
     * 
     * @param digests
     *            - the digests to gossip.
     * @param member
     *            - the live member we've gossiped with.
     */
    protected void gossipWithSeeds(final List<Digest> digests,
                                   InetSocketAddress member) {
        InetSocketAddress address = view.getRandomSeedMember(member);
        if (address == null) {
            return;
        }
        Endpoint endpoint = endpoints.get(address);
        if (endpoint != null) {
            endpoint.getHandler().gossip(digests);
        } else {
            connectAndGossipWith(address, digests);
        }
    }

    /**
     * Gossip with a member who is currently considered dead, with some
     * probability.
     * 
     * @param digests
     *            - the digests of interest
     */
    protected void gossipWithTheDead(List<Digest> digests) {
        InetSocketAddress address = view.getRandomUnreachableMember();
        if (address == null) {
            return;
        }
        connectAndGossipWith(address, digests);
    }

    /**
     * Gossip with a live member of the view.
     * 
     * @param digests
     *            - the digests of interest
     * @return the address of the member contacted
     */
    protected InetSocketAddress gossipWithTheLiving(List<Digest> digests) {
        InetSocketAddress address = view.getRandomLiveMember();
        if (address == null) {
            return null;
        }
        Endpoint endpoint = endpoints.get(address);
        if (endpoint != null) {
            if (log.isTraceEnabled()) {
                log.trace(format("%s gossiping with: %s, #digests: %s",
                                 getId(), endpoint.getState().getId(),
                                 digests.size()));
            }
            endpoint.getHandler().gossip(digests);
            return address;
        }
        if (log.isWarnEnabled()) {
            log.warn(format("Inconsistent state!  View thinks %s is alive, but service has no endpoint!",
                            address));
        }
        view.markDead(address, System.currentTimeMillis());
        return null;
    }

    /**
     * @param state
     */
    protected void notifyAbandon(final ReplicatedState state) {
        assert state != null;
        if (log.isDebugEnabled()) {
            log.debug(String.format("Member: %s notifying abandonment of: %s",
                                    getId(), state));
        }
        dispatcher.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    listener.abandon(state.getState());
                } catch (Throwable e) {
                    log.warn(String.format("exception notifying listener of abandonmen of state %s",
                                           state.getAddress()));
                }
            }
        });
        ring.send(state);
    }

    /**
     * @param state
     */
    protected void notifyDiscover(final ReplicatedState state) {
        assert state != null;
        if (isIgnoring(state.getId())) {
            if (log.isDebugEnabled()) {
                log.debug(String.format("Member: %s discarding notification of: %s",
                                        getId(), state));
            }
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("Member: %s notifying discovery of: %s",
                                    getId(), state));
        }
        dispatcher.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    listener.discover(state.getState());
                } catch (Throwable e) {
                    log.warn(String.format("exception notifying listener of discovery of state %s",
                                           state.getAddress()));
                }
            }
        });
        ring.send(state);
    }

    protected void notifyUpdate(final ReplicatedState state) {
        assert state != null;
        if (isIgnoring(state.getId())) {
            if (log.isDebugEnabled()) {
                log.debug(String.format("Member: %s discarding notification of: %s",
                                        getId(), state));
            }
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("Member: %s notifying update of: %s",
                                    getId(), state));
        }
        dispatcher.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    listener.update(state.getState());
                } catch (Throwable e) {
                    log.warn(String.format("exception notifying listener of update of state %s",
                                           state.getAddress()));
                }
            }
        });
        ring.send(state);
    }

    protected List<Digest> randomDigests() {
        ArrayList<Digest> digests = new ArrayList<Digest>(endpoints.size() + 1);
        for (Entry<InetSocketAddress, Endpoint> entry : endpoints.entrySet()) {
            digests.add(new Digest(entry.getKey(), entry.getValue()));
        }
        digests.add(new Digest(localState.get()));
        Collections.shuffle(digests, entropy);
        if (log.isTraceEnabled()) {
            log.trace(format("Gossip digests are : %s", digests));
        }
        return digests;
    }

    /**
     * The second message in the gossip protocol. This message is sent in reply
     * to the initial gossip message sent by this node. The response is a list
     * of digests that represent the replicated state that is out of date on the
     * sender. In addition, the sender also supplies replicated state that is
     * more recent than the digests supplied in the initial gossip message.
     * 
     * @param digests
     *            - the list of digests the gossiper would like to hear about
     * @param list
     *            - the list of replicated states the gossiper thinks is out of
     *            date on the receiver
     * @param gossipHandler
     *            - the handler to send a list of replicated states that the
     *            gossiper would like updates for
     */
    protected void reply(List<Digest> digests, List<ReplicatedState> list,
                         GossipMessages gossipHandler) {
        if (log.isTraceEnabled()) {
            log.trace(String.format("Member: %s receiving reply digests: %s states: %s",
                                    getId(), digests, list));
        }
        apply(list);

        @SuppressWarnings({ "unchecked", "rawtypes" })
        List<ReplicatedState> deltaState = new ArrayList();
        for (Digest digest : digests) {
            InetSocketAddress addr = digest.getAddress();
            addUpdatedState(deltaState, addr, digest.getTime());
        }
        if (!deltaState.isEmpty()) {
            if (log.isTraceEnabled()) {
                log.trace(String.format("Member: %s sending update states: %s",
                                        getId(), deltaState));
            }
            gossipHandler.update(deltaState);
        }
    }

    protected void sort(List<Digest> digests) {
        Map<InetSocketAddress, Digest> endpoint2digest = new HashMap<InetSocketAddress, Digest>();
        for (Digest digest : digests) {
            endpoint2digest.put(digest.getAddress(), digest);
        }

        Digest[] diffDigests = new Digest[digests.size()];
        int i = 0;
        for (Digest gDigest : digests) {
            InetSocketAddress ep = gDigest.getAddress();
            Endpoint state = endpoints.get(ep);
            long time = state != null ? state.getTime() : -1;
            long diffTime = Math.abs(time - gDigest.getTime());
            diffDigests[i++] = new Digest(ep, diffTime);
        }

        Arrays.sort(diffDigests, new DigestComparator());
        i = 0;
        for (int j = diffDigests.length - 1; j >= 0; --j) {
            digests.set(i++, endpoint2digest.get(diffDigests[j].getAddress()));
        }
        if (log.isTraceEnabled()) {
            log.trace(format("Sorted gossip digests are : %s", digests));
        }
    }

    /**
     * The third message of the gossip protocol. This is the final message in
     * the gossip protocol. The supplied state is the updated state requested by
     * the receiver in response to the digests in the original gossip message.
     * 
     * @param remoteStates
     *            - the list of updated states we requested from our partner
     */
    protected void update(List<ReplicatedState> remoteStates) {
        if (log.isTraceEnabled()) {
            log.trace(String.format("Member: %s receiving update states: %s",
                                    getId(), remoteStates));
        }
        apply(remoteStates);
    }
}
