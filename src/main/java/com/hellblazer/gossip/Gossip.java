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

import static com.hellblazer.gossip.GossipListener.MAX_STATE_SIZE;
import static java.lang.String.format;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.NoArgGenerator;

/**
 * The embodiment of the gossip protocol. This protocol replicates state and
 * forms both a member discovery and failure detection service. Periodically,
 * the protocol chooses a random member from the system view and initiates a
 * round of gossip with it.A round of gossip is push/pull and involves 3
 * messages.
 * 
 * For example, if node A wants to initiate a round of gossip with node B it
 * starts off by sending node B a gossip message containing a digest of the view
 * number state of the local view of the replicated state. Node B on receipt of
 * this message sends node A a reply containing a list of digests representing
 * the updated state required, based on the received digests. In addition, the
 * node also sends along a list of updated state that is more recent, based on
 * the initial list of digests. On receipt of this message node A sends node B
 * the requested state that completes a round of gossip.
 * 
 * When messages are received, the protocol updates the endpoint's failure
 * detector with the liveness information. If the endpoint's failure detector
 * predicts that the endpoint has failed, the endpoint is marked dead and its
 * replicated state is abandoned.
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public class Gossip {
    private static final UUID                                ALL_STATES = new UUID(
                                                                                   0L,
                                                                                   0L);

    private final static Logger                              log        = LoggerFactory.getLogger(Gossip.class);
    private final int                                        cleanupCycles;
    private final GossipCommunications                       communications;
    private final Executor                                   dispatcher;
    private final ConcurrentMap<InetSocketAddress, Endpoint> endpoints  = new ConcurrentHashMap<InetSocketAddress, Endpoint>();
    private final Random                                     entropy;
    private final FailureDetectorFactory                     fdFactory;
    private ScheduledFuture<?>                               gossipTask;
    private final NoArgGenerator                             idGenerator;
    private final int                                        interval;
    private final TimeUnit                                   intervalUnit;
    private final AtomicReference<GossipListener>            listener   = new AtomicReference<GossipListener>();
    private final Map<UUID, ReplicatedState>                 localState = new HashMap<UUID, ReplicatedState>();
    private final Ring                                       ring;
    private final AtomicBoolean                              running    = new AtomicBoolean();
    private final ScheduledExecutorService                   scheduler;
    private final SystemView                                 view;

    /**
     * 
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
    public Gossip(GossipCommunications communicationsService,
                  SystemView systemView,
                  FailureDetectorFactory failureDetectorFactory, Random random,
                  int gossipInterval, TimeUnit unit) {
        this(Generators.timeBasedGenerator(), communicationsService,
             systemView, failureDetectorFactory, random, gossipInterval, unit,
             3);
    }

    /**
     * 
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
     * @param cleanupCycles
     *            - the number of gossip cycles required to convict a failing
     *            endpoint
     */
    public Gossip(GossipCommunications communicationsService,
                  SystemView systemView,
                  FailureDetectorFactory failureDetectorFactory, Random random,
                  int gossipInterval, TimeUnit unit, int cleanupCycles) {
        this(Generators.timeBasedGenerator(), communicationsService,
             systemView, failureDetectorFactory, random, gossipInterval, unit,
             3);
    }

    /**
     * 
     * @param idGenerator
     *            - the UUID generator for state ids on this node
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
    public Gossip(NoArgGenerator idGenerator,
                  GossipCommunications communicationsService,
                  SystemView systemView,
                  FailureDetectorFactory failureDetectorFactory, Random random,
                  int gossipInterval, TimeUnit unit) {
        this(idGenerator, communicationsService, systemView,
             failureDetectorFactory, random, gossipInterval, unit, 3);
    }

    /**
     * 
     * @param idGenerator
     *            - the UUID generator for state ids on this node
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
     * @param cleanupCycles
     *            - the number of gossip cycles required to convict a failing
     *            endpoint
     */
    public Gossip(NoArgGenerator idGenerator,
                  GossipCommunications communicationsService,
                  SystemView systemView,
                  FailureDetectorFactory failureDetectorFactory, Random random,
                  int gossipInterval, TimeUnit unit, int cleanupCycles) {
        this.idGenerator = idGenerator;
        communications = communicationsService;
        communications.setGossip(this);
        entropy = random;
        view = systemView;
        interval = gossipInterval;
        intervalUnit = unit;
        fdFactory = failureDetectorFactory;
        ring = new Ring(getLocalAddress(), communications);
        this.cleanupCycles = cleanupCycles;
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

    /**
     * Deregister the replicated state of this node identified by the id
     * 
     * @param id
     */
    public void deregister(UUID id) {
        if (id == null) {
            throw new NullPointerException(
                                           "replicated state id must not be null");
        }
        ReplicatedState state = new ReplicatedState(id, new byte[0]);
        state.setTime(System.currentTimeMillis());
        synchronized (localState) {
            localState.put(id, state);
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("Member: %s abandoning replicated state",
                                    getLocalAddress()));
        }
        ring.send(new Update(getLocalAddress(), state));
    }

    public InetSocketAddress getLocalAddress() {
        return communications.getLocalAddress();
    }

    /**
     * Add an identified piece of replicated state to this node
     * 
     * @param replicatedState
     * @return the unique identifier for this state
     */
    public UUID register(byte[] replicatedState) {
        if (replicatedState == null) {
            throw new NullPointerException("replicated state must not be null");
        }
        if (replicatedState.length > MAX_STATE_SIZE) {
            throw new IllegalArgumentException(
                                               String.format("State size %s must not be > %s",
                                                             replicatedState.length,
                                                             MAX_STATE_SIZE));
        }
        UUID id = idGenerator.generate();
        ReplicatedState state = new ReplicatedState(id, replicatedState);
        synchronized (localState) {
            localState.put(id, state);
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("Member: %s registering replicated state",
                                    getLocalAddress()));
        }
        ring.send(new Update(getLocalAddress(), state));
        return id;
    }

    public void setListener(GossipListener gossipListener) {
        listener.set(gossipListener);
    }

    /**
     * Start the gossip replication process
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            communications.start();
            gossipTask = scheduler.scheduleWithFixedDelay(gossipTask(),
                                                          interval, interval,
                                                          intervalUnit);
        }
    }

    /**
     * Terminate the gossip replication process
     */
    public void terminate() {
        if (running.compareAndSet(true, false)) {
            communications.terminate();
            scheduler.shutdownNow();
            gossipTask.cancel(true);
            gossipTask = null;
        }
    }

    /**
     * Update the local state
     * 
     * @param id
     * @param replicatedState
     */
    public void update(UUID id, byte[] replicatedState) {
        if (id == null) {
            throw new NullPointerException(
                                           "replicated state id must not be null");
        }
        if (replicatedState == null) {
            throw new NullPointerException("replicated state must not be null");
        }
        if (replicatedState.length > MAX_STATE_SIZE) {
            throw new IllegalArgumentException(
                                               String.format("State size %s must not be > %s",
                                                             replicatedState.length,
                                                             MAX_STATE_SIZE));
        }
        ReplicatedState state = new ReplicatedState(id, replicatedState);
        state.setTime(System.currentTimeMillis());
        synchronized (localState) {
            localState.put(id, state);
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("Member: %s updating replicated state",
                                    getLocalAddress()));
        }
        ring.update(endpoints.values());
        ring.send(new Update(getLocalAddress(), state));
    }

    /**
     * @param deltaState
     */
    private void updateAllLocalState(List<Update> deltaState) {
        synchronized (localState) {
            for (ReplicatedState state : localState.values()) {
                deltaState.add(new Update(getLocalAddress(), state));
            }
        }
    }

    protected void addUpdatedState(List<Update> deltaState, Digest digest) {
        Endpoint endpoint = endpoints.get(digest.getAddress());
        if (endpoint != null) {
            ReplicatedState localState = endpoint.getState(digest.getId());
            if (localState != null && localState.getTime() > digest.getTime()) {
                if (log.isTraceEnabled()) {
                    log.trace(format("local time stamp %s greater than %s for %s ",
                                     localState.getTime(), digest.getTime(),
                                     digest));
                }
                deltaState.add(new Update(digest.getAddress(), localState));
            }
        } else if (getLocalAddress().equals(digest.getAddress())) {
            if (ALL_STATES.equals(digest.getId())) {
                synchronized (localState) {
                    for (ReplicatedState s : localState.values()) {
                        deltaState.add(new Update(digest.getAddress(), s));
                    }
                }
            } else {
                ReplicatedState state = null;
                synchronized (localState) {
                    state = localState.get(digest.getId());
                }
                if (state != null && state.getTime() > digest.getTime()) {
                    deltaState.add(new Update(digest.getAddress(), state));
                } else if (state == null) {
                    log.error(String.format("Looking for local %s on %s",
                                            digest, getLocalAddress()));
                }
            }
        } else {
            log.error(String.format("Looking for %s on %s", digest,
                                    getLocalAddress()));
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
            if (address.equals(getLocalAddress())) {
                continue;
            }

            Endpoint endpoint = entry.getValue();
            if (endpoint.isAlive()
                && endpoint.shouldConvict(now, cleanupCycles)) {
                iterator.remove();
                endpoint.markDead();
                view.markDead(address, now);
                if (log.isDebugEnabled()) {
                    log.debug(format("Endpoint %s is now DEAD on node: %s",
                                     endpoint.getAddress(), getLocalAddress()));
                }
                for (ReplicatedState state : endpoint.getStates()) {
                    notifyDeregister(state);
                }
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
        ring.update(endpoints.values());
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
        final Endpoint newEndpoint = new Endpoint(address, fdFactory.create());
        Runnable connectAction = new Runnable() {
            @Override
            public void run() {
                Endpoint previous = endpoints.putIfAbsent(address, newEndpoint);
                if (previous != null) {
                    newEndpoint.getHandler().close();
                    if (log.isDebugEnabled()) {
                        log.debug(format("Endpoint already established for %s",
                                         newEndpoint.getAddress()));
                    }
                    return;
                }
                view.markAlive(address);
                if (log.isDebugEnabled()) {
                    log.debug(format("Member %s is now CONNECTED",
                                     newEndpoint.getAddress()));
                }
                ring.update(endpoints.values());
                List<Digest> newDigests = new ArrayList<Digest>(digests);
                newDigests.add(new Digest(address, ALL_STATES, -1)); // We want it all, baby
                newEndpoint.getHandler().gossip(newDigests);
            }
        };
        connect(address, newEndpoint, connectAction);
    }

    /**
     * Discover a connection with a previously unconnected member
     * 
     * @param update
     *            - the state from a previously unconnected member of the system
     *            view
     */
    protected void discover(final Update update) {
        final InetSocketAddress address = update.node;
        if (getLocalAddress().equals(address)) {
            return; // it's our state, dummy
        }
        final Endpoint endpoint = new Endpoint(address, update.state,
                                               fdFactory.create());
        Runnable connectAction = new Runnable() {
            @Override
            public void run() {
                Endpoint previous = endpoints.putIfAbsent(address, endpoint);
                if (previous != null) {
                    endpoint.getHandler().close();
                    if (log.isDebugEnabled()) {
                        log.debug(format("Endpoint already established for %s",
                                         endpoint.getAddress()));
                    }
                    return;
                }
                view.markAlive(address);
                if (log.isDebugEnabled()) {
                    log.debug(format("Member %s is now UP",
                                     endpoint.getAddress()));
                }
                for (ReplicatedState state : endpoint.getStates()) {
                    notifyRegister(state);
                }
                ring.update(endpoints.values());
            }

        };
        connect(address, endpoint, connectAction);
    }

    protected void examine(Digest[] digests, GossipMessages gossipHandler) {
        if (log.isTraceEnabled()) {
            log.trace(String.format("Member: %s receiving gossip digests: %s",
                                    getLocalAddress(), digests));
        }
        List<Digest> deltaDigests = new ArrayList<Digest>();
        List<Update> deltaState = new ArrayList<Update>();
        for (Digest digest : digests) {
            if (ALL_STATES.equals(digest.getId())) {
                // They want it all, baby
                updateAllLocalState(deltaState);
            } else {
                long remoteTime = digest.getTime();
                Endpoint endpoint = endpoints.get(digest.getAddress());
                if (endpoint != null) {
                    ReplicatedState localState = endpoint.getState(digest.getId());
                    if (localState != null) {
                        long localTime = localState.getTime();
                        if (remoteTime == localTime) {
                            continue;
                        }
                        if (remoteTime > localTime) {
                            deltaDigests.add(new Digest(digest.getAddress(),
                                                        localState));
                        } else if (remoteTime < localTime) {
                            addUpdatedState(deltaState, digest);
                        }
                    } else {
                        log.error(String.format("Looking for %s on %s from %s",
                                                digest, getLocalAddress(),
                                                gossipHandler.getGossipper()));
                    }
                } else {
                    if (getLocalAddress().equals(digest.getAddress())) {
                        addUpdatedState(deltaState, digest);
                    } else {
                        deltaDigests.add(new Digest(digest.getAddress(),
                                                    digest.getId(), -1L));
                    }
                }
            }
        }
        if (!deltaDigests.isEmpty() || !deltaState.isEmpty()) {
            if (log.isTraceEnabled()) {
                log.trace(String.format("Member: %s replying with digests: %s state: %s",
                                        getLocalAddress(), deltaDigests,
                                        deltaState));
            }
            gossipHandler.reply(deltaDigests, deltaState);
        } else {
            if (log.isTraceEnabled()) {
                log.trace(String.format("Member: %s no state to send",
                                        getLocalAddress()));
            }
        }
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
    protected void gossip(Digest[] digests, GossipMessages gossipHandler) {
        Arrays.sort(digests);
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
        InetSocketAddress address = view.getRandomSeedMember(member,
                                                             endpoints.size());
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
        InetSocketAddress address = view.getRandomUnreachableMember(endpoints.size());
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
        InetSocketAddress address = view.getRandomMember(endpoints.keySet());
        if (address == null) {
            return null;
        }
        Endpoint endpoint = endpoints.get(address);
        if (endpoint != null) {
            if (log.isTraceEnabled()) {
                log.trace(format("%s gossiping with: %s, #digests: %s",
                                 getLocalAddress(), endpoint.getAddress(),
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
    protected void notifyDeregister(final ReplicatedState state) {
        assert state != null;
        if (log.isDebugEnabled()) {
            log.debug(String.format("Member: %s notifying deregistration of: %s",
                                    getLocalAddress(), state));
        }
        dispatcher.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    GossipListener gossipListener = listener.get();
                    if (gossipListener != null) {
                        gossipListener.deregister(state.getId());
                    }
                } catch (Throwable e) {
                    log.warn(String.format("exception notifying listener of deregistration of state %s",
                                           state.getId()), e);
                }
            }
        });
    }

    /**
     * @param state
     */
    protected void notifyRegister(final ReplicatedState state) {
        assert state != null;
        if (log.isDebugEnabled()) {
            log.debug(String.format("Member: %s notifying registration of: %s",
                                    getLocalAddress(), state));
        }
        dispatcher.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    GossipListener gossipListener = listener.get();
                    if (gossipListener != null) {
                        gossipListener.register(state.getId(), state.getState());
                    }
                } catch (Throwable e) {
                    log.warn(String.format("exception notifying listener of registration of state %s",
                                           state.getId()), e);
                }
            }
        });
    }

    protected void notifyUpdate(final ReplicatedState state) {
        assert state != null;
        if (log.isDebugEnabled()) {
            log.debug(String.format("Member: %s notifying update of: %s",
                                    getLocalAddress(), state));
        }
        dispatcher.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    GossipListener gossipListener = listener.get();
                    if (gossipListener != null) {
                        gossipListener.update(state.getId(), state.getState());
                    }
                } catch (Throwable e) {
                    log.warn(String.format("exception notifying listener of update of state %s",
                                           state.getId()), e);
                }
            }
        });
    }

    protected List<Digest> randomDigests() {
        ArrayList<Digest> digests = new ArrayList<Digest>(endpoints.size() + 1);
        for (Endpoint endpoint : endpoints.values()) {
            endpoint.addDigestsTo(digests);
        }
        synchronized (localState) {
            for (ReplicatedState state : localState.values()) {
                digests.add(new Digest(getLocalAddress(), state));
            }
        }
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
     * more recent than the digests supplied in the initial gossip message (sent
     * as update messages)
     * 
     * @param digests
     *            - the list of digests the gossiper would like to hear about
     * @param gossipHandler
     *            - the handler to send a list of replicated states that the
     *            gossiper would like updates for
     */
    protected void reply(Digest[] digests, GossipMessages gossipHandler) {
        if (log.isTraceEnabled()) {
            log.trace(String.format("Member: %s receiving reply digests: %s",
                                    getLocalAddress(), digests));
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        List<Update> deltaState = new ArrayList();
        for (Digest digest : digests) {
            addUpdatedState(deltaState, digest);
        }
        if (!deltaState.isEmpty()) {
            if (log.isTraceEnabled()) {
                log.trace(String.format("Member: %s sending update states: %s",
                                        getLocalAddress(), deltaState));
            }
            gossipHandler.update(deltaState);
        }
    }

    /**
     * The replicated state is being sent around the ring. If the state is
     * applied, continue sending the state around the ring
     * 
     * @param state
     * @param gossiper
     */
    protected void ringUpdate(Update state, InetSocketAddress gossiper) {
        if (update(state, gossiper)) {
            ring.send(state);
        }
    }

    /**
     * The third message of the gossip protocol. This is the final message in
     * the gossip protocol. The supplied state is the updated state requested by
     * the receiver in response to the digests in the original gossip message.
     * 
     * @param update
     *            - the updated state requested from our partner
     * @param gossiper
     * @return true if the state was applied, false otherwise
     */
    protected boolean update(Update update, InetSocketAddress gossiper) {
        if (log.isTraceEnabled()) {
            log.trace(String.format("Member: %s receiving update state: %s",
                                    getLocalAddress(), update));
        }
        if (update.node == null) {
            if (log.isDebugEnabled()) {
                log.debug(String.format("endpoint address is null: " + update));
            }
            return false;
        }
        if (view.isQuarantined(update.node)) {
            if (log.isDebugEnabled()) {
                log.debug(format("Ignoring gossip for %s because it is a quarantined endpoint",
                                 update));
            }
            return false;
        }
        Endpoint endpoint = endpoints.get(update.node);
        if (endpoint != null) {
            ReplicatedState state = endpoint.getState(update.state.getId());
            if (state == null) {
                endpoint.updateState(update.state);
                notifyRegister(update.state);
            } else {
                // TODO this logic is quite slow and unnecessary
                if (update.state.getTime() > state.getTime()) {
                    long oldTime = state.getTime();
                    endpoint.updateState(update.state);
                    notifyUpdate(state);
                    if (log.isTraceEnabled()) {
                        log.trace(format("Updating state time stamp to %s from %s for %s",
                                         state.getTime(), oldTime, update.node));
                    }
                    return true;
                }
            }
        } else {
            discover(update);
        }
        return false;
    }
}
