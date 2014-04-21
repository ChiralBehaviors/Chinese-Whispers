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
import com.hellblazer.utils.fd.FailureDetectorFactory;

/**
 * The embodiment of the gossip protocol. This protocol replicates state and
 * forms both a member discovery and failure detection service. Periodically,
 * the protocol chooses a random member from the system view and initiates a
 * round of gossip with it. A round of gossip is push/pull and involves 3
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
 * To ensure liveness, a special heartbeat state is maintained. This special
 * state is not part of the notification regime and is updated periodically by
 * this host.
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public class Gossip {
    public static final UUID                                 ALL_STATES              = new UUID(
                                                                                                0L,
                                                                                                0L);
    public static final UUID                                 HEARTBEAT               = new UUID(
                                                                                                0L,
                                                                                                1L);
    public static final int                                  DEFAULT_CLEANUP_CYCLES  = 4;
    public static final int                                  DEFAULT_HEARTBEAT_CYCLE = 1;
    public static final int                                  DEFAULT_REDUNDANCY      = 3;
    public final static Logger                               log                     = LoggerFactory.getLogger(Gossip.class);
    private static final byte[]                              EMPTY_STATE             = new byte[0];

    private final int                                        cleanupCycles;
    private final GossipCommunications                       communications;
    private final Executor                                   dispatcher;
    private final ConcurrentMap<InetSocketAddress, Endpoint> endpoints               = new ConcurrentHashMap<InetSocketAddress, Endpoint>();
    private final Random                                     entropy;
    private final FailureDetectorFactory                     fdFactory;
    private ScheduledFuture<?>                               gossipTask;
    private final int                                        heartbeatCycle;
    private int                                              heartbeatCounter        = 0;
    private final NoArgGenerator                             idGenerator;
    private final int                                        interval;
    private final TimeUnit                                   intervalUnit;
    private final AtomicReference<GossipListener>            listener                = new AtomicReference<GossipListener>();
    private final Map<UUID, ReplicatedState>                 localState              = new HashMap<UUID, ReplicatedState>();
    private final int                                        redundancy;
    private final Ring                                       ring;
    private final AtomicBoolean                              running                 = new AtomicBoolean();
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
             DEFAULT_CLEANUP_CYCLES, DEFAULT_HEARTBEAT_CYCLE,
             DEFAULT_REDUNDANCY);
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
             DEFAULT_CLEANUP_CYCLES, DEFAULT_HEARTBEAT_CYCLE,
             DEFAULT_REDUNDANCY);
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
             failureDetectorFactory, random, gossipInterval, unit,
             DEFAULT_CLEANUP_CYCLES, DEFAULT_HEARTBEAT_CYCLE,
             DEFAULT_REDUNDANCY);
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
     * @param heartbeatCycle
     *            = the number of gossip cycles per heartbeat
     * @param redundancy
     *            - the number of members to contact each gossip cycle
     */
    public Gossip(NoArgGenerator idGenerator,
                  GossipCommunications communicationsService,
                  SystemView systemView,
                  FailureDetectorFactory failureDetectorFactory, Random random,
                  int gossipInterval, TimeUnit unit, int cleanupCycles,
                  int heartbeatCycle, int redundancy) {
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
        this.heartbeatCycle = heartbeatCycle;
        this.redundancy = redundancy;
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
        ReplicatedState state = new ReplicatedState(id,
                                                    System.currentTimeMillis(),
                                                    EMPTY_STATE);
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
     * @return
     */
    public int getMaxStateSize() {
        return communications.getMaxStateSize();
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
        if (replicatedState.length > communications.getMaxStateSize()) {
            throw new IllegalArgumentException(
                                               String.format("State size %s must not be > %s",
                                                             replicatedState.length,
                                                             communications.getMaxStateSize()));
        }
        UUID id = idGenerator.generate();
        ReplicatedState state = new ReplicatedState(id,
                                                    System.currentTimeMillis(),
                                                    replicatedState);
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
    public Gossip start() {
        if (running.compareAndSet(false, true)) {
            communications.start();
            gossipTask = scheduler.scheduleWithFixedDelay(gossipTask(),
                                                          interval, interval,
                                                          intervalUnit);
        }
        return this;
    }

    /**
     * Terminate the gossip replication process
     */
    public Gossip terminate() {
        if (running.compareAndSet(true, false)) {
            communications.terminate();
            scheduler.shutdownNow();
            gossipTask.cancel(true);
            gossipTask = null;
        }
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return String.format("Gossip [%s]", getLocalAddress());
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
        if (replicatedState.length > communications.getMaxStateSize()) {
            throw new IllegalArgumentException(
                                               String.format("State size %s must not be > %s",
                                                             replicatedState.length,
                                                             communications.getMaxStateSize()));
        }
        ReplicatedState state = new ReplicatedState(id,
                                                    System.currentTimeMillis(),
                                                    replicatedState);
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
     * Add the local state indicated by the digetst to the list of deltaState
     * 
     * @param deltaState
     * @param digest
     */
    private void addUpdatedLocalState(List<Update> deltaState, Digest digest) {
        if (ALL_STATES.equals(digest.getId())) {
            synchronized (localState) {
                for (ReplicatedState s : localState.values()) {
                    deltaState.add(new Update(digest.getAddress(), s));
                }
            }
        } else {
            ReplicatedState myState = null;
            synchronized (localState) {
                myState = localState.get(digest.getId());
            }
            if (myState != null && myState.getTime() > digest.getTime()) {
                deltaState.add(new Update(digest.getAddress(), myState));
            } else if (myState == null) {
                log.trace(String.format("Looking for deleteded local state %s on %s",
                                        digest, getLocalAddress()));
            }
        }
    }

    /**
     * Add the updates for all the local state for this node to the
     * deltaStateList.
     * 
     * @param deltaState
     */
    private void updateAllLocalState(List<Update> deltaState) {
        synchronized (localState) {
            for (ReplicatedState state : localState.values()) {
                deltaState.add(new Update(getLocalAddress(), state));
            }
        }
    }

    /**
     * Update the heartbeat state for this node, sending it across the ring
     */
    private void updateHeartbeat() {
        if (heartbeatCounter++ % heartbeatCycle != 0) {
            return;
        }
        if (log.isTraceEnabled()) {
            log.trace(String.format("%s updating heartbeat state",
                                    getLocalAddress()));
        }
        ReplicatedState heartbeat = new ReplicatedState(
                                                        HEARTBEAT,
                                                        System.currentTimeMillis(),
                                                        EMPTY_STATE);
        synchronized (localState) {
            localState.put(HEARTBEAT, heartbeat);
        }
        ring.send(new Update(getLocalAddress(), heartbeat));
    }

    /**
     * Add the replicated state we maintain for an endpoint indicated by the
     * digest to the list of deltaState
     * 
     * @param endpoint
     * @param deltaState
     * @param digest
     */
    protected void addUpdatedState(Endpoint endpoint, List<Update> deltaState,
                                   Digest digest) {
        ReplicatedState localCopy = endpoint.getState(digest.getId());
        if (localCopy != null && localCopy.getTime() > digest.getTime()) {
            if (log.isTraceEnabled()) {
                log.trace(format("local time stamp %s greater than %s for %s ",
                                 localCopy.getTime(), digest.getTime(), digest));
            }
            deltaState.add(new Update(digest.getAddress(), localCopy));
        }
    }

    /**
     * Add the replicated state indicated by the digest to the list of
     * deltaState
     * 
     * @param deltaState
     * @param digest
     */
    protected void addUpdatedState(List<Update> deltaState, Digest digest) {
        Endpoint endpoint = endpoints.get(digest.getAddress());
        if (endpoint != null) {
            addUpdatedState(endpoint, deltaState, digest);
        } else if (getLocalAddress().equals(digest.getAddress())) {
            addUpdatedLocalState(deltaState, digest);
        } else {
            log.trace(String.format("Looking for outdated state %s on %s",
                                    digest, getLocalAddress()));
        }
    }

    /**
     * @param gossiper
     * @param gossipingEndpoint
     */
    protected void checkConnectionStatus(final InetSocketAddress gossiper) {
        final Endpoint gossipingEndpoint = endpoints.get(gossiper);
        if (gossipingEndpoint == null) {
            discover(gossiper);
        } else {
            gossipingEndpoint.markAlive(new Runnable() {
                @Override
                public void run() {
                    if (log.isDebugEnabled()) {
                        log.debug(String.format("%s is now UP on %s (check connect)",
                                                gossiper, getLocalAddress()));
                    }
                    view.markAlive(gossiper);
                    // Endpoint has been connected
                    for (ReplicatedState state : gossipingEndpoint.getStates()) {
                        if (state.isNotifiable()) {
                            notifyRegister(state);
                        }
                    }
                    // We want it all, baby
                    gossipingEndpoint.getHandler().gossip(Arrays.asList(new Digest(
                                                                                   gossiper,
                                                                                   ALL_STATES,
                                                                                   -1)));
                }
            }, fdFactory);
        }
    }

    /**
     * Check the status of the endpoints we know about, updating the ring with
     * the new state
     */
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
        final Endpoint newEndpoint = new Endpoint(
                                                  address,
                                                  communications.handlerFor(address));
        Endpoint previous = endpoints.putIfAbsent(address, newEndpoint);
        if (previous == null) {
            if (log.isDebugEnabled()) {
                log.debug(format("%s connecting and gossiping with %s",
                                 getLocalAddress(), address));
            }
            List<Digest> filtered = new ArrayList<Digest>(digests.size());
            for (Digest digest : digests) {
                if (!digest.getAddress().equals(address)) {
                    filtered.add(digest);
                }
            }
            filtered.add(new Digest(address, ALL_STATES, -1)); // We want it
                                                               // all, baby
            newEndpoint.getHandler().gossip(filtered);
        }
    }

    protected void discover(final InetSocketAddress address) {
        if (log.isTraceEnabled()) {
            log.trace(String.format("%s discovering %s", getLocalAddress(),
                                    address));
        }
        final Endpoint endpoint = new Endpoint(
                                               address,
                                               communications.handlerFor(address));
        Endpoint previous = endpoints.putIfAbsent(address, endpoint);
        if (previous != null) {
            if (log.isDebugEnabled()) {
                log.debug(format("%s already discovered on %s",
                                 endpoint.getAddress(), getLocalAddress()));
            }
        } else {
            endpoint.markAlive(new Runnable() {
                @Override
                public void run() {
                    view.markAlive(address);
                    if (log.isDebugEnabled()) {
                        log.debug(String.format("%s is now UP on %s (discover)",
                                                address, getLocalAddress()));
                    }
                    // Endpoint has been connected
                    for (ReplicatedState state : endpoint.getStates()) {
                        if (state.isNotifiable()) {
                            notifyRegister(state);
                        }
                    }
                    // We want it all, baby
                    endpoint.getHandler().gossip(Arrays.asList(new Digest(
                                                                          address,
                                                                          ALL_STATES,
                                                                          -1)));
                }
            }, fdFactory);
        }
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
        Endpoint endpoint = new Endpoint(address, update.state,
                                         communications.handlerFor(address));
        Endpoint previous = endpoints.putIfAbsent(address, endpoint);
        if (previous != null) {
            if (log.isDebugEnabled()) {
                log.debug(format("%s already discovered on %s (update)",
                                 endpoint.getAddress(), getLocalAddress()));
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug(String.format("%s discovered and connecting with %s",
                                        getLocalAddress(), address));
            }
            // We want it all, baby
            endpoint.getHandler().gossip(Arrays.asList(new Digest(address,
                                                                  ALL_STATES,
                                                                  -1)));
        }
    }

    /**
     * Examine all the digests send by a gossiper. Determine whether we have out
     * of date state and need it from our informant, or whether our informant is
     * out of date and we need to send the updated state to the informant
     * 
     * @param digests
     * @param gossipHandler
     */
    protected void examine(Digest[] digests, GossipMessages gossipHandler) {
        if (log.isTraceEnabled()) {
            log.trace(String.format("Member: %s receiving gossip digests: %s",
                                    getLocalAddress(), Arrays.toString(digests)));
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
                            addUpdatedState(endpoint, deltaState, digest);
                        }
                    } else {
                        deltaDigests.add(new Digest(digest.getAddress(),
                                                    digest.getId(), -1L));
                    }
                } else {
                    if (getLocalAddress().equals(digest.getAddress())) {
                        addUpdatedLocalState(deltaState, digest);
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
        updateHeartbeat();
        List<Digest> digests = randomDigests();
        List<InetSocketAddress> members = new ArrayList<>();
        for (int i = 0; i < redundancy; i++) {
            members.add(gossipWithTheLiving(digests));
        }
        gossipWithTheDead(digests);
        gossipWithSeeds(digests, members);
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
        checkConnectionStatus(gossipHandler.getGossipper());
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
     * @param members
     *            - the live member we've gossiped with.
     */
    protected void gossipWithSeeds(final List<Digest> digests,
                                   List<InetSocketAddress> members) {
        InetSocketAddress address = view.getRandomSeedMember(members);
        if (address == null) {
            return;
        }
        Endpoint endpoint = endpoints.get(address);
        if (endpoint != null) {
            List<Digest> filtered = new ArrayList<Digest>(digests.size());
            for (Digest digest : digests) {
                if (!digest.getAddress().equals(address)) {
                    filtered.add(digest);
                }
            }
            filtered.add(new Digest(address, ALL_STATES, -1)); // We want it
                                                               // all, baby
            endpoint.getHandler().gossip(filtered);
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
            List<Digest> filtered = new ArrayList<Digest>(digests.size());
            for (Digest digest : digests) {
                if (!digest.getAddress().equals(address)) {
                    filtered.add(digest);
                }
            }
            endpoint.getHandler().gossip(filtered);
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
     * Notify the gossip listener of the deregistration of the replicated state.
     * This is done on a seperate thread
     * 
     * @param state
     */
    protected void notifyDeregister(final ReplicatedState state) {
        assert state != null;
        if (state.isHeartbeat()) {
            return;
        }
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
     * Notify the gossip listener of the registration of the replicated state.
     * This is done on a seperate thread
     * 
     * @param state
     */
    protected void notifyRegister(final ReplicatedState state) {
        assert state != null : "State cannot be null";
        if (state.isHeartbeat()) {
            return;
        }
        assert state.getState().length > 0 : "State cannot be zero length";
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

    /**
     * Notify the gossip listener of the update of the replicated state. This is
     * done on a seperate thread
     * 
     * @param state
     */
    protected void notifyUpdate(final ReplicatedState state) {
        assert state != null : "State cannot be null";
        if (state.isHeartbeat()) {
            return;
        }
        assert state.getState().length > 0 : "State cannot be zero length";
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

    /**
     * Answer a randomized digest list of all the replicated state this node
     * knows of
     * 
     * @return
     */
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
            log.trace(format("Random gossip digests from %s are : %s",
                             getLocalAddress(), digests));
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
                                    getLocalAddress(), Arrays.toString(digests)));
        }
        checkConnectionStatus(gossipHandler.getGossipper());

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
        assert !getLocalAddress().equals(state.node) : "Should never have received ring state for ourselves";
        ring.send(state);
        update(state, gossiper);
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
    protected boolean update(final Update update, InetSocketAddress gossiper) {
        checkConnectionStatus(gossiper);
        if (log.isTraceEnabled()) {
            log.trace(String.format("Member: %s receiving update state: %s",
                                    getLocalAddress(), update));
        }
        assert update.node != null : String.format("endpoint address is null: "
                                                   + update);
        assert !update.node.equals(getLocalAddress()) : "Should not have received a state update we own";
        if (view.isQuarantined(update.node)) {
            if (log.isDebugEnabled()) {
                log.debug(format("Ignoring gossip for %s because it is a quarantined endpoint",
                                 update));
            }
            return false;
        }
        final Endpoint endpoint = endpoints.get(update.node);
        if (endpoint != null) {
            endpoint.updateState(update.state, Gossip.this);
        } else {
            discover(update);
        }
        return false;
    }
}
