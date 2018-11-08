/** 
 * (C) Copyright 2010 Hal Hildebrand, All Rights Reserved
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

package com.chiralbehaviors.gossip.configuration;

import static com.chiralbehaviors.gossip.Gossip.DEFAULT_CLEANUP_CYCLES;
import static com.chiralbehaviors.gossip.Gossip.DEFAULT_HEARTBEAT_CYCLE;

import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chiralbehaviors.gossip.Gossip;
import com.chiralbehaviors.gossip.SystemView;
import com.chiralbehaviors.gossip.UdpCommunications;
import com.fasterxml.uuid.Generators;
import com.hellblazer.utils.Base64Coder;
import com.hellblazer.utils.Utils;
import com.hellblazer.utils.fd.FailureDetectorFactory;
import com.hellblazer.utils.fd.impl.AdaptiveFailureDetectorFactory;

/**
 * A configuration bean for constructing Gossip instances
 * 
 * @author hhildebrand
 * 
 */
public class GossipConfiguration {
    private static final Logger    log                     = LoggerFactory.getLogger(GossipConfiguration.class);

    public int                     cleanupCycles           = DEFAULT_CLEANUP_CYCLES;
    public int                     commThreads             = 2;
    public InetSocketAddress       endpoint                = new InetSocketAddress(
                                                                                   "localhost",
                                                                                   0);
    public FailureDetectorFactory  fdFactory;
    public int                     gossipInterval          = 3;
    public TimeUnit                gossipUnit              = TimeUnit.SECONDS;
    public int                     heartbeatCycle          = DEFAULT_HEARTBEAT_CYCLE;
    public String                  hmac;
    public String                  hmacKey;
    public String                  networkInterface;
    public long                    quarantineDelay         = TimeUnit.SECONDS.toMillis(30);
    public int                     redundancy              = 3;
    public int                     receiveBufferMultiplier = UdpCommunications.DEFAULT_RECEIVE_BUFFER_MULTIPLIER;
    public List<InetSocketAddress> seeds                   = Collections.emptyList();
    public int                     sendBufferMultiplier    = UdpCommunications.DEFAULT_SEND_BUFFER_MULTIPLIER;
    public long                    unreachableDelay        = (int) TimeUnit.DAYS.toMillis(2);

    public Gossip construct() throws SocketException {
        Random entropy = new Random();
        UdpCommunications comms = constructUdpComms();
        SystemView view = new SystemView(entropy, comms.getLocalAddress(),
                                         seeds, quarantineDelay,
                                         unreachableDelay);
        return new Gossip(Generators.timeBasedGenerator(), comms, view,
                          getFdFactory(), entropy, gossipInterval, gossipUnit,
                          cleanupCycles, heartbeatCycle, redundancy);
    }

    public UdpCommunications constructUdpComms() throws SocketException {
        ThreadFactory threadFactory = new ThreadFactory() {
            int i = 0;

            @Override
            public Thread newThread(Runnable runnable) {
                Thread daemon = new Thread(runnable,
                                           String.format("UDP Comms[%s]", i++));
                daemon.setDaemon(true);
                daemon.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        UdpCommunications.log.error(String.format("Uncaught exception on dispatcher %s",
                                                                  t), e);
                    }
                });
                return daemon;
            }
        };
        return new UdpCommunications(
                                     getEndpoint(),
                                     Executors.newFixedThreadPool(commThreads,
                                                                  threadFactory),
                                     receiveBufferMultiplier,
                                     sendBufferMultiplier, getMac());
    }

    public InetSocketAddress getEndpoint() throws SocketException {
        if (endpoint.getAddress().isAnyLocalAddress()) {
            if (networkInterface == null) {
                NetworkInterface iface = NetworkInterface.getByIndex(1);
                if (iface == null) {
                    String msg = String.format("Supplied ANY address for endpoint: %s with no networkInterface defined, cannot find network interface 1",
                                               endpoint);
                    log.error(msg);
                    throw new IllegalArgumentException(msg);
                }
                log.info(String.format("Supplied ANY address for endpoint: %s with no networkInterface defined, using %s",
                                       endpoint, iface));
                return new InetSocketAddress(Utils.getAddress(iface),
                                             endpoint.getPort());
            } else {
                NetworkInterface iface = NetworkInterface.getByName(networkInterface);
                if (iface == null) {
                    String msg = String.format("Cannot find network interface: %s ",
                                               networkInterface);
                    log.error(msg);
                    throw new IllegalArgumentException(msg);
                }
                return new InetSocketAddress(Utils.getAddress(iface),
                                             endpoint.getPort());
            }
        }
        return endpoint;
    }

    public FailureDetectorFactory getFdFactory() {
        if (fdFactory == null) {
            long gossipIntervalMillis = gossipUnit.toMillis(gossipInterval);
            fdFactory = new AdaptiveFailureDetectorFactory(
                                                           0.9,
                                                           100,
                                                           0.8,
                                                           2
                                                                   * cleanupCycles
                                                                   * gossipIntervalMillis,
                                                           10,
                                                           gossipIntervalMillis);
        }
        return fdFactory;
    }

    public Mac getMac() {
        if (hmac == null || hmacKey == null) {
            return UdpCommunications.defaultMac();
        }
        Mac mac;
        try {
            mac = Mac.getInstance(hmac);
            mac.init(new SecretKeySpec(Base64Coder.decode(hmacKey), hmac));
        } catch (NoSuchAlgorithmException e) {
            log.error(String.format("Unable to create mac %s", hmac));
            throw new IllegalStateException(
                                            String.format("Unable to create mac %s",
                                                          hmac));
        } catch (InvalidKeyException e) {
            log.error(String.format("Invalid key %s for mac %s", hmacKey, hmac));
            throw new IllegalStateException(
                                            String.format("Invalid key %s for mac %s",
                                                          hmacKey, hmac));
        }
        return mac;
    }
}
