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

package com.hellblazer.gossip.configuration;

import static com.hellblazer.gossip.Gossip.DEFAULT_CLEANUP_CYCLES;
import static com.hellblazer.gossip.Gossip.DEFAULT_HEARTBEAT_CYCLE;

import java.io.IOException;
import java.io.InputStream;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.uuid.EthernetAddress;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.UUIDTimer;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import com.hellblazer.gossip.FailureDetectorFactory;
import com.hellblazer.gossip.Gossip;
import com.hellblazer.gossip.SystemView;
import com.hellblazer.gossip.UdpCommunications;
import com.hellblazer.gossip.fd.AdaptiveFailureDetectorFactory;
import com.hellblazer.utils.Base64Coder;

/**
 * A configuration bean for constructing Gossip instances
 * 
 * @author hhildebrand
 * 
 */
public class GossipConfiguration {

    public static class Address {
        public InetAddress host;
        public int         port;

        public Address() {
        }

        public Address(int port) {
            host = new InetSocketAddress(0).getAddress(); //lame
            this.port = port;
        }

        public InetSocketAddress toAddress() {
            return new InetSocketAddress(host, port);
        }
    }

    private static Logger log = LoggerFactory.getLogger(GossipConfiguration.class);

    public static GossipConfiguration fromYaml(InputStream yaml)
                                                                throws JsonParseException,
                                                                JsonMappingException,
                                                                IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(yaml, GossipConfiguration.class);
    }

    public int                    cleanupCycles           = DEFAULT_CLEANUP_CYCLES;
    public int                    commThreads             = 2;
    public Address                endpoint                = new Address(0);
    public FailureDetectorFactory fdFactory;
    public int                    gossipInterval          = 3;
    public String                 gossipUnit              = TimeUnit.SECONDS.name();
    public int                    heartbeatCycle          = DEFAULT_HEARTBEAT_CYCLE;
    public String                 hmac;
    public String                 hmacKey;
    public String                 networkInterface;
    public long                   quarantineDelay         = TimeUnit.SECONDS.toMillis(6);
    public int                    receiveBufferMultiplier = UdpCommunications.DEFAULT_RECEIVE_BUFFER_MULTIPLIER;
    public List<Address>          seeds                   = Collections.emptyList();
    public int                    sendBufferMultiplier    = UdpCommunications.DEFAULT_SEND_BUFFER_MULTIPLIER;
    public long                   unreachableDelay        = (int) TimeUnit.DAYS.toMillis(2);

    public Gossip construct() throws SocketException {
        Random entropy = new SecureRandom();
        UdpCommunications comms = constructUdpComms();
        SystemView view = new SystemView(entropy, comms.getLocalAddress(),
                                         getSeeds(), quarantineDelay,
                                         unreachableDelay);
        return new Gossip(getUuidGenerator(), comms, view, getFdFactory(),
                          entropy, gossipInterval, getGossipUnit(),
                          cleanupCycles, heartbeatCycle);
    }

    public UdpCommunications constructUdpComms() throws SocketException {
        return new UdpCommunications(
                                     endpoint.toAddress(),
                                     Executors.newFixedThreadPool(commThreads,
                                                                  new ThreadFactory() {
                                                                      int i = 0;

                                                                      @Override
                                                                      public Thread newThread(Runnable arg0) {
                                                                          Thread daemon = new Thread(
                                                                                                     String.format("UDP Comms[%s]",
                                                                                                                   i++));
                                                                          daemon.setDaemon(true);
                                                                          daemon.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
                                                                              @Override
                                                                              public void uncaughtException(Thread t,
                                                                                                            Throwable e) {
                                                                                  UdpCommunications.log.error(String.format("Uncaught exception on dispatcher %s",
                                                                                                                            t),
                                                                                                              e);
                                                                              }
                                                                          });
                                                                          return daemon;
                                                                      }
                                                                  }),
                                     receiveBufferMultiplier,
                                     sendBufferMultiplier, getMac());
    }

    public FailureDetectorFactory getFdFactory() {
        if (fdFactory == null) {
            long gossipIntervalMillis = getGossipUnit().toMillis(gossipInterval);
            fdFactory = new AdaptiveFailureDetectorFactory(
                                                           0.9,
                                                           50,
                                                           0.8,
                                                           cleanupCycles
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
            throw new IllegalStateException(
                                            String.format("Unable to create mac %s",
                                                          hmac));
        } catch (InvalidKeyException e) {
            throw new IllegalStateException(
                                            String.format("Invalid key %s for mac %s",
                                                          hmacKey, hmac));
        }
        return mac;
    }

    public List<InetSocketAddress> getSeeds() {
        List<InetSocketAddress> seedAddresses = new ArrayList<InetSocketAddress>();
        for (Address a : seeds) {
            seedAddresses.add(a.toAddress());
        }
        return seedAddresses;
    }

    public TimeBasedGenerator getUuidGenerator() {
        if (networkInterface != null) {
            try {
                NetworkInterface nif = NetworkInterface.getByName(networkInterface);
                EthernetAddress addr = new EthernetAddress(
                                                           nif.getHardwareAddress());
                return new TimeBasedGenerator(addr, (UUIDTimer) null);
            } catch (SocketException e) {
                log.warn(String.format("The network interface %s does not exist",
                                       networkInterface));
            }
        }
        return Generators.timeBasedGenerator();
    }

    public TimeUnit getGossipUnit() {
        try {
            return TimeUnit.valueOf(gossipUnit);
        } catch (IllegalArgumentException e) {
            log.error(String.format("%s is not a legal TimeUnit value"),
                      gossipUnit);
            throw e;
        }
    }
}
