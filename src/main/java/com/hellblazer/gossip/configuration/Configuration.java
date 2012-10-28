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

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.hellblazer.gossip.UdpCommunications;
import com.hellblazer.gossip.fd.AdaptiveFailureDetectorFactory;

/**
 * A configuration bean for constructing Gossip instances
 * 
 * @author hhildebrand
 * 
 */
public class Configuration {

    public static class Address {
        public Address(int port) {
            this.address = new InetSocketAddress(0).getAddress(); //lame
            this.port = port;
        }

        public InetSocketAddress toAddress() {
            return new InetSocketAddress(address, port);
        }

        public InetAddress address;
        public int         port;
    }

    public static Configuration fromYaml(InputStream yaml)
                                                          throws JsonParseException,
                                                          JsonMappingException,
                                                          IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(yaml, Configuration.class);
    }

    @JsonDeserialize(as = byte[].class, using = Base64Deserializer.class)
    public byte[]                         hmacKey;
    public List<Address>                  seeds                   = Collections.emptyList();
    public int                            quarantineDelay;
    public int                            unreachableDelay;
    public Address                        endpoint                = new Address(
                                                                                0);
    public int                            receiveBufferMultiplier = UdpCommunications.DEFAULT_RECEIVE_BUFFER_MULTIPLIER;
    public int                            sendBufferMultiplier    = UdpCommunications.DEFAULT_SEND_BUFFER_MULTIPLIER;
    public AdaptiveFailureDetectorFactory failureDetector;

    /*
    public Gossip construct() {
        UdpCommunications comms = new UdpCommunications(endpoint.toAddress(), receiveBufferMultiplier, sendBufferMultiplier, new HMAC(new SecretKeySpec(hmacKey, null))
    }
    */
}
