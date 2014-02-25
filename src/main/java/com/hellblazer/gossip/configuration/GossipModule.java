/*
 * (C) Copyright 2014 Chiral Behaviors, All Rights Reserved
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

import java.net.InetSocketAddress;

import com.hellblazer.utils.deserializers.InetSocketAddressDeserializer;
import com.hellblazer.utils.fd.json.FdFactoryModule;

/**
 * @author hhildebrand
 * 
 */

public class GossipModule extends FdFactoryModule {

    private static final long serialVersionUID = 1L;

    public GossipModule() {
        super("GossipModule");
    }

    public GossipModule(String name) {
        super(name);
    }

    @Override
    public void setupModule(SetupContext context) {
        addDeserializer(InetSocketAddress.class,
                        new InetSocketAddressDeserializer());
        super.setupModule(context);
    }
}