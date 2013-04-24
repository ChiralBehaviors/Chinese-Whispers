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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * @author hhildebrand
 * 
 */
public class YamlHelper {
	public static GossipConfiguration fromYaml(File yaml)
			throws JsonParseException, JsonMappingException, IOException {
		return fromYaml(new FileInputStream(yaml));
	}

	public static GossipConfiguration fromYaml(InputStream yaml)
			throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		mapper.registerModule(getModule());
		return mapper.readValue(yaml, GossipConfiguration.class);
	}

	public static Module getModule() {
		SimpleModule module = new SimpleModule();
		module.addDeserializer(InetSocketAddress.class,
				new InetSocketAddressDeserializer());
		return module;
	}
}
