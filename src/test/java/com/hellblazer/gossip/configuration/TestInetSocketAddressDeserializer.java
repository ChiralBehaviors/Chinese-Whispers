/** (C) Copyright 2013 Hal Hildebrand, All Rights Reserved
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.net.InetSocketAddress;

import org.junit.Test;

import com.fasterxml.jackson.databind.exc.InvalidFormatException;

/**
 * @author hhildebrand
 * 
 */
public class TestInetSocketAddressDeserializer {

	@Test
	public void testDeserialization() throws Exception {
		InetSocketAddressDeserializer test = new InetSocketAddressDeserializer();
		InetSocketAddress address = test._deserialize(":0", null);
		assertEquals(new InetSocketAddress(0), address);
		address = test._deserialize("hellblazer.com:666", null);
		assertEquals(new InetSocketAddress("hellblazer.com", 666), address);

		// failure case
		try {
			address = test._deserialize("hellblazer.com666", null);
			fail("Should have failed with a bad format exception");
		} catch (InvalidFormatException e) {
			// expected
		}
	}
}
