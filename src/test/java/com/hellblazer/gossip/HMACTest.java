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

import static junit.framework.Assert.fail;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.SecretKeySpec;

import org.junit.Test;

/**
 * @author hhildebrand
 * 
 */
public class HMACTest {

    @Test
    public void testMac() throws InvalidKeyException, NoSuchAlgorithmException,
                         ShortBufferException, SecurityException {

        byte[] keyData2 = { (byte) 0x23, (byte) 0x45, (byte) 0x83, (byte) 0xad,
                (byte) 0x23, (byte) 0x45, (byte) 0x83, (byte) 0xad,
                (byte) 0x23, (byte) 0x45, (byte) 0x83, (byte) 0xad,
                (byte) 0x23, (byte) 0x45, (byte) 0x83, (byte) 0xad,
                (byte) 0x23, (byte) 0x45, (byte) 0x83, (byte) 0xad };

        SecretKey sk2 = new SecretKeySpec(keyData2, HMAC.MAC_TYPE);

        int startOffset = 0;
        int length = 16;
        byte[] data1 = {
                //the data
                (byte) 0x01, (byte) 0x02, (byte) 0x03, (byte) 0x04,
                (byte) 0x11, (byte) 0x12, (byte) 0x13, (byte) 0x14,
                (byte) 0x21, (byte) 0x22, (byte) 0x23, (byte) 0x24,
                (byte) 0x31, (byte) 0x32, (byte) 0x33, (byte) 0x34,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };

        byte[] data2 = {
                //the data
                (byte) 0xf1, (byte) 0xf2, (byte) 0xf3, (byte) 0xf4,
                (byte) 0x11, (byte) 0x12, (byte) 0x13, (byte) 0x14,
                (byte) 0x21, (byte) 0x22, (byte) 0x23, (byte) 0x24,
                (byte) 0x31, (byte) 0x32, (byte) 0x33, (byte) 0x34,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };

        HMAC m1a = new HMAC();
        HMAC m2a = new HMAC();
        m2a.setKey(sk2);
        HMAC m1b = new HMAC();
        HMAC m2b = new HMAC();
        m2b.setKey(sk2);

        m1a.addMAC(data1, startOffset, length);
        m2a.addMAC(data2, startOffset, length);

        try { // m1a & m1b
            m1b.checkMAC(data1, startOffset, length);
        } catch (SecurityException e) {
            fail("m1a & m1b failed");
        }

        try { // m2a & m2b
            m2b.checkMAC(data2, startOffset, length);
        } catch (SecurityException e) {
            fail("m2a & m2b failed");
        }

        try { // m1a & m2b
            m2b.checkMAC(data1, startOffset, length);
            fail("m1a & m2b failed");
        } catch (SecurityException e) {
            // expected
        }

        try { // m2a & m1b
            m1b.checkMAC(data2, startOffset, length);
            fail("m2a & m1b failed");
        } catch (SecurityException e) {
            // expected
        }

        try { // m1a & mod data & m1b
            data1[3] = (byte) 0xff;
            m1b.checkMAC(data1, startOffset, length);
            fail("m1a & mod data & m1b failed");
        } catch (SecurityException e) {
        }

        try { // m2a & mod mac & m2b
            data2[32] = (byte) 0xff;
            m2b.checkMAC(data2, startOffset, length);
            System.out.println("m2a & mod mac & m2b failed");
        } catch (SecurityException e) {
        }
    }
}
