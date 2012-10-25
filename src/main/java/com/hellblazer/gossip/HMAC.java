package com.hellblazer.gossip;

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
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Mac;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.SecretKeySpec;

/**
 * @author hhildebrand
 *
 */

/**
 * Calculate/check the MAC associated with data in a byte array. The MAC is
 * assumed to belong at the end of the byte array containing the data
 * <p/>
 * Can statically set a default key that will be used for all subsequent created
 * MACData objects
 * <p/>
 * To cope with distributed key update being non-transactional, it will check
 * the MAC with the current and the last key but generates MAC with the latest
 * key only.
 */
public class HMAC {
    public static int     MAC_BYTE_SIZE    = 20;
    public static String  MAC_TYPE         = "HmacSHA1";
    private static byte[] DEFAULT_KEY_DATA = { (byte) 0x23, (byte) 0x45,
            (byte) 0x83, (byte) 0xad, (byte) 0x23, (byte) 0x46, (byte) 0x83,
            (byte) 0xad, (byte) 0x23, (byte) 0x45, (byte) 0x83, (byte) 0xad,
            (byte) 0x23, (byte) 0x45, (byte) 0x83, (byte) 0xad, (byte) 0x23,
            (byte) 0x45, (byte) 0x83, (byte) 0xad };

    private volatile Mac  currentMAC       = null;
    private volatile Mac  defaultMAC       = null;                       //only to be used if no key explicitly set 
    private volatile Key  key              = new SecretKeySpec(
                                                               DEFAULT_KEY_DATA,
                                                               MAC_TYPE);
    private Mac           lastMAC          = null;

    public HMAC() {
        try {
            defaultMAC = Mac.getInstance(MAC_TYPE);
            defaultMAC.init(key);
        } catch (Exception e) {
            throw new IllegalStateException();
        }
    }

    /**
     * Adds a MAC of the data to the byte array following the data
     * 
     * @param data
     *            - the buffer holding the data for the MAC, and the MAC
     * @param start
     *            - inclusive start of data
     * @param length
     *            - length of the data, mac added after this
     * @throws javax.crypto.ShortBufferException
     *             - if the buffer is insufficiently long to hold the mac
     * @throws SecurityException
     *             - no key yet provided for MAC calculation
     */
    public synchronized void addMAC(byte[] data, int start, int length)
                                                                       throws ShortBufferException,
                                                                       SecurityException {
        Mac mac = currentMAC == null ? defaultMAC : currentMAC;
        if (mac == null) {
            throw new SecurityException("no key for mac calculation");
        }
        mac.reset();
        mac.update(data, start, length);
        mac.doFinal(data, start + length);
    }

    /**
     * validate a mac that is at the end of a piece of byte array data
     * 
     * @param data
     *            to validate
     * @param start
     *            of the data - inclusive
     * @param length
     *            of the data
     * @throws SecurityException
     *             - the mac does not match
     */
    public synchronized void checkMAC(byte[] data, int start, int length)
                                                                         throws SecurityException {
        if (currentMAC != null) {
            if (validateMac(currentMAC, data, start, length)) {
                return;
            }
        }
        if (lastMAC != null) {
            if (validateMac(lastMAC, data, start, length)) {
                return;
            }
        }
        if (defaultMAC != null) {
            if (validateMac(defaultMAC, data, start, length)) {
                return;
            }
        }
        throw new SecurityException("MAC not valid");
    }

    public synchronized Key getKey() {
        return key;
    }

    /**
     * Return the size, in bytes, of the MAC
     * 
     * @return the size in bytes
     */
    public int getMacSize() {
        return MAC_BYTE_SIZE;
    }

    /**
     * Set the current key to use for the MAC. Makes the existing current key
     * the last key. The default key is destroyed for this MACData object.
     * 
     * @param k
     *            the key to use for the MAC
     * @throws InvalidKeyException
     */
    public synchronized void setKey(Key k) throws InvalidKeyException {
        lastMAC = currentMAC;
        defaultMAC = null; //eliminate the default...
        try {
            currentMAC = Mac.getInstance(MAC_TYPE);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace(); //shouldn't happen as predefined to work...
        }
        currentMAC.init(k);
    }

    private boolean validateMac(Mac m, byte[] data, int start, int length) {
        m.reset();
        m.update(data, start, length);
        byte[] checkMAC = m.doFinal();
        int len = checkMAC.length;
        assert len == MAC_BYTE_SIZE;

        for (int i = 0; i < len; i++) {
            if (checkMAC[i] != data[start + length + i]) {
                return false;
            }
        }
        return true;
    }
}
