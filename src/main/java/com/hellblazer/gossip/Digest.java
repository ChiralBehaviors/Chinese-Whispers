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

import static com.hellblazer.gossip.Endpoint.readInetAddress;
import static com.hellblazer.gossip.Endpoint.writeInetAddress;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * Contains information about a specified list of Endpoints and the largest
 * version of the state they have generated as known by the local endpoint.
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public class Digest {
    public static class DigestComparator implements Serializable,
            Comparator<Digest> {
        private static final long serialVersionUID = 1L;

        @Override
        public int compare(Digest digest1, Digest digest2) {
            return (int) (digest1.time - digest2.time);
        }
    }

    private final InetSocketAddress address;
    private final long              time;

    public Digest(ByteBuffer msg) throws UnknownHostException {
        address = readInetAddress(msg);
        assert address != null : "Null digest address";
        time = msg.getLong();
    }

    public Digest(ReplicatedState state) {
        address = state.getAddress();
        assert address != null : "Null replicated state address";
        time = state.getTime();
    }

    public Digest(InetSocketAddress socketAddress, Endpoint ep) {
        address = socketAddress;
        assert address != null : "Null digest address address";
        time = ep.getTime();
    }

    public Digest(InetSocketAddress ep, long diffTime) {
        address = ep;
        time = diffTime;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Digest other = (Digest) obj;
        if (address == null) {
            if (other.address != null) {
                return false;
            }
        } else if (!address.equals(other.address)) {
            return false;
        }
        if (time != other.time) {
            return false;
        }
        return true;
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public long getTime() {
        return time;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (address == null ? 0 : address.hashCode());
        result = prime * result + (int) (time ^ time >>> 32);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(address);
        sb.append(":");
        sb.append(time);
        return sb.toString();
    }

    public void writeTo(ByteBuffer buffer) {
        writeInetAddress(address, buffer);
        buffer.putLong(time);
    }
}
