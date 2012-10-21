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
package com.hellblazer.gossip.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * @author hhildebrand
 * 
 */
public class ByteBufferOutputStream extends OutputStream {

    private final ByteBufferPool bufferPool;
    private ByteBuffer           buffer;

    /**
     * @param bp
     */
    public ByteBufferOutputStream(ByteBufferPool bp) {
        this(bp, 32);
    }

    /**
     * @param bp
     */
    public ByteBufferOutputStream(ByteBufferPool bp, int initialSize) {
        bufferPool = bp;
        buffer = bufferPool.allocate(initialSize);
    }

    public ByteBuffer toByteBuffer() {
        buffer.limit(buffer.position());
        return buffer;
    }

    /**
     * Increases the capacity if necessary to ensure that it can hold at least
     * the number of elements specified by the minimum capacity argument.
     * 
     * @param minCapacity
     *            the desired minimum capacity
     * @throws OutOfMemoryError
     *             if {@code minCapacity < 0}. This is interpreted as a request
     *             for the unsatisfiably large capacity
     *             {@code (long) Integer.MAX_VALUE + (minCapacity - Integer.MAX_VALUE)}
     *             .
     */
    private void ensureCapacity(int minCapacity) {
        // overflow-conscious code
        if (minCapacity - buffer.limit() > 0) {
            try {
                grow(minCapacity);
            } catch (OutOfMemoryError e) {
                System.out.println(String.format("Attempted to grow stream to %s bytes",
                                                 minCapacity));
                throw e;
            }
        }
        assert buffer.capacity() >= minCapacity : String.format("Need: %s, required %s more bytes ",
                                                                minCapacity,
                                                                minCapacity
                                                                        - buffer.remaining());
    }

    /**
     * Increases the capacity to ensure that it can hold at least the number of
     * elements specified by the minimum capacity argument.
     * 
     * @param minCapacity
     *            the desired minimum capacity
     */
    private void grow(int minCapacity) {
        if (buffer.capacity() >= minCapacity) {
            buffer.limit(minCapacity);
            return;
        }
        // overflow-conscious code
        int oldCapacity = buffer.capacity();
        int newCapacity = oldCapacity << 1;
        if (newCapacity - minCapacity < 0)
            newCapacity = minCapacity;
        if (newCapacity < 0) {
            if (minCapacity < 0) // overflow
                throw new OutOfMemoryError("Math overflow!");
            newCapacity = Integer.MAX_VALUE;
        }
        assert newCapacity >= minCapacity : "Math is hard";
        int position = buffer.position();
        ByteBuffer oldBuffer = buffer;
        buffer = bufferPool.allocate(newCapacity);
        buffer.put(oldBuffer.array(), 0, position);
        bufferPool.free(oldBuffer);
        assert buffer.capacity() >= minCapacity : "Math is hard";
    }

    /**
     * Writes the specified byte to this byte buffer output stream.
     * 
     * @param b
     *            the byte to be written.
     */
    public void write(int b) {
        ensureCapacity(buffer.position() + 1);
        buffer.put((byte) b);
    }

    /**
     * Writes <code>len</code> bytes from the specified byte buffer starting at
     * offset <code>off</code> to this byte buffer output stream.
     * 
     * @param b
     *            the data.
     * @param off
     *            the start offset in the data.
     * @param len
     *            the number of bytes to write.
     */
    public void write(byte b[], int off, int len) {
        if ((off < 0) || (off > b.length) || (len < 0)
            || ((off + len) - b.length > 0)) {
            throw new IndexOutOfBoundsException();
        }
        ensureCapacity(buffer.position() + len);
        buffer.put(b, off, len);
    }

    /**
     * Resets the <code>count</code> field of this byte buffer output stream to
     * zero, so that all currently accumulated output in the output stream is
     * discarded. The output stream can be used again, reusing the already
     * allocated buffer space.
     * 
     * @see java.io.ByteArrayInputStream#count
     */
    public void reset() {
        buffer.rewind();
    }

    /**
     * Returns the current size of the buffer.
     * 
     * @return the value of the <code>count</code> field, which is the number of
     *         valid bytes in this output stream.
     * @see java.io.ByteArrayOutputStream#count
     */
    public int size() {
        return buffer.position();
    }

    /**
     * Closing a <tt>ByteBufferOutputStream</tt> has no effect. The methods in
     * this class can be called after the stream has been closed without
     * generating an <tt>IOException</tt>.
     * <p>
     * 
     */
    public void close() throws IOException {
    }
}
