/*
 *  Copyright 2014 Adam Browning
 *  
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package hadoop.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Writable;

/**
 * Implementation of Writable that stores its data off the JVM's heap. The class 
 * can be used as an output stream to act as a sink for data of unknown size that
 * needs to be held together. It also supports acquiring an InputStream to retrieve
 * the information without having to hold it all in the JVM's heap.
 * @author adam
 */
public class OffHeapWritable extends OutputStream implements Writable  {

    private final int VERSION = 1;
    private final List<ByteBuffer> buffers = new ArrayList<>();
    private final int maxNumBlocks;
    private final int blockSize;
    private final byte[] copyBuffer = new byte[512];
    private long lengthWritten = 0;
    private ByteBuffer current;
    
    /**
     * Creates an OffHeapWritable that can be up to blockSize*(maxSize/blockSize) bytes.
     * @param blockSize The number of bytes allocated when more memory is needed to
     * store data; making this sufficiently large will improve performance in data
     * access, but this class will always use at least that much memory, so making it
     * too large will waste RAM
     * @param maxSize  The maximum amount of RAM to use for storing data (the serialized
     * format may be slightly larger due to header information)
     */
    public OffHeapWritable(int blockSize, long maxSize) {
        maxNumBlocks = (int) (maxSize / blockSize);
        current = ByteBuffer.allocateDirect(blockSize);
        buffers.add(current);
        this.blockSize = blockSize;
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
        WritableHeader header = new WritableHeader(VERSION, lengthWritten);
        header.writeHeader(d);        
        current.flip();
        for(int i = 0; i < buffers.size(); ++i) {
            ByteBuffer readFrom = buffers.get(i);
            int numBytesLeftInBuffer = readFrom.limit();
            while(numBytesLeftInBuffer > 0) {
                int numBytesToGet = Math.min(copyBuffer.length, numBytesLeftInBuffer);
                readFrom.get(copyBuffer, 0, numBytesToGet);
                d.write(copyBuffer, 0, numBytesToGet);
                numBytesLeftInBuffer -= numBytesToGet;
            }
            // doing equality check because current doubles as a sentinel
            if(readFrom == current) {                
                break;
            }
        }
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        int version = di.readInt();
        switch(version) {
            case 1:
                readVersion1Data(di);
                break;
            default:
                throw new IllegalStateException("Unknown version: " + version);
        }
    }

    @Override
    public void write(int b) throws IOException {
        if(current.position() == current.limit()) {
            addBlock();
        }
        ++lengthWritten;
        current.put((byte) b);
    }

    public void reset() {
        lengthWritten = 0;
        current = buffers.get(0);
        for(int i = 0; i < buffers.size(); ++i) {
            buffers.get(i).clear();            
        }
    }
    
    /**
     * Returns an input stream reflecting the contents of this OffheapWritable;
     * NOTE: changes to the contents of this may be reflected in the InputStream
     * @return an InputStream on the contents of this buffer
     */
    public InputStream asInputStream() {        
        return new OffheapInputStream();
    }
    
    protected void readVersion1Data(DataInput input) throws IOException {
        long toRead = input.readLong();
        lengthWritten = toRead;
        for(int i = 0; i < buffers.size(); ++i) {
            buffers.get(i).clear();
        }
        int bufferIndex = 0;
        current = null;
        while(toRead > 0) {
            if(current != null) {
                current.flip();
            }
            ByteBuffer writeTo = null;
            if(bufferIndex < buffers.size()) {
                writeTo = buffers.get(bufferIndex);
            } else {
                int allocateSize = (int) Math.min(toRead, Integer.MAX_VALUE);
                writeTo = ByteBuffer.allocateDirect(allocateSize);
                buffers.add(writeTo);                
            }
            ++bufferIndex;
            int leftToCopy = (int) Math.min(toRead, writeTo.capacity());
            toRead -= leftToCopy;
            while(leftToCopy > 0) {
                int copy = Math.min(copyBuffer.length, leftToCopy);
                input.readFully(copyBuffer, 0, copy);
                writeTo.put(copyBuffer, 0, copy);
                leftToCopy -= copy;                
            }            
            current = writeTo;
        }
    }
    
    protected void addBlock() throws IOException {
        current.flip();
        if(buffers.size() == maxNumBlocks) {
            throw new IOException("Buffer full");
        }
        current = ByteBuffer.allocateDirect(blockSize);
        buffers.add(current);
    }

    /**
     * Standard write method from OutputStream; this acts like a ByteArrayOutputStream
     * that stores its bytes off JVM heap, with the difference that this _may_ throw
     * exceptions
     * @param b
     * @param off
     * @param len
     * @throws IOException 
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if(off < 0 || len < 0 || off >= b.length) {
            throw new IllegalArgumentException("offset and length must be at least 0 and offset must be less than length, "
                    + "received offset: " + off +", length: " + len + " for an array of size " + b.length);
        }
        int remaining = len;
        int offset = off;
        while(remaining > 0) {
            int canWrite = current.limit() - current.position();
            if(canWrite == 0) {
                addBlock();
                canWrite = current.limit() - current.position();
            }
            current.put(b, offset, Math.min(remaining, canWrite));
            remaining -= canWrite;
            offset += canWrite;
        }
        lengthWritten += len;
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }
    
    protected class OffheapInputStream extends InputStream {

        protected List<ByteBuffer> bufferClones = new ArrayList<>(buffers.size());
        protected ByteBuffer current = null;
        
        public OffheapInputStream() {
            for(int i = 0; i < buffers.size(); ++i) {
                final ByteBuffer duplicate = buffers.get(i).duplicate();
                if(duplicate.position() > 0) {
                    duplicate.flip();
                }
                bufferClones.add(duplicate);
            }
            current = bufferClones.get(0);
        }
        
        @Override
        public int read() throws IOException {
            int retVal = -1;
            while(current == null || current.remaining() == 0) {
                if(bufferClones.isEmpty()) {
                    return -1;
                }
                current = bufferClones.remove(0);
            }
            retVal = current.get();
            return retVal;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int numRead = 0;
            while(numRead < len) {
                if(current == null || current.remaining() == 0) {
                    if(bufferClones.isEmpty()) {
                        return numRead > 0 ? numRead : -1;
                    }
                    current = bufferClones.remove(0);
                }
                int toRead = Math.min(len - numRead, current.remaining());
                current.get(b, off+numRead, toRead);
                numRead += toRead;
            }
            return numRead;
        }

        @Override
        public int read(byte[] b) throws IOException {
            return read(b, 0, b.length); 
        }
    }
}
