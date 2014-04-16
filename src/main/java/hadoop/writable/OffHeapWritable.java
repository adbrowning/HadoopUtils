/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package hadoop.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author adam
 */
public class OffHeapWritable extends OutputStream implements Writable  {

    private final int VERSION = 1;
    private final List<ByteBuffer> buffers = new ArrayList<>();
    private final int maxNumBlocks;
    private final int blockSize;
    private final byte[] copyBuffer = new byte[1024];
    private long lengthWritten = 0;
    private ByteBuffer current;
    
    public OffHeapWritable(int blockSize, long maxSize) {
        maxNumBlocks = (int) (maxSize / blockSize);
        current = ByteBuffer.allocateDirect(blockSize);
        buffers.add(current);
        this.blockSize = blockSize;
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
        d.writeInt(VERSION);
        System.out.println("version: " + VERSION);
        d.writeLong(lengthWritten);
        System.out.println("length: " + lengthWritten);
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
}
