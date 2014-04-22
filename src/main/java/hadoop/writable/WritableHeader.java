/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package hadoop.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author adam
 */
public class WritableHeader {
    private long length;
    private int version;
    
    public WritableHeader(int version, long length) {
        this.version = version;
        this.length = length;
    }
    
    public WritableHeader(DataInput in) throws IOException {
        this.version = in.readInt();
        switch(version) {
            case 1:
                this.length = in.readLong();
                break;
            default:
                throw new IOException("Stream corrupted. Unknown version: " + version);
        }
    }
    
    public void writeHeader(DataOutput out) throws IOException {
        out.writeInt(version);
        out.writeLong(length);
    }
}
