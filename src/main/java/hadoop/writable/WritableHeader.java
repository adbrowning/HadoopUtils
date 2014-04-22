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
