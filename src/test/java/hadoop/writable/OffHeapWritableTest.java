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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author adam
 */
public class OffHeapWritableTest {
    
    private OffHeapWritable writable;
    
    public OffHeapWritableTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
        writable = new OffHeapWritable(10, 30);
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of write method, of class OffHeapWritable.
     */
    @Test
    public void testSimpleWrite_DataOutput() throws Exception {
        System.out.println("write");
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        writable.write(5);
        DataOutputStream d = new DataOutputStream(bout);
        d.flush();
        int dataStartsAt = bout.size();
        System.out.println("Data starts at: " + dataStartsAt);
        writable.write(d);
        d.close();
        assertEquals("resulting bytes: " + Arrays.toString(bout.toByteArray()), 1, bout.toByteArray()[dataStartsAt+3]);
        assertEquals("resulting bytes: " + Arrays.toString(bout.toByteArray()), 1, bout.toByteArray()[dataStartsAt+11]);
        assertEquals("resulting bytes: " + Arrays.toString(bout.toByteArray()), 5, bout.toByteArray()[dataStartsAt+12]);
    }

    @Test
    public void testFullPageWrite() throws Exception {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        for(int i = 0; i < 10; ++i) {
            writable.write(5);
        }
        DataOutputStream d = new DataOutputStream(bout);
        d.flush();
        int dataStartsAt = bout.size();
        System.out.println("Data starts at: " + dataStartsAt);
        writable.write(d);
        d.close();
        byte[] writtenBytes = bout.toByteArray();
        byte[] dataBytes = Arrays.copyOfRange(writtenBytes, dataStartsAt, writtenBytes.length);
        byte[] expectedBytes = generateExpectedBytes(5, 10);
        assertArrayEquals(expectedBytes, dataBytes);
    }
    
    @Test
    public void testFullPageWritePlusOne() throws Exception {
        for(int i = 0; i < 11; ++i) {
            writable.write(7);
        }
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(sink);
        writable.write(out);
        out.close();
        assertArrayEquals(generateExpectedBytes(7, 11), sink.toByteArray());
    }
    
    @Test
    public void testReset() throws Exception {
        for(int i = 0; i < 11; ++i) {
            writable.write(7);
        }
        writable.reset();
        for(int i = 0; i < 3; ++i) {
            writable.write(3);
        }
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(sink);
        writable.write(out);
        out.close();
        assertArrayEquals(generateExpectedBytes(3, 3), sink.toByteArray());
    }
    
    @Test
    public void testRead() throws Exception {
        byte[] readFrom = generateExpectedBytes(3, 15);
        writable.readFields(new DataInputStream(new ByteArrayInputStream(readFrom)));
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(sink);
        writable.write(out);
        out.close();
        assertArrayEquals(readFrom, sink.toByteArray());
        
    }
    @Test
    public void testGenerateExpectedBytes() {
        byte[] generated = generateExpectedBytes((byte) 3, 0x1001);
        byte[] expected = new byte[0x1001+12];
        System.arraycopy(new byte[]{0,0,0,1,0,0,0,0,0,0,0x10,1}, 0, expected, 0, 12);
        for(int i = 0; i < 0x1001; ++i) {
            expected[i+12] = 3;
        }
        assertArrayEquals("Expected: " + Arrays.toString(Arrays.copyOfRange(expected, 0, 14)) + "\nActual: " + Arrays.toString(Arrays.copyOfRange(generated,0, 14)), expected, generated);
    }
    
    @Test
    public void testInputStreamHalfPage() throws Exception {
        for(int i = 0; i < 5; ++i) {
            writable.write(i);
        }
        InputStream in = writable.asInputStream();
        for(int i = 0; i < 5; ++i) {
            assertEquals(i, in.read());
        }
        assertEquals(-1, in.read());
    }
    
    @Test
    public void testInputStreamExactPage() throws Exception {
        for(int i = 0; i < 10; ++i) {
            writable.write(i);
        }
        InputStream in = writable.asInputStream();
        for(int i = 0; i < 10; ++i) {
            assertEquals(i, in.read());
        }
        assertEquals(-1, in.read());
    }
    
    @Test
    public void testInputStreamPagePlusOne() throws Exception {
        for(int i = 0; i < 11; ++i) {
            writable.write(i);
        }
        InputStream in = writable.asInputStream();
        for(int i = 0; i < 11; ++i) {
            assertEquals(i, in.read());
        }
        assertEquals(-1, in.read());
    }
    
    private byte[] generateExpectedBytes(int fillWith, int count) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write(new byte[]{0,0,0,1,0,0,0,0});
            System.out.println(count + " => " + ((count & 0xFF000000) >> 24) + " " + ((count & 0x00FF0000) >> 16) + " "
                    + ((count & 0x0000FF00) >> 8) + " " + (count & 0x000000FF));
            out.write((count & 0xFF000000) >> 24);
            out.write((count & 0x00FF0000) >> 16);
            out.write((count & 0x0000FF00) >> 8);
            out.write(count & 0x000000FF);
            for(int i = 0; i < count; ++i) {
                out.write(fillWith);
            }
        } catch (IOException ex) {
            Logger.getLogger(OffHeapWritableTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        return out.toByteArray();
    }
    
}
