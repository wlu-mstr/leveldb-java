/*
 *  Copyright 2011 Tor-Einar Jarnbjo
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package de.jarnbjo.jsnappy;

import java.io.IOException;
import java.io.OutputStream;

/**
 * <p>
 * This class implements a stream filter for writing compressed data in the SNZ file format.
 * </p>
 *  
 * <p>
 * The data format is compatible to the snzip tool, which is available as a patch to the
 * official Snappy source code.
 * </p>
 *   
 * @author Tor-Einar Jarnbjo
 * @since 1.0
 */
public class SnzOutputStream extends OutputStream {

	public static final int DEFAULT_BUFFER_SIZE = 65536;

	private OutputStream delegate;

	private int bufferSize;
	
	private byte[] buffer;
	private int bufferIndex;
	
	private Buffer cbuffer;

	private byte[] tmpBuffer = new byte[1];

	private boolean closed = false;

	private int effort = SnappyCompressor.DEFAULT_EFFORT;
	
	/**
	 * Creates a new compressing output stream with the default buffer size.
	 * @param out target output stream
	 * @throws IOException
	 */
	public SnzOutputStream(OutputStream out) throws IOException {
		this(out, DEFAULT_BUFFER_SIZE);
	}

	/**
	 * Creates a new compressing output stream with the specified buffer size.
	 * @param out target output stream
	 * @param bufferSize buffer size must be a power of 2 between 2**0 and 2**29
	 * @throws IOException
	 */
	public SnzOutputStream(OutputStream out, int bufferSize) throws IOException {
		this.delegate = out;
		this.bufferSize = bufferSize;

		if (bufferSize < 0) {
			throw new IllegalArgumentException("bufferSize must be a power of 2 between 2**0 and 2**29");
		}
		
		int bufferSize2 = 0;
		while(bufferSize > 1) {
			bufferSize2++;
			if(bufferSize % 2 == 1 || bufferSize2 > 29) {
				throw new IllegalArgumentException("bufferSize must be a power of 2 between 2**0 and 2**29");
			}
			bufferSize >>= 1;
		}

		this.buffer = new byte[this.bufferSize];
		cbuffer = new Buffer(this.bufferSize * 6 / 5);
		
		delegate.write("SNZ".getBytes("ASCII"));
		delegate.write(1);
		delegate.write(bufferSize2);
	}

	/**
	 * Writes the byte to the compressed output stream.
	 */
	@Override
	public void write(int data) throws IOException {
		tmpBuffer[0] = (byte) data;
		write(tmpBuffer);
	}

	/**
	 * Writes the content of <code>data</code> to the compressed output stream.
	 */
	@Override
	public void write(byte[] data) throws IOException {
		write(data, 0, data.length);
	}

	/**
	 * Writes <code>length</code> bytes of data to the compressed stream
	 * from <code>data</code>, starting at index <code>offset</code>.
	 */
	@Override
	public void write(byte[] data, int offset, int length) throws IOException {

		if(closed) {
			throw new IllegalStateException("Stream is closed");
		}

		while(length > 0) {
			if(length > buffer.length - bufferIndex) {
				System.arraycopy(data, offset, buffer, bufferIndex, buffer.length - bufferIndex);
				offset += buffer.length - bufferIndex;
				length -= buffer.length - bufferIndex;
				bufferIndex = buffer.length;
				flushBuffer();
			}
			else {
				System.arraycopy(data, offset, buffer, bufferIndex, length);
				bufferIndex += length;
				length = 0;
			}
		}

	}

	/**
	 * Returns the compression effort used by this stream. The effort
	 * defaults to 1 (fastest, less compression).
	 * 
	 * @return
	 */
	public int getCompressionEffort() {
		return effort;
	}
	
	/**
	 * Sets the compression effort used by this stream from 1 (fastest, less
	 * compression) to 100 (slowest, best compression). If the effort is 
	 * changed after the stream has been written to, the new effort will take
	 * effect on the next packet processed internally and may so affect
	 * data already written to the stream.
	 * 
	 * @param effort
	 */
	public void setCompressionEffort(int effort) {
		this.effort = effort;
	}
	
	private void flushBuffer() throws IOException {
		if(bufferIndex > 0) {
			SnappyCompressor.compress(buffer, 0, bufferIndex, cbuffer, effort);
			bufferIndex = 0;
			int l = cbuffer.getLength();
			while(l>0) {
				delegate.write(l >= 128 ? 0x80 | (l&0x7f) : l);
				l >>= 7;
			}
			delegate.write(cbuffer.getData(), 0, cbuffer.getLength());
		}
	}

	/**
	 * Flushes the remaining data into a new compressed block, writes an end-of-stream
	 * marker and closes the underlaying output stream.
	 */
	@Override
	public void close() throws IOException {
		flushBuffer();
		delegate.write(0);
		delegate.close();
		closed = true;
	}

}
