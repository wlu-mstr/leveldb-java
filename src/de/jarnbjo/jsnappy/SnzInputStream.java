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

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * <p>
 * This class implements a stream filter for reading compressed data in the SNZ file format.
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
public class SnzInputStream extends FilterInputStream {

	boolean initialized = false;
	private boolean eof = false;

	private int blockSize;

	private Buffer dbuffer;
	private int dbufferIndex = 0;

	private byte[] tmpBuffer = new byte[1];
	
	/**
	 * Creates a new SnzInputStream, reading from the provided InputStream <code>in</code>.
	 * @param in
	 */
	public SnzInputStream(InputStream in) {
		super(in);
	}

	/**
	 * Returns the block size of the snz stream.
	 * @return snz stream block size
	 * @throws IOException
	 */
	public int getBlockSize() throws IOException {
		init();
		return blockSize;
	}

	/**
	 * Reads a single byte from the uncompressed stream.
	 * @throws FormatViolationException if the input data is invalid
	 * @return the read byte or -1 if end of stream is reached
	 */
	@Override
	public int read() throws IOException {
		init();
		return read(tmpBuffer) < 0 ? -1 : tmpBuffer[0] & 0xff;
	}

	/**
	 * Fills the byte array with data from the uncompressed stream.
	 * @throws FormatViolationException if the input data is invalid
	 * @return the number of bytes read or -1 if end of stream is reached
	 */
	@Override
	public int read(byte[] b) throws IOException {
		return read(b, 0, b.length);
	}


	/**
	 * Fills the byte array with data from the uncompressed stream starting
	 * at the specified <code>offset</code> and no more than <code>length</code> bytes.
	 * @param b destination array
	 * @param offset offset into the byte array, on which the data is written
	 * @length maximum number of bytes to write into the byte array
	 * @throws FormatViolationException if the input data is invalid
	 * @return the number of bytes read or -1 if end of stream is reached
	 */
	@Override
	public int read(byte[] b, int offset, int length) throws IOException {

		init();

		if (eof) {
			return -1;
		}

		if (dbuffer == null || dbufferIndex >= dbuffer.getLength()) {
			int cLength = readVInt();
			if (cLength == 0) {
				eof = true;
				return -1;
			}
			byte[] cbuffer = new byte[cLength];
			int o = 0;
			while(o < cLength) {
				o += super.read(cbuffer, o, cbuffer.length - o);
			}

			dbuffer = SnappyDecompressor.decompress(cbuffer);
			dbufferIndex = 0;
		}

		if(length > dbuffer.getLength() - dbufferIndex) {
			length = dbuffer.getLength() - dbufferIndex;
		}

		System.arraycopy(dbuffer.getData(), dbufferIndex, b, offset, length);
		dbufferIndex += length;

		return length;
	}


	void init() throws IOException {
		if(!initialized) {
			char c1 = (char) super.read();
			char c2 = (char) super.read();
			char c3 = (char) super.read();
			int v = super.read();
			if(c1 != 'S' || c2 != 'N' || c3 != 'Z') {
				throw new FormatViolationException("Illegal prefix in SNZ stream");
			}
			if(v != 1) {
				throw new FormatViolationException("Illegal SNZ version: " + v + " (only 1 is supported)", 1);
			}
			blockSize = 1 << super.read();
		}
		initialized = true;
	}


	int readVInt() throws IOException {
		int i, o = 0, vint = 0;
		do {
			i = super.read();
			if (i < 0) {
				throw new EOFException();
			}
			i &= 0xff;
			vint += (i & 0x7f) << (o++ * 7);
		} while ((i & 0x80) == 0x80);
		return vint;
	}

}
