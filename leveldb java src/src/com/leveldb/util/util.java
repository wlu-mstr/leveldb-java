package com.leveldb.util;

import java.io.UnsupportedEncodingException;
import java.util.Random;

public class util {

	public static int compareTo(byte[] buffer1, int offset1, int length1,
			byte[] buffer2, int offset2, int length2) {
		// Short circuit equal case
		if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2) {
			return 0;
		}
		// Bring WritableComparator code local
		int end1 = offset1 + length1;
		int end2 = offset2 + length2;
		for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
			int a = (buffer1[i] & 0xff);
			int b = (buffer2[j] & 0xff);
			if (a != b) {
				return a - b;
			}
		}
		return length1 - length2;
	}

	public static int compareTo(byte[] buffer1, byte[] buffer2) {
		return compareTo(buffer1, 0, buffer1.length, buffer2, 0, buffer2.length);
	}

	/**
	 * Convert an int value to a byte array
	 * 
	 * @param val
	 *            value
	 * @return the byte array
	 */
	public static byte[] toBytes(int val) {
		byte[] b = new byte[4];
		for (int i = 3; i > 0; i--) {
			b[i] = (byte) val;
			val >>>= 8;
		}
		b[0] = (byte) val;
		return b;
	}

	public static byte[] toBytes(byte val) {
		byte[] b = new byte[1];

		b[0] = val;
		return b;
	}

	/**
	 * Put an int value out to the specified byte array position.
	 * 
	 * @param bytes
	 *            the byte array
	 * @param offset
	 *            position in the array
	 * @param val
	 *            int to write out
	 * @return incremented offset
	 * @throws IllegalArgumentException
	 *             if the byte array given doesn't have enough room at the
	 *             offset specified.
	 */
	public static int putInt(byte[] bytes, int offset, int val) {
		if (bytes.length - offset < SIZEOF_INT) {
			throw new IllegalArgumentException(
					"Not enough room to put an int at" + " offset " + offset
							+ " in a " + bytes.length + " byte array");
		}
		for (int i = offset + 3; i > offset; i--) {
			bytes[i] = (byte) val;
			val >>>= 8;
		}
		bytes[offset] = (byte) val;
		return offset + SIZEOF_INT;
	}

	public static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;

	/**
	 * Converts a byte array to an int value
	 * 
	 * @param bytes
	 *            byte array
	 * @return the int value
	 */
	public static int toInt(byte[] bytes) {
		return toInt(bytes, 0, SIZEOF_INT);
	}

	/**
	 * Converts a byte array to an int value
	 * 
	 * @param bytes
	 *            byte array
	 * @param offset
	 *            offset into array
	 * @return the int value
	 */
	public static int toInt(byte[] bytes, int offset) {
		return toInt(bytes, offset, SIZEOF_INT);
	}

	/**
	 * Converts a byte array to an int value
	 * 
	 * @param bytes
	 *            byte array
	 * @param offset
	 *            offset into array
	 * @param length
	 *            length of int (has to be {@link #SIZEOF_INT})
	 * @return the int value
	 * @throws IllegalArgumentException
	 *             if length is not {@link #SIZEOF_INT} or if there's not enough
	 *             room in the array at the offset indicated.
	 */
	public static int toInt(byte[] bytes, int offset, final int length) {
		if (length != SIZEOF_INT || offset + length > bytes.length) {
		}
		int n = 0;
		for (int i = offset; i < (offset + length); i++) {
			n <<= 8;
			n ^= bytes[i] & 0xFF;
		}
		return n;
	}

	/**
	 * Convert a long value to a byte array using big-endian.
	 * 
	 * @param val
	 *            value to convert
	 * @return the byte array
	 */
	public static byte[] toBytes(long val) {
		byte[] b = new byte[8];
		for (int i = 7; i > 0; i--) {
			b[i] = (byte) val;
			val >>>= 8;
		}
		b[0] = (byte) val;
		return b;
	}

	/**
	 * Put a long value out to the specified byte array position.
	 * 
	 * @param bytes
	 *            the byte array
	 * @param offset
	 *            position in the array
	 * @param val
	 *            long to write out
	 * @return incremented offset
	 * @throws IllegalArgumentException
	 *             if the byte array given doesn't have enough room at the
	 *             offset specified.
	 */
	public static int putLong(byte[] bytes, int offset, long val) {
		if (bytes.length - offset < SIZEOF_LONG) {
			throw new IllegalArgumentException(
					"Not enough room to put a long at" + " offset " + offset
							+ " in a " + bytes.length + " byte array");
		}
		for (int i = offset + 7; i > offset; i--) {
			bytes[i] = (byte) val;
			val >>>= 8;
		}
		bytes[offset] = (byte) val;
		return offset + SIZEOF_LONG;
	}

	/**
	 * @param b
	 *            Presumed UTF-8 encoded byte array.
	 * @return String made from <code>b</code>
	 */
	public static String toString(final byte[] b) {
		if (b == null) {
			return null;
		}
		return toString(b, 0, b.length);
	}

	/**
	 * Joins two byte arrays together using a separator.
	 * 
	 * @param b1
	 *            The first byte array.
	 * @param sep
	 *            The separator to use.
	 * @param b2
	 *            The second byte array.
	 */
	public static String toString(final byte[] b1, String sep, final byte[] b2) {
		return toString(b1, 0, b1.length) + sep + toString(b2, 0, b2.length);
	}

	/**
	 * This method will convert utf8 encoded bytes into a string. If an
	 * UnsupportedEncodingException occurs, this method will eat it and return
	 * null instead.
	 * 
	 * @param b
	 *            Presumed UTF-8 encoded byte array.
	 * @param off
	 *            offset into array
	 * @param len
	 *            length of utf-8 sequence
	 * @return String made from <code>b</code> or null
	 */
	public static String toString(final byte[] b, int off, int len) {
		if (b == null) {
			return null;
		}
		if (len == 0) {
			return "";
		}
		try {
			return new String(b, off, len, "utf-8");
		} catch (UnsupportedEncodingException e) {
			return null;
		}
	}

	public static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;

	/**
	 * Converts a byte array to a long value. Reverses {@link #toBytes(long)}
	 * 
	 * @param bytes
	 *            array
	 * @return the long value
	 */
	public static long toLong(byte[] bytes) {
		return toLong(bytes, 0, SIZEOF_LONG);
	}

	/**
	 * Converts a byte array to a long value. Assumes there will be
	 * {@link #SIZEOF_LONG} bytes available.
	 * 
	 * @param bytes
	 *            bytes
	 * @param offset
	 *            offset
	 * @return the long value
	 */
	public static long toLong(byte[] bytes, int offset) {
		return toLong(bytes, offset, SIZEOF_LONG);
	}

	/**
	 * Converts a byte array to a long value.
	 * 
	 * @param bytes
	 *            array of bytes
	 * @param offset
	 *            offset into array
	 * @param length
	 *            length of data (must be {@link #SIZEOF_LONG})
	 * @return the long value
	 * @throws IllegalArgumentException
	 *             if length is not {@link #SIZEOF_LONG} or if there's not
	 *             enough room in the array at the offset indicated.
	 */
	public static long toLong(byte[] bytes, int offset, final int length) {
		if (length != SIZEOF_LONG || offset + length > bytes.length) {
		}
		long l = 0;
		for (int i = offset; i < offset + length; i++) {
			l <<= 8;
			l ^= bytes[i] & 0xFF;
		}
		return l;
	}

	/**
	 * @param a
	 *            array
	 * @param length
	 *            amount of bytes to grab
	 * @return First <code>length</code> bytes from <code>a</code>
	 */
	public static byte[] head(final byte[] a, final int length) {
		if (a.length < length) {
			return null;
		}
		byte[] result = new byte[length];
		System.arraycopy(a, 0, result, 0, length);
		return result;
	}

	public static byte[] end(final byte[] a, final int length) {
		if (a.length < length) {
			return null;
		}
		byte[] result = new byte[length];
		System.arraycopy(a, a.length - length, result, 0, length);
		return result;
	}

	/**
	 * Converts a string to a UTF-8 byte array.
	 * 
	 * @param s
	 *            string
	 * @return the byte array
	 */
	public static byte[] toBytes(String s) {
		try {
			return s.getBytes("utf-8");
		} catch (UnsupportedEncodingException e) {
			return null;
		}
	}

	/**
	 * @param a
	 *            lower half
	 * @param b
	 *            upper half
	 * @return New array that has a in lower half and b in upper half.
	 */
	public static byte[] add(final byte[] a, final byte[] b) {
		byte[] result = new byte[a.length + b.length];
		System.arraycopy(a, 0, result, 0, a.length);
		System.arraycopy(b, 0, result, a.length, b.length);
		return result;
	}

	/**
	 * do deep data copy
	 * 
	 * @param a
	 * @return
	 */
	public static byte[] deepCopy(byte[] a) {
		byte[] result = new byte[a.length];
		System.arraycopy(a, 0, result, 0, a.length);
		return result;
	}

	/**
	 * @param a
	 *            first third
	 * @param b
	 *            second third
	 * @param c
	 *            third third
	 * @return New array made from a, b and c
	 */
	public static byte[] add(final byte[] a, final byte[] b, final byte[] c) {
		byte[] result = new byte[a.length + b.length + c.length];
		System.arraycopy(a, 0, result, 0, a.length);
		System.arraycopy(b, 0, result, a.length, b.length);
		System.arraycopy(c, 0, result, a.length + b.length, c.length);
		return result;
	}

	public static byte[] add(final byte[] a, final byte[] b, final byte[] c,
			final byte[] d) {
		byte[] result = new byte[a.length + b.length + c.length + d.length];
		System.arraycopy(a, 0, result, 0, a.length);
		System.arraycopy(b, 0, result, a.length, b.length);
		System.arraycopy(c, 0, result, a.length + b.length, c.length);
		System.arraycopy(d, 0, result, a.length + b.length + c.length, d.length);
		return result;
	}

	public static byte[] addN(final byte[]... a) {
		int n = a.length;
		int len = 0;
		for (int i = 0; i < n; i++) {
			len += a[i].length;
		}
		byte[] result = new byte[len];
		int offset = 0;
		for (int i = 0; i < n; i++) {
			System.arraycopy(a[i], 0, result, offset, a[i].length);
			offset += a[i].length;
		}

		return result;
	}

	public static byte[] subN(final byte[] src, int offset, int length) {
		byte[] t = new byte[length];
		System.arraycopy(src, offset, t, 0, length);
		return t;
	}

	// byte[] array as key
	public static byte[] RandomKey(Random rnd, int len) {
		// Make sure to generate a wide variety of characters so we
		// test the boundary conditions for short-key optimizations.
		byte kTestChars[] = { 'a', 'b', 'c', 'd', 'e' };
		byte[] result = new byte[len];
		for (int i = 0; i < len; i++) {
			int idx = Math.abs(rnd.nextInt()) % kTestChars.length;
			result[i] = kTestChars[idx];
		}
		return result;
	}

	// String as value for beter display
	public static String RandomString(Random rnd, int len) {
		char dst[] = new char[len];
		for (int i = 0; i < len; i++) {
			dst[i] = (char) (' ' + (Math.abs(rnd.nextInt()) % 95)); // ' ' ..
																	// '~'
		}
		return String.valueOf(dst);
	}

	public static void main(String args[]) {
		int ival = 32354;
		// System.out.print(ival == (toInt(toBytes(ival))));
		byte[] b = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7 };
		System.out.println(b.length);
		System.out.println(subN(b, 1, 4).length);
	}

}
