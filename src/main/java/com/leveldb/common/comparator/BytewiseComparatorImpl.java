package com.leveldb.common.comparator;

import com.leveldb.common.Comparator;
import com.leveldb.common.Slice;
import com.leveldb.util.util;

/*
 * when compare two byte value, we use (a & 0xff) < (b & 0xff);
 * a == b;
 */
public class BytewiseComparatorImpl extends Comparator {

	@Override
	public int Compare(Slice a, Slice b) {
		return a.compareTo(b);
	}

	@Override
	public String Name() {
		return BytewiseComparatorImpl.class.getName();
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @param start
	 *            abcdef
	 * @param limit
	 *            abklmfasdgs
	 * @return abce, side effect: start is modified to abceef
	 */
	@Override
	public byte[] FindShortestSeparator(byte[] start, Slice limit) {

		// Find length of common prefix
		int min_length = Math.min(start.length, limit.size());
		int diff_index = 0;
		while ((diff_index < min_length)
				&& (start[diff_index] == limit.get(diff_index))) {
			diff_index++;
		}

		if (diff_index >= min_length) {
			// Do not shorten if one string is a prefix of the other
		} else {
			int diff_byte = (start[diff_index] & 0xff);
			if (diff_byte < 0xff
					&& diff_byte + 1 < (limit.get(diff_index) & 0xff)) {
				start[diff_index]++;
				start = util.head(start, diff_index + 1); // 0...diff_index
				// start.resize(diff_index + 1);
				// assert(Compare(*start, limit) < 0);
			}
		}

		return start;

	}

	/**
	 * {@inheritDoc} input byte array is modified and ... maybe not useful
	 * anymore
	 * 
	 * @param key
	 *            \x255\x255abc
	 * @return \x255\x255b, side effect: key is modified to \x255\x255bbc
	 */
	@Override
	public byte[] FindShortSuccessor(byte[] key) {

		// Find first character that can be incremented
		int n = key.length;
		for (int i = 0; i < n; i++) {
			int byte_ = (key[i] & 0xff);
			if (byte_ != 0xff) {
				key[i] = (byte) (byte_ + 1);

				key = util.head(key, i + 1);
				// resize(i+1);
				return key;
			}
		}
		// key is a run of 0xffs. Leave it alone.
		return key;

	}

	/**
	 * I am not sure singleton is OK
	 */
	private static Comparator instance_ = new BytewiseComparatorImpl();

	public static Comparator getInstance() {
		return instance_;
	}

}
