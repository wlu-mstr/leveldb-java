package com.leveldb.util;

public class SequenceNumber {
	// We leave eight bits empty at the bottom so a type and sequence#
	// can be packed together into 64-bits.
	// [Note:] Max value is not 0x1l << 56 - 1 in java. Because java
	// does not support unsigned long
	public static long kMaxSequenceNumber = ((0x1l << 55) - 1);

	public static SequenceNumber MaxSequenceNumber = new SequenceNumber(
			kMaxSequenceNumber);

	public long value;

	public SequenceNumber(long v) {
		assert (v <= kMaxSequenceNumber);
		value = v;
	}

	public String toString() {
		return "Sequence: " + value + ".";
	}
}
