package com.leveldb.common;

import com.leveldb.common.comparator.BytewiseComparatorImpl;

public abstract class Comparator {

	// Three-way comparison. Returns value:
	// < 0 iff "a" < "b",
	// == 0 iff "a" == "b",
	// > 0 iff "a" > "b"
	public abstract int Compare(Slice a, Slice b);

	// The name of the comparator. Used to check for comparator
	// mismatches (i.e., a DB created with one comparator is
	// accessed using a different comparator.
	//
	// The client of this package should switch to a new name whenever
	// the comparator implementation changes in a way that will cause
	// the relative ordering of any two keys to change.
	//
	// Names starting with "leveldb." are reserved and should not be used
	// by any clients of this package.
	public abstract String Name();

	// Advanced functions: these are used to reduce the space requirements
	// for internal data structures like index blocks.

	// If *start < limit, changes *start to a short string in [start,limit).
	// Simple comparator implementations may return with *start unchanged,
	// i.e., an implementation of this method that does nothing is correct.
	public abstract byte[] FindShortestSeparator(byte[] start, Slice limit);

	// Changes *key to a short string >= *key.
	// Simple comparator implementations may return with *key unchanged,
	// i.e., an implementation of this method that does nothing is correct.
	public abstract byte[] FindShortSuccessor(byte[] last_key);

	// Return a builtin comparator that uses lexicographic byte-wise
	// ordering. The result remains the property of this module and
	// must not be deleted.
	public static Comparator BytewiseComparator() {
		return new BytewiseComparatorImpl();
	}

}