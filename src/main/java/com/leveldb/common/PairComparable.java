package com.leveldb.common;

public abstract class PairComparable<Key> {
	public abstract int compare(Key k, Key k2);
}
