package com.leveldb.common.db;

import com.leveldb.common.Slice;

public class Range {
	public Slice start; // Included in the range
	public Slice limit; // Not included in the range

	public Range(Slice s, Slice l) {
		setStart(s);
		setLimit(l);
	}

	public void setStart(Slice start) {
		this.start = start;
	}

	public Slice getStart() {
		return start;
	}

	public void setLimit(Slice limit) {
		this.limit = limit;
	}

	public Slice getLimit() {
		return limit;
	}
	
}