package com.leveldb.common.db;

import com.leveldb.common.Slice;
import com.leveldb.util.SequenceNumber;
import com.leveldb.util.ValueType;

public class ParsedInternalKey {
	public Slice user_key;
	public SequenceNumber sequence;
	public ValueType type;

	public ParsedInternalKey() {
	} // Intentionally left uninitialized (for speed)

	public ParsedInternalKey(Slice u, SequenceNumber seq, ValueType t) {
		user_key = u;
		sequence = seq;
		type = t;
	}

	public String DebugString() {
		return "'" + user_key + "' @ " + sequence + ": " + type;
	}

	public Slice getUser_key() {
		return user_key;
	}

	public void setUser_key(Slice user_key) {
		this.user_key = user_key;
	}

	public SequenceNumber getSequence() {
		return sequence;
	}

	public void setSequence(SequenceNumber sequence) {
		this.sequence = sequence;
	}

	public ValueType getType() {
		return type;
	}

	public void setType(ValueType type) {
		this.type = type;
	}

}
