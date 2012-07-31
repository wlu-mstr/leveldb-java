package com.leveldb.common.db;

import com.leveldb.common.Slice;
import com.leveldb.util.SequenceNumber;
import com.leveldb.util.ValueType;
import com.leveldb.util.coding;
import com.leveldb.util.util;

// A helper class useful for DBImpl::Get()
public class LookupKey {
	// kValueTypeForSeek defines the ValueType that should be passed when
	// constructing a ParsedInternalKey object for seeking to a particular
	// sequence number (since we sort sequence numbers in decreasing order
	// and the value type is embedded as the low 8 bits in the sequence
	// number in internal keys, we need to use the highest-numbered
	// ValueType, not the lowest).
	static ValueType kValueTypeForSeek = new ValueType(ValueType.kTypeValue);

	// Initialize *this for looking up user_key at a snapshot with
	// the specified sequence number.
	// wlu, 2012-7-7, bugFix: package of squence number and value type should be
	// encoded into fixed64 rather than
	// variant length int64
	public LookupKey(Slice user_key, SequenceNumber sequence) {
		int usize = user_key.size();
		// int needed = usize + 13; // A conservative estimate
		data = new byte[0];
		start_ = 0;
		byte len_[] = coding.EncodeVarint32(usize + 8);
		kstart_ = len_.length;

		data = util.addN(data, len_,// size|
				user_key.data(),// user_key|
				util.toBytes(InternalKey.PackSequenceAndType(sequence,
						kValueTypeForSeek)) // ...
				);

		end_ = data.length;
	}

	// Return a key suitable for lookup in a MemTable.
	// [ u_k.len + 8][ user_key ][ 8 bytes]
	// ^start_ |
	// ^kstart_ |
	// ^ ...
	// [ memtable_key ]
	// [ internal_key ]^end
	public Slice memtable_key() {
		return new Slice(data, start_, end_ - start_);
	}

	// Return an internal key (suitable for passing to an internal iterator)
	public Slice internal_key() {
		return new Slice(data, kstart_, end_ - kstart_);
	}

	// Return the user key
	public Slice user_key() {
		return new Slice(data, kstart_, end_ - kstart_ - 8);
	}

	// We construct a char array of the form:
	// klength varint32 <-- start_
	// userkey char[klength] <-- kstart_
	// tag uint64
	// <-- end_
	// The array is a suitable MemTable key.
	// The suffix starting with "userkey" can be used as an InternalKey.
	byte[] data;
	int start_;
	int kstart_;
	int end_;
	// char space_[200]; // Avoid allocation for short keys

}
