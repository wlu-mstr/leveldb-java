package com.leveldb.common.db;

import java.util.concurrent.atomic.AtomicLong;

import com.leveldb.common.ByteCollection;
import com.leveldb.common.Iterator;
import com.leveldb.common.Slice;
import com.leveldb.common.Status;
import com.leveldb.common.PairComparable;
//import com.leveldb.common.Table;
import com.leveldb.common.comparator.InternalKeyComparator;
import com.leveldb.common.db.MemTable.KeyComparator;
import com.leveldb.util.SequenceNumber;
import com.leveldb.util.ValueType;
import com.leveldb.util.coding;
import com.leveldb.util.util;

class MemTableIterator extends com.leveldb.common.Iterator {

	private SkipListIterator<Slice, KeyComparator> iter_;
	String tmp_; // For passing to EncodeKey

	public MemTableIterator(SkipList<Slice, KeyComparator> table) {
		iter_ = new SkipListIterator<Slice, KeyComparator>(table);
	}

	@Override
	public boolean Valid() {
		return iter_.Valid();
	}

	@Override
	public void SeekToFirst() {
		iter_.SeekToFirst();

	}

	@Override
	public void SeekToLast() {
		iter_.SeekToLast();

	}

	// encode to s.size | s.data
	public byte[] EncodeKey(Slice target) {
		byte[] scratch = new byte[0];
		scratch = util.add(scratch, coding.PutVarint32(target.size()),
				target.data());
		return scratch;
	}

	@Override
	public void Seek(Slice k) {
		iter_.Seek(new Slice(EncodeKey(k)));

	}

	@Override
	public void Next() {
		iter_.Next();

	}

	@Override
	public void Prev() {
		iter_.Prev();

	}

	private ByteCollection l4key_value = null;

	@Override
	public Slice key() {
		l4key_value = new ByteCollection(iter_.key().data(), 0);
		return coding.GetLengthPrefixedSlice(l4key_value);// get
															// user-key|[sequencenumber
															// <<8 | type]
	}

	@Override
	public Slice value() {
		return coding.GetLengthPrefixedSlice(l4key_value);
	}

	@Override
	public Status status() {
		return Status.OK();
	}

}

// @ format of Internal_key: {user_key, squence number, type}
// Format of an entry is concatenation of:
// internal_key_size : varint32 of internal_key.size()
// internal_key bytes : byte[internal_key.size()]
// value_size : varint32 of value.size()
// value bytes : byte[value.size()]
public class MemTable {

	/**
	 * key.size|key.data|val.size|val.data
	 * 
	 * #wlu: 2012-5-29, Debug compare(Slice k, Slice k2)#
	 * 
	 * because bytes inserted to SkilList is internalkey_len|internal_key|...,
	 * we need to extract the internal key first before compare. internal_key:
	 * userkey|[sequencenumber << 8 | type]
	 */
	class KeyComparator extends PairComparable<Slice> {
		InternalKeyComparator comparator;

		KeyComparator(InternalKeyComparator c) {
			comparator = c;
		}

		int compare(byte[] a, byte[] b) {
			Slice as = coding.GetLengthPrefixedSlice(a);
			Slice bs = coding.GetLengthPrefixedSlice(b);
			return comparator.Compare(as, bs);
		}

		@Override
		public int compare(Slice k, Slice k2) {
			Slice as = coding.GetLengthPrefixedSlice(k.data());
			Slice bs = coding.GetLengthPrefixedSlice(k2.data());
			return comparator.Compare(as, bs);
		}
	}

	// MemTables are reference counted. The initial reference count
	// is zero and the caller must call Ref() at least once.
	public MemTable(InternalKeyComparator icomparator) {
		comparator_ = new KeyComparator(icomparator);
		refs_ = 0;
		table_ = new SkipList<Slice, KeyComparator>(comparator_);
	}

	// Increase reference count.
	public void Ref() {
		++refs_;
	}

	// Drop reference count. Delete if no more references exist.
	public void Unref() {
		--refs_;
		assert (refs_ >= 0);
		if (refs_ <= 0) {
			// this.finalize();
		}
	}

	// Returns an estimate of the number of bytes of data in use by this
	// data structure.
	//
	// REQUIRES: external synchronization to prevent simultaneous
	// operations on the same MemTable.
	public long ApproximateMemoryUsage() {
		return approximateMemoryUsage.get();
	}

	// Return an iterator that yields the contents of the memtable.
	//
	// The caller must ensure that the underlying MemTable remains live
	// while the returned iterator is live. The keys returned by this
	// iterator are internal keys encoded by AppendInternalKey in the
	// db/format.{h,cc} module.
	public Iterator NewIterator() {
		return new MemTableIterator(table_);
	}

	// Add an entry into memtable that maps key to value at the
	// specified sequence number and with the specified type.
	// Typically value will be empty if type==kTypeDeletion.
	public void Add(SequenceNumber seq, int /* ValueType */type, Slice key,
			Slice value) {
		// Format of an entry is concatenation of:
		// key_size : varint32 of internal_key.size()
		// key bytes : char[internal_key.size()]
		// value_size : varint32 of value.size()
		// value bytes : char[value.size()]
		int key_size = key.size();
		int val_size = value.size();
		int internal_key_size = key_size + 8;
		int encoded_len = coding.VarintLength(internal_key_size)
				+ internal_key_size + coding.VarintLength(val_size) + val_size;
		byte[] buf = new byte[0];
		// char* buf = arena_.Allocate(encoded_len);
		buf = util.addN(coding.EncodeVarint32(internal_key_size), key.data(),
				util.toBytes((long) (seq.value << 8) | type),
				coding.EncodeVarint32(val_size), value.data());
		// assert((p + val_size) - buf == encoded_len);
		assert (buf.length == encoded_len);
		table_.Insert(new Slice(buf));
		// update the size of memtable
		approximateMemoryUsage.addAndGet(buf.length);
	}

	// If memtable contains a value for key, store it in *value and return true.
	// If memtable contains a deletion for key, store a NotFound() error
	// in *status and return true.
	// Else, return false.
	public Slice Get(LookupKey key, Slice value, Status s) {
		Slice getValue;
		Slice memkey = key.memtable_key(); // whole slice data
		SkipListIterator<Slice, KeyComparator> iter = new SkipListIterator<Slice, KeyComparator>(
				table_);
		iter.Seek(memkey);
		if (iter.Valid()) {
			// entry format is:
			// klength varint32
			// userkey char[klength]
			// tag uint64
			// vlength varint32
			// value char[vlength]
			// Check that it belongs to same user key. We do not check the
			// sequence number since the Seek() call above should have skipped
			// all entries with overly large sequence numbers.
			Slice entry = iter.key();
			ByteCollection entry_ = new ByteCollection(entry.data(), 0);
			int key_length = coding.GetVarint32(entry_); // get the key length
			if (comparator_.comparator.user_comparator().Compare(
					new Slice(entry_.bytes, entry_.curr_pos, key_length - 8),
					key.user_key()) == 0) {
				// Correct user key
				entry_.curr_pos += (key_length - 8); // to the begin of the tag
				long tag = util.toLong(entry_.bytes, entry_.curr_pos);

				switch ((int) (tag & 0xff)) {
				case ValueType.kTypeValue: {
					entry_.curr_pos += 8;
					Slice v = coding.GetLengthPrefixedSlice(entry_);
					getValue = new Slice(v.data()); // deep copy
					// wlu, 2012-7-7
					value.setData_(getValue.data());
					return getValue;
				}
				case ValueType.kTypeDeletion:
					// has been deleted
					return null;
				}
			}
		}
		return null;
	}

	// SkipList<const char*, KeyComparator> Table;

	KeyComparator comparator_;
	int refs_;
	// Arena arena_;
	private final SkipList<Slice, KeyComparator> table_;
	private final AtomicLong approximateMemoryUsage = new AtomicLong(0);

	// No copying allowed
}
