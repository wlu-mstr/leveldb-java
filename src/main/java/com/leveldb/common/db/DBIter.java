package com.leveldb.common.db;

import com.leveldb.common.ByteVector;
import com.leveldb.common.Comparator;
import com.leveldb.common.Env;
import com.leveldb.common.Iterator;
import com.leveldb.common.Slice;
import com.leveldb.common.Status;
import com.leveldb.util.SequenceNumber;
import com.leveldb.util.ValueType;

//Memtables and sstables that make the DB representation contain
//(userkey,seq,type) => uservalue entries.  DBIter
//combines multiple entries for the same userkey found in the DB
//representation into a single entry while accounting for sequence
//numbers, deletion markers, overwrites, etc.
class DBIter extends Iterator {
	String dbname_;
	Env env_;
	Comparator user_comparator_;
	Iterator iter_;
	SequenceNumber sequence_;

	Status status_;
	ByteVector saved_key_; // == current key when direction_==kReverse
	ByteVector saved_value_; // == current raw value when direction_==kReverse
	int direction_;
	boolean valid_;
	
	// Which direction is the iterator currently moving?
	// (1) When moving forward, the internal iterator is positioned at
	// the exact entry that yields this->key(), this->value()
	// (2) When moving backwards, the internal iterator is positioned
	// just before all entries whose user key == this->key().
	static class Direction {
		public static final int kForward = 0;
		public static final int kReverse = 1;
	}

	public DBIter(String dbname, Env env, Comparator cmp, Iterator iter,
			SequenceNumber s) {
		dbname_ = dbname;
		env_ = env;
		user_comparator_ = cmp;
		iter_ = iter;
		sequence_ = s;
		direction_ = Direction.kForward;
		valid_ = false;
		saved_key_ = new ByteVector(); 
		saved_value_ = new ByteVector();
	}

	public boolean Valid() {
		return valid_;
	}

	public Slice key() {
		assert (valid_);
		return (direction_ == Direction.kForward) ? InternalKey
				.ExtractUserKey(iter_.key()) : new Slice(saved_key_.getData());
	}

	public Slice value() {
		assert (valid_);
		return (direction_ == Direction.kForward) ? iter_.value() : new Slice(
				saved_value_.getData());
	}

	public Status status() {
		if (status_.ok()) {
			return iter_.status();
		} else {
			return status_;
		}
	}

	void SaveKey(Slice k, ByteVector dst) {
		dst.set(k.data());
	}

	void ClearSavedValue() {
		if (saved_value_.getCapacity() > 1048576) {
			saved_value_ = new ByteVector();
		} else {
			saved_value_.clear();
		}
	}

	// No copying allowed

	ParsedInternalKey ParseKey() {
		ParsedInternalKey lkey = InternalKey.ParseInternalKey_(iter_.key());
		if (lkey == null) {
			status_ = Status.Corruption(new Slice(
					"corrupted internal key in DBIter"), null);
			return null;
		} else {
			return lkey;
		}
	}

	public void Next() {
		assert (valid_);

		if (direction_ == Direction.kReverse) { // Switch directions?
			direction_ = Direction.kForward;
			// iter_ is pointing just before the entries for this->key(),
			// so advance into the range of entries for this->key() and then
			// use the normal skipping code below.
			if (!iter_.Valid()) {
				iter_.SeekToFirst();
			} else {
				iter_.Next();
			}
			if (!iter_.Valid()) {
				valid_ = false;
				saved_key_.clear();
				return;
			}
		}

		// Temporarily use saved_key_ as storage for key to skip.
		ByteVector skip = saved_key_;
		SaveKey(InternalKey.ExtractUserKey(iter_.key()), skip);
		FindNextUserEntry(true, skip);
	}

	void FindNextUserEntry(boolean skipping, ByteVector skip) {
		// Loop until we hit an acceptable entry to yield
		assert (iter_.Valid());
		assert (direction_ == Direction.kForward);
		do {
			ParsedInternalKey ikey = ParseKey();
			if (ikey.sequence.value <= sequence_.value) {
				switch (ikey.type.value) {
				case ValueType.kTypeDeletion:
					// Arrange to skip all upcoming entries for this key since
					// they are hidden by this deletion.
					SaveKey(ikey.user_key, skip);
					skipping = true;
					break;
				case ValueType.kTypeValue:
					if (skipping
							&& user_comparator_.Compare(
									ikey.user_key,
									new Slice(skip.getRawRef(), 0, skip
											.getSize())) <= 0) {
						// Entry hidden
					} else {
						valid_ = true;
						saved_key_.clear();
						return;
					}
					break;
				}
			}
			iter_.Next();
		} while (iter_.Valid());
		saved_key_.clear();
		valid_ = false;
	}

	public void Prev() {
		assert (valid_);

		if (direction_ == Direction.kForward) { // Switch directions?
			// iter_ is pointing at the current entry. Scan backwards until
			// the key changes so we can use the normal reverse scanning code.
			assert (iter_.Valid()); // Otherwise valid_ would have been false
			SaveKey(InternalKey.ExtractUserKey(iter_.key()), saved_key_);
			while (true) {
				iter_.Prev();
				if (!iter_.Valid()) {
					valid_ = false;
					saved_key_.clear();
					ClearSavedValue();
					return;
				}
				if (user_comparator_.Compare(InternalKey.ExtractUserKey(iter_
						.key()), new Slice(saved_key_.getRawRef(), 0,
						saved_key_.getSize())) < 0) {
					break;
				}
			}
			direction_ = Direction.kReverse;
		}

		FindPrevUserEntry();
	}

	void FindPrevUserEntry() {
		assert (direction_ == Direction.kReverse);

		ValueType value_type = ValueType.TypeDeletion;
		if (iter_.Valid()) {
			do {
				ParsedInternalKey ikey = ParseKey();
				if (ikey.sequence.value <= sequence_.value) {
					if ((value_type != ValueType.TypeDeletion)
							&& user_comparator_.Compare(ikey.user_key,
									new Slice(saved_key_.getRawRef(), 0,
											saved_key_.getSize())) < 0) {
						// We encountered a non-deleted value in entries for
						// previous keys,
						break;
					}
					value_type = ikey.type;
					if (value_type.value == ValueType.kTypeDeletion) {
						saved_key_.clear();
						ClearSavedValue();
					} else {
						Slice raw_value = iter_.value();
						if (saved_value_.getCapacity() > raw_value.size() + 1048576) {
							saved_value_ = new ByteVector();
						}
						SaveKey(InternalKey.ExtractUserKey(iter_.key()),
								saved_key_);
						saved_value_.set(raw_value.data());
					}
				}
				iter_.Prev();
			} while (iter_.Valid());
		}

		if (value_type.value == ValueType.kTypeDeletion) {
			// End
			valid_ = false;
			saved_key_.clear();
			ClearSavedValue();
			direction_ = Direction.kForward;
		} else {
			valid_ = true;
		}
	}

	public void Seek(Slice target) {
		direction_ = Direction.kForward;
		ClearSavedValue();
		saved_key_.clear();
		// wlu:Debug issue, 2012-5-29: need to set new bytes to saved_key_
		saved_key_.set(InternalKey.AppendInternalKey(saved_key_.getData(),
				new ParsedInternalKey(target, sequence_,
						ValueType.ValueTypeForSeek)));
		iter_.Seek(new Slice(saved_key_.getRawRef(), 0, saved_key_.getSize()));
		if (iter_.Valid()) {
			FindNextUserEntry(false, saved_key_ /* temporary storage */);
		} else {
			valid_ = false;
		}
	}

	public void SeekToFirst() {
		direction_ = Direction.kForward;
		ClearSavedValue();
		iter_.SeekToFirst();
		if (iter_.Valid()) {
			FindNextUserEntry(false, saved_key_ /* temporary storage */);
		} else {
			valid_ = false;
		}
	}

	public void SeekToLast() {
		direction_ = Direction.kReverse;
		ClearSavedValue();
		iter_.SeekToLast();
		FindPrevUserEntry();
	}

	public static Iterator NewDBIterator(String dbname, Env env,
			Comparator user_key_comparator, Iterator internal_iter,
			SequenceNumber sequence) {
		return new DBIter(dbname, env, user_key_comparator, internal_iter,
				sequence);
	}
}
