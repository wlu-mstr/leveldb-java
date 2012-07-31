package com.leveldb.common.table;

import com.leveldb.common.Iterator;
import com.leveldb.common.Slice;
import com.leveldb.common.Status;

//A internal wrapper class with an interface similar to Iterator that
//caches the valid() and key() results for an underlying iterator.
//This can help avoid virtual function calls and also gives better
//cache locality.

public class IteratorWrapper {
	public IteratorWrapper() {
		iter_ = null;
		valid_ = false;
	}

	public IteratorWrapper(Iterator iter) {
		iter_ = null;
		Set(iter);
	}

	// ~IteratorWrapper() { delete iter_; }
	public Iterator iter() {
		return iter_;
	}

	// Takes ownership of "iter" and will delete it when destroyed, or
	// when Set() is invoked again.
	public void Set(Iterator iter) {
		// delete iter_;
		iter_ = iter;
		if (iter_ == null) {
			valid_ = false;
		} else {
			Update();
		}
	}

	// Iterator interface methods
	public boolean Valid() {
		return valid_;
	}

	public Slice key() {
		assert (Valid());
		return key_;
	}

	public Slice value() {
		assert (Valid());
		return iter_.value();
	}

	// Methods below require iter() != NULL
	public Status status() {
		assert (iter_ != null);
		return iter_.status();
	}

	public void Next() {
		assert (iter_ != null);
		iter_.Next();
		Update();
	}

	public void Prev() {
		assert (iter_ != null);
		iter_.Prev();
		Update();
	}

	public void Seek(Slice k) {
		assert (iter_ != null);
		iter_.Seek(k);
		Update();
	}

	public void SeekToFirst() {
		assert (iter_ != null);
		iter_.SeekToFirst();
		Update();
	}

	public void SeekToLast() {
		assert (iter_ != null);
		iter_.SeekToLast();
		Update();
	}

	private void Update() {
		valid_ = iter_.Valid();
		if (valid_) {
			key_ = iter_.key();
		}
	}

	private Iterator iter_;
	private boolean valid_;
	private Slice key_;
}
