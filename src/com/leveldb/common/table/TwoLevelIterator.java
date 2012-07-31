package com.leveldb.common.table;

import com.leveldb.common.Iterator;
import com.leveldb.common.Slice;
import com.leveldb.common.Status;
import com.leveldb.common.options.ReadOptions;

/*
 * wlu: 2012-4-9 @Home
 */
public class TwoLevelIterator extends Iterator {
	public interface BlockFunction {
		// public Object arg;
		// public ReadOptions readoption;
		// public Slice slice;

		public Iterator exec(Object arg, ReadOptions readoption, Slice slice);
		// Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&)
	}

	// Members
	private BlockFunction block_function_;
	Object arg_;
	ReadOptions options_;
	Status status_ = new Status();
	private IteratorWrapper index_iter_;
	private IteratorWrapper data_iter_; // May be NULL
	// If data_iter_ is non-NULL, then "data_block_handle_" holds the
	// "index_value" passed to block_function_ to create the data_iter_.
	private byte[] data_block_handle_;

	/**
	 * Construction
	 * 
	 * @param index_iter
	 *            (outer iterator) is wrappered to be IteratorWrapper
	 * @param block_function
	 *            Guess: get inner iterator according to outer iterator's value
	 *            from a cache
	 * @param arg
	 * @param options
	 *            (inner iterator) is set as null
	 */
	public TwoLevelIterator(Iterator index_iter, BlockFunction block_function,
			Object arg, ReadOptions options) {
		block_function_ = block_function;
		arg_ = arg;
		options_ = options;
		index_iter_ = new IteratorWrapper(index_iter); // wrapper the Iterator
		data_iter_ = new IteratorWrapper(null);
	}

	// (outer) coarse level: seek to a "range"
	// set data_iter_ to the coarse range
	// (inner) fine level: seek to ...
	public void Seek(Slice target) {
		index_iter_.Seek(target);
		InitDataBlock();
		if (data_iter_.iter() != null) {
			data_iter_.Seek(target);
		}
		SkipEmptyDataBlocksForward();
	}

	// (outer) index_iter_ seek to first
	// set data_iter_ to index_iter_'s value as inner iterator
	// (inner) data_iter_ seek to the first
	// move next and skip empty/invalid ...
	public void SeekToFirst() {
		index_iter_.SeekToFirst();
		InitDataBlock();
		if (data_iter_.iter() != null) {
			data_iter_.SeekToFirst();
		}
		SkipEmptyDataBlocksForward(); // skip the first invalid ones
	}

	// (outer) index_iter_ seek to last
	// set data_iter_ to index_iter_'s value as inner iterator
	// (inner) data_iter_ seek to the last
	// move prev and skip empty/invalid ...
	public void SeekToLast() {
		index_iter_.SeekToLast();
		InitDataBlock();
		if (data_iter_.iter() != null) {
			data_iter_.SeekToLast();
		}
		SkipEmptyDataBlocksBackward(); // skip the last invalid ones
	}

	// data_iter_'s next and skip empty/invalid by moving next...
	public void Next() {
		assert (Valid());
		data_iter_.Next();
		SkipEmptyDataBlocksForward();
	}

	// data_iter_'s prev and skip empty/invalid by moving prev...
	public void Prev() {
		assert (Valid());
		data_iter_.Prev();
		SkipEmptyDataBlocksBackward();
	}

	// whether data_iter_ is valid
	public boolean Valid() {
		return data_iter_.Valid();
	}

	// return data_iter_'s key
	public Slice key() {
		assert (Valid());
		return data_iter_.key();
	}

	// return data_iter_'s vlue
	public Slice value() {
		assert (Valid());
		return data_iter_.value();
	}

	// ...
	public Status status() {
		// It'd be nice if status() returned a const Status& instead of a Status
		if (!index_iter_.status().ok()) {
			return index_iter_.status();
		} else if (data_iter_.iter() != null && !data_iter_.status().ok()) {
			return data_iter_.status();
		} else {
			return status_;
		}
	}

	// save s to member status_
	private void SaveError(Status s) {
		// wlu, 2012-7-10, bugFix:  s!= null
		if (status_.ok() && s!= null && !s.ok())
			status_ = s;
	}

	// skip empty/invalid iterator and move to the next one
	private void SkipEmptyDataBlocksForward() {
		while (data_iter_.iter() == null || !data_iter_.Valid()) {
			// Move to next block
			if (!index_iter_.Valid()) {
				SetDataIterator(null);
				return;
			}
			index_iter_.Next();
			InitDataBlock();
			if (data_iter_.iter() != null) {
				data_iter_.SeekToFirst();
			}
		}
	}

	// skip empty/invalid iterator and move to the previous one
	private void SkipEmptyDataBlocksBackward() {
		while (data_iter_.iter() == null || !data_iter_.Valid()) {
			// Move to next block
			if (!index_iter_.Valid()) {
				SetDataIterator(null);
				return;
			}
			index_iter_.Prev();
			InitDataBlock();
			if (data_iter_.iter() != null) {
				data_iter_.SeekToLast();
			}
		}
	}

	// Set data_iter_ to be input parameter
	private void SetDataIterator(Iterator data_iter) {
		if (data_iter_.iter() != null) {
			SaveError(data_iter_.status());
		}
		data_iter_.Set(data_iter);
	}

	/**
	 * 1) get outer iterator's #value#; 2) get the iterator #iter# of the one
	 * inner the #value#; 3) set data_block_handle_ and set #iter# to data_iter_
	 */
	private void InitDataBlock() {
		if (!index_iter_.Valid()) {
			SetDataIterator(null);
		} else {
			Slice handle = index_iter_.value();
			// already done
			if (data_iter_.iter() != null
					&& handle.compareTo(new Slice(data_block_handle_)) == 0) {
				// data_iter_ is already constructed with this iterator, so
				// no need to change anything
			} else {
				Iterator iter = block_function_.exec(arg_, options_, handle);
				// data_block_handle_.assign(handle.data(), handle.size());//
				// assign the first n(all) bytes of ... to ...
				data_block_handle_ = handle.data();
				SetDataIterator(iter);
			}
		}
	}

	// factory
	public static Iterator NewTwoLevelIterator(Iterator index_iter,
			BlockFunction block_function, Object arg, ReadOptions options) {
		return new TwoLevelIterator(index_iter, block_function, arg, options);
	}

}
