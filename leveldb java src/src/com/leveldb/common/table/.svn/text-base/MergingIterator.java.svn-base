package com.leveldb.common.table;

import com.leveldb.common.Comparator;
import com.leveldb.common.Iterator;
import com.leveldb.common.Slice;
import com.leveldb.common.Status;

/**
 * This is an Iterator over an array of Iterators, To efficiently do merge, the
 * Iterator will always get the smallest inner iterator
 * 
 * @author wlu 2012-4-19
 * 
 */
public class MergingIterator extends Iterator {

	// We might want to use a heap in case there are lots of children.
	// For now we use a simple array since we expect a very small number
	// of children in leveldb.
	private Comparator comparator_;
	private IteratorWrapper[] children_;
	private int n_;
	private IteratorWrapper current_;

	private Direction direction_;

	public MergingIterator(Comparator comparator, Iterator children[], int n) {
		comparator_ = comparator;
		children_ = new IteratorWrapper[n];
		n_ = n;
		current_ = null;
		direction_ = Direction.kForward;
		for (int i = 0; i < n; i++) {
			children_[i] = new IteratorWrapper();
			children_[i].Set(children[i]);
		}
	}

	public boolean Valid() {
		return (current_ != null);
	}

	public void SeekToFirst() {
		for (int i = 0; i < n_; i++) {
			children_[i].SeekToFirst();
		}
		FindSmallest();
		direction_ = Direction.kForward;
	}

	public void SeekToLast() {
		for (int i = 0; i < n_; i++) {
			children_[i].SeekToLast();
		}
		FindLargest();
		direction_ = Direction.kReverse;
	}

	public void Seek(Slice target) {
		for (int i = 0; i < n_; i++) {
			children_[i].Seek(target);
		}
		FindSmallest();
		direction_ = Direction.kForward;
	}

	public void Next() {
		assert (Valid());

		// Ensure that all children are positioned after key().
		// If we are moving in the forward direction, it is already
		// true for all of the non-current_ children since current_ is
		// the smallest child and key() == current_->key(). Otherwise,
		// we explicitly position the non-current_ children.
		if (direction_ != Direction.kForward) {
			for (int i = 0; i < n_; i++) {
				IteratorWrapper child = children_[i];
				if (child != current_) {
					child.Seek(key());
					if (child.Valid()
							&& comparator_.Compare(key(), child.key()) == 0) {
						child.Next();
					}
				}
			}
			direction_ = Direction.kForward;
		}

		current_.Next();
		FindSmallest();
	}

	public void Prev() {
		assert (Valid());

		// Ensure that all children are positioned before key().
		// If we are moving in the reverse direction, it is already
		// true for all of the non-current_ children since current_ is
		// the largest child and key() == current_->key(). Otherwise,
		// we explicitly position the non-current_ children.
		if (direction_ != Direction.kReverse) {
			for (int i = 0; i < n_; i++) {
				IteratorWrapper child = children_[i];
				if (child != current_) {
					child.Seek(key());
					if (child.Valid()) {
						// Child is at first entry >= key(). Step back one to be
						// < key()
						child.Prev();
					} else {
						// Child has no entries >= key(). Position at last
						// entry.
						child.SeekToLast();
					}
				}
			}
			direction_ = Direction.kReverse;
		}

		current_.Prev();
		FindLargest();
	}

	public Slice key() {
		assert (Valid());
		return current_.key();
	}

	public Slice value() {
		assert (Valid());
		return current_.value();
	}

	public Status status() {
		Status status = Status.OK();
		for (int i = 0; i < n_; i++) {
			status = children_[i].status();
			if (!status.ok()) {
				break;
			}
		}
		return status;
	}

	void FindSmallest() {
		IteratorWrapper smallest = null;
		for (int i = 0; i < n_; i++) {
			IteratorWrapper child = children_[i];
			if (child.Valid()) {
				if (smallest == null) {
					smallest = child;
				} else if (comparator_.Compare(child.key(), smallest.key()) < 0) {
					smallest = child;
				}
			}
		}
		current_ = smallest;
	}

	void FindLargest() {
		IteratorWrapper largest = null;
		for (int i = n_ - 1; i >= 0; i--) {
			IteratorWrapper child = children_[i];
			if (child.Valid()) {
				if (largest == null) {
					largest = child;
				} else if (comparator_.Compare(child.key(), largest.key()) > 0) {
					largest = child;
				}
			}
		}
		current_ = largest;
	}

	// Which direction is the iterator moving?
	enum Direction {
		kForward, kReverse
	}

	/**
	 * Return an iterator that provided the union of the data in
	 * children[0,n-1]. Takes ownership of the child iterators and will delete
	 * them when the result iterator is deleted.
	 * 
	 * The result does no duplicate suppression. I.e., if a particular key is
	 * present in K child iterators, it will be yielded K times.
	 * 
	 * REQUIRES: n >= 0
	 */
	public static Iterator NewMergingIterator(Comparator cmp, Iterator list[], int n) {
		assert (n >= 0);
		if (n == 0) {
			return NewEmptyIterator();
		} else if (n == 1) {
			return list[0];
		} else {
			return new MergingIterator(cmp, list, n);
		}
	}

}
