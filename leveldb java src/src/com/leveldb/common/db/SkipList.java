package com.leveldb.common.db;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.leveldb.common.AtomicPointer;
import com.leveldb.common._Comparable;
import com.leveldb.common.comparator.InternalKeyComparator;

class Node<Key> {
	@SuppressWarnings("unchecked")
	Node(Key k, int iMaxHeight) {
		key = k;
		next_ = new AtomicPointer[iMaxHeight];
		for (int i = 0; i < iMaxHeight; i++) {
			next_[i] = new AtomicPointer<Node<Key>>(null);
		}
	}

	Key key;

	// Accessors/mutators for links. Wrapped in methods so we can
	// add the appropriate barriers as necessary.
	Node<Key> Next(int n) {
		assert (n >= 0);
		// Use an 'acquire load' so that we observe a fully initialized
		// version of the returned Node.
		return next_[n].Acquire_Load();
	}

	void SetNext(int n, Node<Key> x) {
		assert (n >= 0);
		// Use a 'release store' so that anybody who reads through this
		// pointer observes a fully initialized version of the inserted
		// node.
		next_[n].Release_Store(x);
	}

	// No-barrier variants that can be safely used in a few locations.
	Node<Key> NoBarrier_Next(int n) {
		assert (n >= 0);
		return next_[n].NoBarrier_Load();
	}

	void NoBarrier_SetNext(int n, Node<Key> x) {
		assert (n >= 0);
		next_[n].NoBarrier_Store(x);
	}

	public String toString() {
		String s = "[" + key.toString() + "]";
		int idx = 0;
		for (AtomicPointer<Node<Key>> n : next_) {
			if (n.Acquire_Load() != null) {
				s += (idx + ": " + n.Acquire_Load().toString());
			}
			idx += 1;
		}
		return s;
	}

	// Array of length equal to the node height. next_[0] is lowest level
	// link.
	AtomicPointer<Node<Key>> next_[];

}

/**
 * donot need to extend Common.Iterator
 * 
 * @author wlu
 * 
 * @param <Key>
 * @param <Comparator>
 */
class SkipListIterator<Key, Comparator extends _Comparable<Key>> {
	// Initialize an iterator over the specified list.
	// The returned iterator is not valid.
	public SkipListIterator(SkipList<Key, Comparator> list) {
		list_ = list;
		node_ = null;
	}

	// Returns true iff the iterator is positioned at a valid node.
	boolean Valid() {
		return node_ != null;
	}

	// Returns the key at the current position.
	// REQUIRES: Valid()
	Key key() {
		assert (Valid());
		if(node_ == null){
			@SuppressWarnings("unused")
			int d = 0;
		}
		return node_.key;
	}

	// Advances to the next position.
	// REQUIRES: Valid()
	void Next() {
		assert (Valid());
		node_ = node_.Next(0);
	}

	// Advances to the previous position.
	// REQUIRES: Valid()
	void Prev() {
		// Instead of using explicit "prev" links, we just search for the
		// last node that falls before key.
		assert (Valid());
		node_ = list_.FindLessThan(node_.key);
		if (node_ == list_.head_) {
			node_ = null;
		}
	}

	// Advance to the first entry with a key >= target
	void Seek(Key target) {
		node_ = list_.FindGreaterOrEqual(target, null);
	}

	// Position at the first entry in list.
	// Final state of iterator is Valid() iff list is not empty.
	void SeekToFirst() {
		node_ = list_.head_.Next(0);
	}

	// Position at the last entry in list.
	// Final state of iterator is Valid() iff list is not empty.
	void SeekToLast() {
		node_ = list_.FindLast();
		if (node_ == list_.head_) {
			node_ = null;
		}
	}

	SkipList<Key, Comparator> list_;
	Node<Key> node_;
	// Intentionally copyable
}

public class SkipList<Key, Comparator extends _Comparable<Key>> {

	// parameters
	private final int kMaxHeight = 12;

	// Immutable after construction
	// Arena* const arena_; // Arena used for allocations of nodes

	Node<Key> head_;

	// Modified only by Insert(). Read racily by readers, but stale
	// values are ok.
	AtomicPointer<Integer> max_height_; // Height of the entire list

	Comparator compare_;

	private int GetMaxHeight() {
		return (max_height_.NoBarrier_Load());
	}

	public Node<Key> FindLast() {
		Node<Key> x = head_;
		int level = GetMaxHeight() - 1;
		while (true) {
			Node<Key> next = x.Next(level);
			if (next == null) {
				if (level == 0) {
					return x;
				} else {
					// Switch to next list
					level--;
				}
			} else {
				x = next;
			}
		}
	}

	// Read/written only by Insert().
	Random rnd_;

	/* construction */
	public SkipList(Comparator icomparator) {
		compare_ = icomparator;
		head_ = new Node<Key>(null /* any key will do */, kMaxHeight); // TODO
																		// need
		// more
		// consideration
		max_height_ = new AtomicPointer<Integer>(1);
		rnd_ = new Random();
		for (int i = 0; i < kMaxHeight; i++) {
			head_.SetNext(i, null);
		}
	}

	// find the node that is less than key
	public Node<Key> FindLessThan(Key key) {
		Node<Key> x = head_;
		int level = GetMaxHeight() - 1;
		while (true) {
			assert (x == head_ || compare_.compare(x.key, key) < 0);
			Node<Key> next = x.Next(level);
			if (next == null || compare_.compare(next.key, key) >= 0) {
				if (level == 0) {
					return x;
				} else {
					// Switch to next list
					level--;
				}
			} else {
				x = next;
			}
		}
	}

	/* Insert a key into the list */
	public void Insert(Key key) {
		List<Node<Key>> prev = new ArrayList<Node<Key>>(kMaxHeight);
		for (int i = 0; i < kMaxHeight; i++) {
			prev.add(new Node<Key>(null, kMaxHeight));
		}
		Node<Key> x = FindGreaterOrEqual(key, prev);
		assert (x == null || compare_.compare(key, x.key) != 0);
		int height = RandomHeight();
		if (height > GetMaxHeight()) {
			for (int i = GetMaxHeight(); i < height; i++) {
				prev.set(i, head_);
			}
			// It is ok to mutate max_height_ without any synchronization
			// with concurrent readers. A concurrent reader that observes
			// the new value of max_height_ will see either the old value of
			// new level pointers from head_ (NULL), or a new value set in
			// the loop below. In the former case the reader will
			// immediately drop to the next level since NULL sorts after all
			// keys. In the latter case the reader will use the new node.
			max_height_.NoBarrier_Store(height);
		}

		x = new Node<Key>(key, height);
		for (int i = 0; i < height; i++) {
			// NoBarrier_SetNext() suffices since we will add a barrier when
			// we publish a pointer to "x" in prev[i].
			x.NoBarrier_SetNext(i, prev.get(i).NoBarrier_Next(i));
			prev.get(i).SetNext(i, x);
		}

	}

	// whether key is contained in the list
	public boolean Contains(Key key) {
		Node<Key> x = FindGreaterOrEqual(key, null);
		if (x != null && compare_.compare(key, x.key) == 0) {
			return true;
		} else {
			return false;
		}
	}

	// randomly generate height for a node
	private int RandomHeight() {
		// increase length by i with probability (0.25)^(i-1) * (0.75)
		int kBranching = 4;
		int height = 1;
		while (height < kMaxHeight
				&& ((Math.abs(rnd_.nextInt()) % kBranching) == 0)) {
			height++;
		}
		assert (height > 0);
		assert (height <= kMaxHeight);
		return height;
	}

	// find and return the node whose key is >= input key;
	// set prev[0] to be the node right before the returned one (
	// to be as the node's previous). it is interesting to find the
	// return statement is ...
	public Node<Key> FindGreaterOrEqual(Key key, List<Node<Key>> prev) {
		Node<Key> x = head_;
		int level = GetMaxHeight() - 1;
		while (true) {
			Node<Key> next = x.Next(level);
			if (KeyIsAfterNode(key, next)) {
				// Keep searching in this list
				x = next;
			} else {
				if (level == 0) {
					if (prev != null)
						prev.set(level, x);
					return next;
				} else {
					// Switch to next list
					level--;
				}
			}
		}
	}

	private boolean KeyIsAfterNode(Key key, Node<Key> n) {
		// NULL n is considered infinite
		return (n != null) && compare_.compare(n.key, key) < 0;
	}

	boolean Equal(Key a, Key b) {
		return compare_.compare(a, b) == 0;
	}

	// /////////////////////////////////////////////////////////
	// / test cases
	static class TestComparator extends _Comparable<Integer> {

		@Override
		public int compare(Integer a, Integer b) {
			if (a < b) {
				return -1;
			} else if (a > b) {
				return +1;
			} else {
				return 0;
			}
		}
	}
	
	static class TestInternalKeyComparator extends _Comparable<InternalKey>{

		@Override
		public int compare(InternalKey k, InternalKey k2) {
			// TODO Auto-generated method stub
			return 0;
		}
		
	}

	static class SkiplistTest {
		void ASSERT_TRUE(boolean b) {
			if (!b) {
				System.out.println(b);
			}
		}

		void ASSERT_EQ(int a, int b) {
			if (a != b) {
				System.out.println(a == b);
			}
		}

		void Empty() {
			TestComparator cmp = new TestComparator();
			SkipList<Integer, TestComparator> list = new SkipList<Integer, TestComparator>(
					cmp);
			ASSERT_TRUE(!list.Contains(10));

			SkipListIterator<Integer, TestComparator> iter = new SkipListIterator<Integer, TestComparator>(
					list);
			ASSERT_TRUE(!iter.Valid());
			iter.SeekToFirst();
			ASSERT_TRUE(!iter.Valid());
			iter.Seek(100);
			ASSERT_TRUE(!iter.Valid());
			iter.SeekToLast();
			ASSERT_TRUE(!iter.Valid());
		}

		void InsertAndLookup() {

			class re_integer implements Comparable<re_integer> {
				int val;

				public re_integer(int v) {
					val = v;
				}

				@Override
				public int compareTo(re_integer arg0) {
					return arg0.val - val;
				}

			}
			int N = 2000;
			int R = 5000;
			Random rnd = new Random();
			// baseline
			SortedSet<Integer> keys = new TreeSet<Integer>();
			SortedSet<re_integer> keys_reverse = new TreeSet<re_integer>();
			TestComparator cmp = new TestComparator();
			SkipList<Integer, TestComparator> list = new SkipList<Integer, SkipList.TestComparator>(
					cmp);
			for (int i = 0; i < N; i++) {
				int key = rnd.nextInt() % R;
				if (keys.add(key)) {
					keys_reverse.add(new re_integer(key));
					// System.out.println(key);
					list.Insert(key);
				}
			}

			for (int i = 0; i < R; i++) {
				if (list.Contains(i)) {
					ASSERT_TRUE(keys.contains(i));
				} else {
					ASSERT_TRUE(!keys.contains(i));
				}
			}

			// Simple iterator tests
			{
				SkipListIterator<Integer, TestComparator> iter = new SkipListIterator<Integer, SkipList.TestComparator>(
						list);
				ASSERT_TRUE(!iter.Valid());

				iter.Seek(Integer.MIN_VALUE);// different from cpp, 'cause java
												// doesnot support uint64
				ASSERT_TRUE(iter.Valid());
				ASSERT_EQ(keys.first(), iter.key());

				iter.SeekToFirst();
				ASSERT_TRUE(iter.Valid());
				ASSERT_EQ(keys.first(), iter.key());

				iter.SeekToLast();
				ASSERT_TRUE(iter.Valid());
				ASSERT_EQ(keys.last(), iter.key());
			}

			// Forward iteration test
			for (int i = 0; i < R; i++) {
				SkipListIterator<Integer, TestComparator> iter = new SkipListIterator<Integer, SkipList.TestComparator>(
						list);
				iter.Seek(i);

				// Compare against model iterator
				// std::set<Key>::iterator model_iter = keys.lower_bound(i);
				SortedSet<Integer> model_ = keys.tailSet(i);
				Iterator<Integer> model_iter = model_.iterator();
				for (int j = 0; j < 3; j++) {
					if (!model_iter.hasNext()) {
						ASSERT_TRUE(!iter.Valid());
						break;
					} else {
						ASSERT_TRUE(iter.Valid());
						ASSERT_EQ(model_iter.next(), iter.key());
						iter.Next();
					}
				}
			}

			// Backward iteration test
			{
				SkipListIterator<Integer, TestComparator> iter = new SkipListIterator<Integer, SkipList.TestComparator>(
						list);
				iter.SeekToLast();

				// Compare against model iterator
				Iterator<re_integer> model_iter = keys_reverse.iterator();
				for (; model_iter.hasNext();) {
					ASSERT_TRUE(iter.Valid());
					ASSERT_EQ(model_iter.next().val, iter.key());
					iter.Prev();
				}
				ASSERT_TRUE(!iter.Valid());
			}

		}
	
		
	
	}

	public static void main(String args[]) {
		SkiplistTest slt = new SkiplistTest();
		// slt.Empty();
		slt.InsertAndLookup();
	}
}