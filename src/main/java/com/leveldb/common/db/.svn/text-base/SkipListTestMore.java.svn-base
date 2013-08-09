package com.leveldb.common.db;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.leveldb.common.AtomicPointer;
import com.leveldb.common.Env;
import com.leveldb.common.Function;
import com.leveldb.common._Comparable;
import com.leveldb.util.JenkinsHash;
import com.leveldb.util.util;

//We want to make sure that with a single writer and multiple
//concurrent readers (with no synchronization other than when a
//reader's iterator is created), the reader always observes all the
//data that was present in the skip list when the iterator was
//constructor.  Because insertions are happening concurrently, we may
//also observe new values that were inserted since the iterator was
//constructed, but we should never miss any values that were present
//at iterator construction time.
//
//We generate multi-part keys:
//<key,gen,hash>
//where:
//key is in range [0..K-1]
//gen is a generation number for key
//hash is hash(key,gen)
//
//The insertion code picks a random key, sets gen to be 1 + the last
//generation number inserted for that key, and sets hash to Hash(key,gen).
//
//At the beginning of a read, we snapshot the last inserted
//generation number for each key.  We then iterate, including random
//calls to Next() and Seek().  For every key we encounter, we
//check that it is either expected given the initial snapshot or has
//been concurrently added since the iterator started.

public class SkipListTestMore {
	static final int K = 4;

	static long key(long key) {
		return (key >> 40);
	}

	static int gen(long key) {
		return (int) ((key >> 8) & 0xffffffff);
	}

	static long hash(long key) {
		return key & 0xff;
	}

	static long HashNumbers(long k, long g) {
		byte[] kg = new byte[16];
		util.putLong(kg, 0, k);
		util.putLong(kg, 8, g);
		// return Hash(reinterpret_cast<char*>(data), sizeof(data), 0);
		return JenkinsHash.hash(kg);
	}

	static long MakeKey(long k, long g) {
		// assert(sizeof(Key) == sizeof(uint64_t));
		assert (k <= K); // We sometimes pass K to seek to the end of the
							// skiplist
		assert (g <= 0xffffffff);
		return ((k << 40) | (g << 8) | (HashNumbers(k, g) & 0xff));
	}

	static boolean IsValidKey(long k) {
		return hash(k) == (HashNumbers(key(k), gen(k)) & 0xff);
	}

	static long RandomTarget(Random rnd) {
		switch (Math.abs(rnd.nextInt()) % 10) {
		case 0:
			// Seek to beginning
			return MakeKey(0, 0);
		case 1:
			// Seek to end
			return MakeKey(K, 0);
		default:
			// Seek to middle
			return MakeKey((Math.abs(rnd.nextInt())) % K, 0);
		}
	}

	// Per-key generation
	class State {
		List<AtomicPointer<Long>> generation = new ArrayList<AtomicPointer<Long>>(
				K);

		void Set(int k, long v) {
			generation.get(k).Release_Store(v);
			// generation[k].Release_Store(reinterpret_cast<void*>(v));
		}

		long Get(int k) {
			return generation.get(k).Acquire_Load();
		}

		public State() {
			for (int k = 0; k < K; k++) {
				generation.add(new AtomicPointer<Long>(0l));
				Set(k, 0);
			}
		}
	}

	// Current state of the test
	State current_ = new State();

	static class TestComparator extends _Comparable<Long> {

		@Override
		public int compare(Long a, Long b) {
			if (a < b) {
				return -1;
			} else if (a > b) {
				return +1;
			} else {
				return 0;
			}
		}
	}

	// SkipList is not protected by mu_. We just use a single writer
	// thread to modify it.
	SkipList<Long, TestComparator> list_;

	public SkipListTestMore() {
		list_ = new SkipList<Long, TestComparator>(new TestComparator());
	}

	// REQUIRES: External synchronization
	void WriteStep(Random rnd) {
		int k = (Math.abs(rnd.nextInt())) % K;
		long g = current_.Get(k) + 1;
		long key = MakeKey(k, g);
		list_.Insert(key);
		current_.Set(k, g);
	}

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

	void ReadStep(Random rnd) {
		// Remember the initial committed state of the skiplist.
		State initial_state = new State();
		for (int k = 0; k < K; k++) {
			initial_state.Set(k, current_.Get(k));
		}

		// start with gen = 0; later added by 1
		long pos = RandomTarget(rnd);
		// System.out.println(pos);
		SkipListIterator<Long, TestComparator> iter = new SkipListIterator<Long, SkipListTestMore.TestComparator>(
				list_);
		iter.Seek(pos);
		while (true) {
			long current;
			if (!iter.Valid()) {
				current = MakeKey(K, 0);
			} else {
				// get the value, is it the latest???
				current = iter.key();
				ASSERT_TRUE(IsValidKey(current));// << current
				if (!IsValidKey(current)) {
					System.out.println("current invalid: " + key(current)
							+ ", " + gen(current) + ", ");
				}
			}
			ASSERT_TRUE(pos <= current);// << ;
			if (pos > current) {
				System.out.println("should not go backwards");
			}

			// Verify that everything in [pos,current) was not present in
			// initial_state. So, it is the latest!!!
			while (pos < current) {
				ASSERT_TRUE(key(pos) <= K);// << pos;
				if (key(pos) > K) {
					System.out
							.println(key(pos) + " >= " + K + ", pos = " + pos);
				}

				// Note that generation 0 is never inserted, so it is ok if
				// <*,0,*> is missing.
				ASSERT_TRUE((gen(pos) == 0)
						|| (gen(pos) > initial_state.Get((int) key(pos))));
				if (!((gen(pos) == 0) || (gen(pos) > initial_state
						.Get((int) key(pos))))) {
					System.out.println("key: " + key(pos) + "; gen: "
							+ gen(pos) + "; initgen: "
							+ initial_state.Get((int) key(pos)));
				}
				// << "key: " << key(pos)
				// << "; gen: " << gen(pos)
				// << "; initgen: "
				// << initial_state.Get(key(pos));

				// Advance to next key in the valid key space
				if (key(pos) < key(current)) {
					pos = MakeKey(key(pos) + 1, 0);
				} else {
					pos = MakeKey(key(pos), gen(pos) + 1);
				}
			}

			if (!iter.Valid()) {
				break;
			}

			if (Math.abs(rnd.nextInt()) % 2 != 0) {
				iter.Next();
				pos = MakeKey(key(pos), gen(pos) + 1);
			} else {
				long new_target = RandomTarget(rnd);
				if (new_target > pos) {
					pos = new_target;
					iter.Seek(new_target);
				}
			}
		}
	}

	static void ConcurrentWithoutThreads() {
		SkipListTestMore test = new SkipListTestMore();
		Random rnd = new Random();
		for (int i = 0; i < 10000; i++) {
			test.ReadStep(rnd);
			test.WriteStep(rnd);
		}
	}

	static class TestState {
		SkipListTestMore t_ = new SkipListTestMore();
		int seed_;
		AtomicPointer<TestState> quit_flag_;

		enum ReaderState {
			STARTING, RUNNING, DONE
		}

		public TestState(int s) {
			seed_ = s;
			quit_flag_ = new AtomicPointer<SkipListTestMore.TestState>(null);
			state_ = ReaderState.STARTING;
		}

		void Wait(ReaderState s) {
			mu_.lock();
			try {
				while (state_ != s) {
					try {
						// System.out.println(Thread.currentThread().getName() +
						// " -- in wait for " + s + ", but is " + state_);
						state_cv_.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				// System.out.println(Thread.currentThread().getName() +
				// " quit wait: " + state_ + " == " + s);
			} finally {
				mu_.unlock();
			}
		}

		void Change(ReaderState s) {
			mu_.lock();
			try {
				// System.out.println(Thread.currentThread().getName() +
				// " -- in change " + s);
				state_ = s;
				state_cv_.signalAll();
			} finally {
				mu_.unlock();
			}
		}

		ReentrantLock mu_ = new ReentrantLock();
		volatile ReaderState state_;
		Condition state_cv_ = mu_.newCondition();
	}

	static class ConcurrentReader implements Function {
		TestState state;

		public ConcurrentReader(TestState s) {
			state = s;
		}

		@Override
		public void exec(Object... args) {
			// TestState* state = reinterpret_cast<TestState*>(arg);
			Random rnd = new Random();
			long reads = 0;
			state.Change(TestState.ReaderState.RUNNING);
			// System.out.println("sub reader thread running");
			while (state.quit_flag_.Acquire_Load() == null) {
				state.t_.ReadStep(rnd);
				++reads;
				// System.out.print("-");
			}
			System.out.println(reads);
			state.Change(TestState.ReaderState.DONE);
			// System.out.println("sub reader thread done");
		}

	}

	static void RunConcurrent(int run) {
		// int seed = test::RandomSeed() + (run * 100);
		Random rnd = new Random();
		int N = 1000;
		int kSize = 1000;
		for (int i = 0; i < N; i++) {
			if ((i % 100) == 0) {
				System.out.println("Run " + i + " of " + N);
			}
			TestState state = new TestState(0);// (seed + 1);
			ConcurrentReader cr = new ConcurrentReader(state);
			Env.Default().Schedule(cr);// ConcurrentReader, state
			state.Wait(TestState.ReaderState.RUNNING);
			for (int j = 0; j < kSize; j++) {
				state.t_.WriteStep(rnd);
			}
			state.quit_flag_.Release_Store(state); // Any non-NULL arg will do
			state.Wait(TestState.ReaderState.DONE);
		}
	}

	public static void main(String args[]) {
		// ConcurrentWithoutThreads();
		RunConcurrent(1);
	}
}