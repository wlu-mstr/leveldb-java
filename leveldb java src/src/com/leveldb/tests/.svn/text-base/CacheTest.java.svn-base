package com.leveldb.tests;

import java.util.ArrayList;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import com.leveldb.common.Cache;
import com.leveldb.common.Function;
import com.leveldb.common.Slice;
import com.leveldb.util.JenkinsHash;
import com.leveldb.util.LRUCache;

public class CacheTest extends TestCase {

	static CacheTest current_;

	static Function deleter = new Function() {

		Slice _key;
		Integer _v;

		private void _exec() {
			// TODO Auto-generated method stub
			current_.deleted_keys_.add(JenkinsHash.hash(_key.data()));
			current_.deleted_values_.add(_v.hashCode());

		}

		@Override
		public void exec(Object... args) {
			_key = (Slice) args[0];
			_v = (Integer) args[1];
			_exec();

		}
	};

	static int kCacheSize = 100;
	static ArrayList<Integer> deleted_keys_;
	static ArrayList<Integer> deleted_values_;
	static Cache cache_;

	public CacheTest() {
		cache_ = LRUCache.NewLRUCache(kCacheSize);
		current_ = this;
		deleted_keys_ = new ArrayList<Integer>();
		deleted_values_ = new ArrayList<Integer>();
	}

	static int Lookup(Slice key) {
		Cache.Handle handle = cache_.Lookup(key);
		int r = (handle == null) ? -1 : (cache_.Value(handle)).hashCode();
		if (handle != null) {
			cache_.Release(handle);
		}
		return r;
	}

	static void Insert(Slice key, int value, int charge) {
		cache_.Release(cache_.Insert(key, (value), charge, deleter));// #ref is
																		// 2, so
																		// need
																		// to
																		// release
																		// after
																		// returning?
	}

	static void Erase(Slice key) {
		cache_.Erase(key);
	}

	public void test_insert() {
		Slice s1 = new Slice("100".getBytes());
		_assert(-1 == Lookup(new Slice("100")));
		Insert(s1, 101, 1);
		_assert(101 == Lookup(new Slice("100")));

		Slice s2 = new Slice("200");
		Slice s3 = new Slice("300");

		_assert(-1 == Lookup(new Slice("200")));
		_assert(-1 == Lookup(new Slice("300")));

		Insert(s2, 201, 1);
		_assert(101 == Lookup(new Slice("100")));
		_assert(201 == Lookup(new Slice("200")));
		_assert(-1 == Lookup(new Slice("300")));

		Insert(s1, 102, 1);
		_assert(102 == Lookup(new Slice("100")));
		_assert(201 == Lookup(new Slice("200")));
		_assert(-1 == Lookup(new Slice("300")));

		_assert(1 == deleted_keys_.size());
		_assert(JenkinsHash.hash("100".getBytes()) == deleted_keys_.get(0));
		_assert(101 == deleted_values_.get(0));

	}

	public void test_erase() {
		Erase(new Slice("200"));
		_assert(0 == deleted_keys_.size());
		Insert(new Slice("100".getBytes()), 101, 1);
		Insert(new Slice("200".getBytes()), 201, 1);
		Erase(new Slice("100"));
		_assert(-1 == Lookup(new Slice("100")));
		_assert(201 == Lookup(new Slice("200")));
		_assert(1 == deleted_keys_.size());
		_assert(JenkinsHash.hash("100".getBytes()) == deleted_keys_.get(0));
		_assert(101 == deleted_values_.get(0));

		Erase(new Slice("100"));
		_assert(-1 == Lookup(new Slice("100")));
		_assert(201 == Lookup(new Slice("200")));
		_assert(1 == deleted_keys_.size());
	}

	public void test_entries_pinned() {
		Insert(new Slice("100".getBytes()), 101, 1);
		Cache.Handle h1 = cache_.Lookup(new Slice("100")); // ref is increased
															// by 1, to 2
		_assert(101 == (Integer) (cache_.Value(h1)));

		Insert(new Slice("100".getBytes()), 102, 1); // old ref reduce to 1
		Cache.Handle h2 = cache_.Lookup(new Slice("100")); // ref from 1 to 2
		_assert(102 == (Integer) (cache_.Value(h2)));
		_assert(0 == deleted_keys_.size());

		cache_.Release(h1); // old ref is 0, deleted
		_assert(1 == deleted_keys_.size());
		_assert(JenkinsHash.hash("100".getBytes()) == deleted_keys_.get(0));
		_assert(101 == deleted_values_.get(0));

		Erase(new Slice("100")); // ref from 2 to 1,
		_assert(-1 == Lookup(new Slice("100")));
		_assert(1 == deleted_keys_.size());

		cache_.Release(h2); // ref from 1 to 0, deleted
		_assert(2 == deleted_keys_.size());
		_assert(JenkinsHash.hash("100".getBytes()) == deleted_keys_.get(1));
		_assert(102 == deleted_values_.get(1));

	}

	public void test_eviction_policy() {
		Insert(new Slice("100".getBytes()), 101, 1);
		Insert(new Slice("200".getBytes()), 201, 1);

		// Frequently used entry must be kept around
		for (int i = 0; i < kCacheSize; i++) {
			Insert(new Slice("" + (1000 + i)), 2000 + i, 1);
			System.out.print(i + ": ");
			_assert((2000 + i) == Lookup(new Slice("" + (1000 + i))));
			_assert(101 == Lookup(new Slice("100"))); // 100 is always the
														// newest one!!!
		}
		_assert(101 == Lookup(new Slice("100")));
		_assert(2 == deleted_keys_.size());
		_assert(JenkinsHash.hash("200".getBytes()) == deleted_keys_.get(0));
		_assert(201 == deleted_values_.get(0));
		System.out.print("\n" + deleted_values_);
	}

	public void test_heavy_entry() {
		Insert(new Slice("100".getBytes()), 101, 1);
		Insert(new Slice("200".getBytes()), 201, kCacheSize);
		_assert(1 == deleted_keys_.size());
		_assert(JenkinsHash.hash("100".getBytes()) == deleted_keys_.get(0));
		_assert(101 == deleted_values_.get(0));
	}

	public void test_new_id() {
		long a = cache_.NewId();
		long b = cache_.NewId();
		_assert(a != b);
	}

	private static void _assert(boolean b) {
		System.out.print(b + "\t");

	}

	public static Test suite() {
		TestSuite suite = new TestSuite("CacheTest Test");
		suite.addTestSuite(CacheTest.class);
		return suite;
	}

	public static void main(String args[]) {
		TestRunner.run(suite());
	}
}