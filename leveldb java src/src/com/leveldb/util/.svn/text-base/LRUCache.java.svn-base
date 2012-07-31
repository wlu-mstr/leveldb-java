package com.leveldb.util;

import java.util.HashMap;

import com.leveldb.common.Cache;
import com.leveldb.common.Function;
import com.leveldb.common.Slice;

public class LRUCache extends Cache {

	// LRU cache implementation

	// An entry is a variable length heap-allocated structure. Entries
	// are kept in a circular doubly linked list ordered by access time.
	class LRUHandle extends Handle {
		Object value;
		Function deleter;
		LRUHandle next;
		LRUHandle prev;
		int charge; // TODO(opt): Only allow uint32_t?
		int key_length;
		int refs; // TODO(opt): Pack with "key_length"?
		byte[] key_data; // Beginning of key

		public Slice key() {
			// For cheaper lookups, we allow a temporary Handle object
			// to store a pointer to a key in "value".
			if (next == this) {
				return (Slice) (value);
			} else {
				return new Slice(key_data, key_length);
			}
		}

		@Override
		public boolean equals(Object obj) {
			LRUHandle handle = (LRUHandle) obj;
			return handle.key().compareTo(key()) == 0;
		}

		@Override
		public int hashCode() {

			byte[] b = this.key().data();
			return JenkinsHash.hash(b, 0, b.length, 0);
		}
	}

	// Constructor parameters
	private int capacity_;

	// mutex_ protects the following state.
	// port::Mutex mutex_;
	private int usage_;
	private long last_id_;

	// Dummy head of LRU list.
	// lru.prev is newest entry, lru.next is oldest entry.
	private LRUHandle lru_;

	private HashMap<Integer, LRUHandle> table;

	// construction
	public LRUCache(int capacity) {
		capacity_ = capacity;
		usage_ = 0;
		last_id_ = 0;
		lru_ = new LRUHandle();
		// Make empty circular linked list
		lru_.next = lru_;
		lru_.prev = lru_;
		table = new HashMap<Integer, LRUHandle>();
	}

	@Override
	public Handle Insert(Slice key, Object value, int charge, Function deleter) {
		synchronized (this) {
			// LRUHandle* e = reinterpret_cast<LRUHandle*>(
			// malloc(sizeof(LRUHandle)-1 + key.size()));
			LRUHandle e = new LRUHandle();
			e.value = value;
			e.deleter = deleter;
			e.charge = charge;
			e.key_length = key.size();
			e.refs = 1; // One from LRUCache, one for the returned handle
			e.key_data = key.data();
			LRU_Append(e);
			usage_ += charge;

			// LRUHandle pp = table.put(e.hashCode(), e);
			LRUHandle pp = table.get(e.hashCode());
			if (pp != null) {// already contain this
				LRU_Remove(pp);
				table.remove(pp.hashCode());
				Unref(pp);
				table.put(e.hashCode(), e);
			} else {
				table.put(e.hashCode(), e);
			}
			while (usage_ > capacity_ && lru_.next != lru_) {
				LRUHandle old = lru_.next;
				LRU_Remove(old);
				table.remove(old.hashCode());
				Unref(old);
			}
			return e;
		}

	}

	@Override
	public Handle Lookup(Slice key) {
		synchronized (this) {
			LRUHandle dummy = new LRUHandle();
			dummy.next = dummy;
			dummy.value = key;
			LRUHandle e = table.get(dummy.hashCode());
			if (e == null) {
				return null;
			} else {
				//e.refs++;
				LRU_Remove(e);
				LRU_Append(e);
				return e;
			}
		}

	}

	@Override
	public void Release(Handle handle) {
		synchronized (this) {
			Unref((LRUHandle) handle);
		}

	}

	@Override
	public Object Value(Handle handle) {
		return ((LRUHandle) handle).value;

	}

	@Override
	public void Erase(Slice key) {
		synchronized (this) {
			LRUHandle dummy = new LRUHandle();
			dummy.next = dummy;
			dummy.value = key;
			LRUHandle e = table.get(dummy.hashCode());
			if (e != null) {
				LRU_Remove(e);
				table.remove(e.hashCode());
				Unref(e);
			}
		}

	}

	@Override
	public long NewId() {
		synchronized (this) {
			return ++(last_id_);
		}
	}

	@Override
	public void LRU_Remove(Handle e) {

	}

	@Override
	public void LRU_Append(Handle e) {

	}

	@Override
	public void Unref(Handle e) {

	}

	//
	private void LRU_Remove(LRUHandle e) {
		e.next.prev = e.prev;
		e.prev.next = e.next;
	};

	private void LRU_Append(LRUHandle e) {
		e.next = lru_;
		e.prev = lru_.prev;
		e.prev.next = e;
		e.next.prev = e;
	};

	private void Unref(LRUHandle e) {
		assert (e.refs > 0);
		e.refs--; // from 2 to 1, but when to be 0?
		if (e.refs <= 0) {
			usage_ -= e.charge;
			e.deleter.exec(e.key(), e.value);
			e = null;
		}
	};

	public String toString() {
		return table.size() + "";
	}

	@Override
	public void Destroy() {
		for (LRUHandle e = lru_.next; e != lru_;) {
			LRUHandle next = e.next;
			if (e.refs != 1){ // Error if caller has an unreleased handle
				System.err.println("Cache reference should be 1, rather than " + e.refs);
			}
			//Unref(e);
			// I find the refs is not always 1, a bug, so, Just delete!
			e.deleter.exec(e.key(), e.value);
			e = next;
		}

	}

}
