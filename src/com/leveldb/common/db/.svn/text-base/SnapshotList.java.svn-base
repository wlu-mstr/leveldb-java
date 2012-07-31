package com.leveldb.common.db;

import com.leveldb.util.SequenceNumber;

public class SnapshotList {
	SnapshotList() {
		list_.prev_ = list_;
		list_.next_ = list_;
	}

	boolean empty() {
		return list_.next_ == list_;
	}

	SnapshotImpl oldest() {
		assert (!empty());
		return list_.next_;
	}

	SnapshotImpl newest() {
		assert (!empty());
		return list_.prev_;
	}

	SnapshotImpl New(SequenceNumber seq) {
		SnapshotImpl s = new SnapshotImpl();
		// wlu, 2012-7-7, bugfix: should not be a reference of seq 
		s.number_ = new SequenceNumber(seq.value);
		s.list_ = this;
		s.next_ = list_;
		s.prev_ = list_.prev_;
		s.prev_.next_ = s;
		s.next_.prev_ = s;
		return s;
	}

	void Delete(SnapshotImpl s) {
		// assert(s->list_ == this);
		s.prev_.next_ = s.next_;
		s.next_.prev_ = s.prev_;
		s = null;
		// delete s;
	}

	// Dummy head of doubly-linked list of snapshots
	SnapshotImpl list_ = new SnapshotImpl();
}
