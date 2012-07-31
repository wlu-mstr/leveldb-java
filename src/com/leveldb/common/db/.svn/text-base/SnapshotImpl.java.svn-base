package com.leveldb.common.db;

import com.leveldb.util.SequenceNumber;

public class SnapshotImpl extends Snapshot {
	public SequenceNumber number_; // const after creation
	// SnapshotImpl is kept in a doubly-linked circular list
	public SnapshotImpl prev_;
	public SnapshotImpl next_;
	public SnapshotList list_; // just for sanity checks
}
