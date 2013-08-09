package com.leveldb.common.options;

import com.leveldb.common.db.Snapshot;
//Options that control read operations
public class ReadOptions {
	// If true, all data read from underlying storage will be
	// verified against corresponding checksums.
	// Default: false
	public boolean verify_checksums;

	// Should the data read for this iteration be cached in memory?
	// Callers may wish to set this field to false for bulk scans.
	// Default: true
	public boolean fill_cache;

	// If "snapshot" is non-NULL, read as of the supplied snapshot
	// (which must belong to the DB that is being read and which must
	// not have been released). If "snapshot" is NULL, use an impliicit
	// snapshot of the state at the beginning of this read operation.
	// Default: NULL
	public Snapshot snapshot;

	public ReadOptions() {
		verify_checksums = false;
		fill_cache = true;
		snapshot = null;
	}
}
