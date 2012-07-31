package com.leveldb.common.file;

import com.leveldb.common.Slice;
import com.leveldb.common.Status;

public abstract class _SequentialFile {

	// Read up to "n" bytes from the file. "scratch[0..n-1]" may be
	// written by this routine. Sets "*result" to the data that was
	// read (including if fewer than "n" bytes were successfully read).
	// If an error was encountered, returns a non-OK status.
	//
	// REQUIRES: External synchronization
	public abstract byte[] Read(int n, Slice result);

	// Skip "n" bytes from the file. This is guaranteed to be no
	// slower that reading the same data, but may be faster.
	//
	// If end of file is reached, skipping will stop at the end of the
	// file, and Skip will return OK.
	//
	// REQUIRES: External synchronization
	public abstract Status Skip(long n);
	
	public  void Close(){};
}
