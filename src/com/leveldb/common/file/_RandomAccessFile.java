package com.leveldb.common.file;

import com.leveldb.common.Slice;
import com.leveldb.common.Status;

public abstract class _RandomAccessFile {

	/**
	 * Read up to "n" bytes from the file starting at "offset".
	 * "scratch[0..n-1]" may be written by this routine. Sets "*result" to the
	 * data that was read (including if fewer than "n" bytes were successfully
	 * read). If an error was encountered, returns a non-OK status.
	 * 
	 * Safe for concurrent use by multiple threads.
	 */
	public abstract byte[] Read(long offset, int n, Slice result);
	
	public abstract void Close();
	
	public abstract String FileName();
}