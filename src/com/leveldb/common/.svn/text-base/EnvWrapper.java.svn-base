package com.leveldb.common;

import java.nio.channels.FileLock;
import java.util.List;

import com.leveldb.common.file._RandomAccessFile;
import com.leveldb.common.file._SequentialFile;
import com.leveldb.common.file._WritableFile;

public class EnvWrapper extends Env {
	// Initialize an EnvWrapper that delegates all calls to *target
	public EnvWrapper(Env target) {
		target_ = target;
	}

	// Return the target to which this Env forwards all calls
	public Env target() {
		return target_;
	}

	// The following text is boilerplate that forwards all methods to target()
	public _SequentialFile NewSequentialFile(String f) {
		return target_.NewSequentialFile(f);
	}

	public _RandomAccessFile NewRandomAccessFile(String f) {
		return target_.NewRandomAccessFile(f);
	}

	public _WritableFile NewWritableFile(String f) {
		return target_.NewWritableFile(f);
	}

	public boolean FileExists(String f) {
		return target_.FileExists(f);
	}

	public List<String> GetChildren(String dir) {
		return target_.GetChildren(dir);
	}

	public Status DeleteFile(String f) {
		return target_.DeleteFile(f);
	}

	public Status CreateDir(String d) {
		return target_.CreateDir(d);
	}

	public Status DeleteDir(String d) {
		return target_.DeleteDir(d);
	}

	public long GetFileSize(String f) {
		return target_.GetFileSize(f);
	}

	public Status RenameFile(String s, String t) {
		return target_.RenameFile(s, t);
	}

	public FileLock LockFile(String f) {
		return target_.LockFile(f);
	}

	public Status UnlockFile(FileLock l) {
		return target_.UnlockFile(l);
	}

	public void Schedule(Function fun) {
		target_.Schedule(fun);
	}

	public void StartThread(Function fun) {
		target_.StartThread(fun);
	}

	public String GetTestDirectory() {
		return target_.GetTestDirectory();
	}


	public long NowMicros() {
		return target_.NowMicros();
	}

	public void SleepForMicroseconds(int micros) {
		target_.SleepForMicroseconds(micros);
	}

	

	@Override
	public Logger NewLogger(String fname) {
		return target_.NewLogger(fname);
	}
	
	private Env target_;
}