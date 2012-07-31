package com.leveldb.common.file;

import com.leveldb.common.Slice;
import com.leveldb.common.Status;

public abstract class _WritableFile {

	public abstract Status Append(Slice data);

	public abstract Status Close();

	public abstract Status Flush();

	public abstract Status Sync();

}