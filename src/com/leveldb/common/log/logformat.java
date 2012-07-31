package com.leveldb.common.log;

public class logformat {
	public static final int kMaxRecordType = RecordType.kLastType;

	public static final int kBlockSize = 32768;

	// Header is checksum (4 bytes), type (1 byte), length (2 bytes).
	public static final int kHeaderSize = 4 + 1 + 2;
}
