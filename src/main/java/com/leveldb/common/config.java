package com.leveldb.common;

public class config {
	public static int kNumLevels = 7;

	// Level-0 compaction is started when we hit this many files.
	public static int kL0_CompactionTrigger = 4;

	// Soft limit on number of level-0 files. We slow down writes at this point.
	public static int kL0_SlowdownWritesTrigger = 8;

	// Maximum number of level-0 files. We stop writes at this point.
	public static int kL0_StopWritesTrigger = 12;

	// Maximum level to which a new compacted memtable is pushed if it
	// does not create overlap. We try to push to level 2 to avoid the
	// relatively expensive level 0=>1 compactions and to avoid some
	// expensive manifest file operations. We do not push all the way to
	// the largest level since that can generate a lot of wasted disk
	// space if the same key space is being repeatedly overwritten.
	public static int kMaxMemCompactLevel = 2;

} // namespace config