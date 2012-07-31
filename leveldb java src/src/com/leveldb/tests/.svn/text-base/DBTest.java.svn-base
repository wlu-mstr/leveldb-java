package com.leveldb.tests;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import sun.font.StrikeCache;

import com.leveldb.common.AtomicPointer;
import com.leveldb.common.Comparator;
import com.leveldb.common.Env;
import com.leveldb.common.EnvWrapper;
import com.leveldb.common.Iterator;
import com.leveldb.common.Slice;
import com.leveldb.common.Status;
import com.leveldb.common.config;
import com.leveldb.common.comparator.BytewiseComparatorImpl;
import com.leveldb.common.db.DB;
import com.leveldb.common.db.DBImpl;
import com.leveldb.common.db.InternalKey;
import com.leveldb.common.db.ParsedInternalKey;
import com.leveldb.common.db.Range;
import com.leveldb.common.db.Snapshot;
import com.leveldb.common.file._WritableFile;
import com.leveldb.common.options.CompressionType;
import com.leveldb.common.options.Options;
import com.leveldb.common.options.ReadOptions;
import com.leveldb.common.options.WriteOptions;
import com.leveldb.util.SequenceNumber;
import com.leveldb.util.ValueType;
import com.leveldb.util.util;

import junit.textui.TestRunner;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class DBTest extends TestCase {
	// Special Env used to delay background operations
	class SpecialEnv extends EnvWrapper {
		// sstable Sync() calls are blocked while this pointer is non-NULL.
		AtomicPointer delay_sstable_sync_ = new AtomicPointer(null);

		// Simulate no-space errors while this pointer is non-NULL.
		AtomicPointer no_space_ = new AtomicPointer(null);

		public SpecialEnv(Env base) {
			super(base);
			delay_sstable_sync_.Release_Store(null);
			no_space_.Release_Store(null);
		}

		public _WritableFile NewWritableFile(String f) {
			_WritableFile r;
			class SSTableFile extends _WritableFile {
				SpecialEnv env_;
				_WritableFile base_;

				SSTableFile(SpecialEnv env, _WritableFile base) {
					env_ = env;
					base_ = base;
				}

				// ~SSTableFile() { delete base_; }
				public Status Append(Slice data) {
					if (env_.no_space_.Acquire_Load() != null) {
						// Drop writes on the floor
						return Status.OK();
					} else {
						return base_.Append(data);
					}
				}

				public Status Close() {
					return base_.Close();
				}

				public Status Flush() {
					return base_.Flush();
				}

				public Status Sync() {
					while (env_.delay_sstable_sync_.Acquire_Load() != null) {
						env_.SleepForMicroseconds(100000);
					}
					return base_.Sync();
				}
			}

			r = target().NewWritableFile(f);
			if (f.contains(".sst")) {
				r = new SSTableFile(this, r);
			}
			return r;
		}
	}

	String dbname_;
	SpecialEnv env_;
	DB db_;

	Options last_options_;

	static boolean constructed = false;

	public void setUp() {
		// if (!constructed) {
		System.out.println("DBTest is constructed");
		env_ = (new SpecialEnv(Env.Default()));
		dbname_ = TableTest.TmpDir() + "/db_test";
		DB.DestroyDB(dbname_, new Options());
		db_ = null;
		Reopen(null);
		// constructed = true;
		// }
	}

	boolean Close() {
		System.out.println("Close");
		if (db_ != null) {
			db_.Close();
		}
		DB.DestroyDB(dbname_, new Options());
		env_ = null;
		return true;
	}

	DBImpl dbfull() {
		return (DBImpl) (db_);
	}

	void Reopen(Options options) {
		assertNotNull(TryReopen(options));
	}

	void Reopen() {
		Reopen(null);
	}

	void DestroyAndReopen(Options options) {
		if (db_ != null) {
			db_.Close();
		}
		db_ = null;
		DB.DestroyDB(dbname_, new Options());
		assertNotNull(TryReopen(options));
	}

	DB TryReopen(Options options) {
		if (db_ != null) {
			db_.Close();
		}
		db_ = null;
		Options opts = new Options();
		if (options != null) {
			opts = options;
		} else {
			opts.create_if_missing = true;
		}
		last_options_ = opts;
		db_ = DB.Open(opts, dbname_);
		return db_;
	}

	DB TryReopen(Options options, Status s_) {
		if (db_ != null) {
			db_.Close();
		}
		db_ = null;
		Options opts = new Options();
		if (options != null) {
			opts = options;
		} else {
			opts.create_if_missing = true;
		}
		last_options_ = opts;
		db_ = DB.Open(opts, dbname_, s_);
		return db_;
	}

	Status Put(Slice k, Slice v) {
		return db_.Put(new WriteOptions(), k, v);
	}

	Status Put(String k, String v) {
		return Put(new Slice(k), new Slice(v));
	}

	Status Delete(Slice k) {
		return db_.Delete(new WriteOptions(), k);
	}

	Status Delete(String k) {
		return Delete(new Slice(k));
	}

	Slice Get(Slice k, Snapshot snapshot) {
		ReadOptions options = new ReadOptions();
		options.snapshot = snapshot;
		Status s = new Status();
		Slice result = db_.Get(options, k, s);
		if (s.IsNotFound()) {
			result = new Slice("NOT_FOUND");
		} else if (!s.ok()) {
			result = new Slice(s.toString());
		}
		return result;
	}

	Slice Get(String k, Snapshot snapshot) {
		return Get(new Slice(k), snapshot);
	}

	Slice Get(Slice k) {
		return Get(k, null);
	}

	Slice Get(String k) {
		return Get(new Slice(k));
	}

	String IterStatus(Iterator iter) {
		String result;
		if (iter.Valid()) {
			result = iter.key().toString() + "->" + iter.value().toString();
		} else {
			result = "(invalid)";
		}
		return result;
	}

	// Return a string that contains all key,value pairs in order,
	// formatted like "(k1->v1)(k2->v2)".
	String Contents() {
		List<String> forward = new ArrayList<String>();
		String result = "";
		Iterator iter = db_.NewIterator(new ReadOptions());
		for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
			String s = IterStatus(iter);
			result += ('(');
			result += (s);
			result += (')');
			forward.add(s);
		}

		// Check reverse iteration results are the reverse of forward results
		int matched = 0;
		for (iter.SeekToLast(); iter.Valid(); iter.Prev()) {
			assertTrue(matched < forward.size());
			assertEquals(IterStatus(iter),
					forward.get(forward.size() - matched - 1));
			matched++;
		}
		assertEquals(matched, forward.size());

		iter = null;
		return result;
	}

	String AllEntriesFor(Slice user_key) {
		Iterator iter = dbfull().TEST_NewInternalIterator();
		InternalKey target = new InternalKey(user_key,
				SequenceNumber.MaxSequenceNumber, ValueType.TypeValue);
		iter.Seek(target.Encode());
		String result;
		if (!iter.status().ok()) {
			result = iter.status().toString();
		} else {
			result = "[ ";
			boolean first = true;
			while (iter.Valid()) {
				ParsedInternalKey ikey = InternalKey.ParseInternalKey_(iter
						.key());
				if (ikey == null) {
					result += "CORRUPTED";
				} else {
					if (last_options_.comparator.Compare(ikey.user_key,
							user_key) != 0) {
						break;
					}
					if (!first) {
						result += ", ";
					}
					first = false;
					switch (ikey.type.value) {
					case ValueType.kTypeValue:
						result += iter.value().toString();
						break;
					case ValueType.kTypeDeletion:
						result += "DEL";
						break;
					}
				}
				iter.Next();
			}
			if (!first) {
				result += " ";
			}
			result += "]";
		}
		iter = null;
		return result;
	}

	String AllEntriesFor(String user_key) {
		return AllEntriesFor(new Slice(user_key));
	}

	int NumTableFilesAtLevel(int level) {
		StringBuffer property = new StringBuffer();
		assertTrue(db_.GetProperty(new Slice("leveldb.num-files-at-level"
				+ level), property));
		return Integer.parseInt(property.toString());
	}

	int TotalTableFiles() {
		int result = 0;
		for (int level = 0; level < config.kNumLevels; level++) {
			result += NumTableFilesAtLevel(level);
		}
		return result;
	}

	// Return spread of files per level
	String FilesPerLevel() {
		String result = "";
		for (int level = 0; level < config.kNumLevels; level++) {
			int f = NumTableFilesAtLevel(level);
			String buf = (level > 0 ? "," : "") + f;
			result += buf;
		}
		return result;
	}

	int CountFiles() {
		List<String> files = env_.GetChildren(dbname_);
		return files.size();
	}

	long Size(Slice start, Slice limit) {
		Range r = new Range(start, limit);
		long size = db_.GetApproximateSizes(r);
		return size;
	}

	long Size(String start, String limit) {
		return Size(new Slice(start), new Slice(limit));
	}

	void Compact(Slice start, Slice limit) {
		db_.CompactRange(start, limit);
	}

	void Compact(String start, String limit) {
		Compact(new Slice(start), new Slice(limit));
	}

	// Do n memtable compactions, each of which produces an sstable
	// covering the range [small,large].
	void MakeTables(int n, Slice small, Slice large) {
		for (int i = 0; i < n; i++) {
			Put(small, new Slice("begin"));
			Put(large, new Slice("end"));
			dbfull().TEST_CompactMemTable();
		}
	}

	void MakeTables(int n, String small, String large) {
		MakeTables(n, new Slice(small), new Slice(large));
	}

	// Prevent pushing of new sstables into deeper levels by adding
	// tables that cover a specified range to all levels.
	void FillLevels(Slice smallest, Slice largest) {
		MakeTables(config.kNumLevels, smallest, largest);
	}

	void DumpFileCounts(String label) {
		System.err.println("---\n" + label + ":\n");
		System.err.println("maxoverlap: "
				+ dbfull().TEST_MaxNextLevelOverlappingBytes() + "\n");
		for (int level = 0; level < config.kNumLevels; level++) {
			int num = NumTableFilesAtLevel(level);
			if (num > 0) {
				System.err.println("  level " + level + " : " + num
						+ " files\n");
			}
		}
	}

	String DumpSSTableList() {
		StringBuffer property = new StringBuffer();
		db_.GetProperty(new Slice("leveldb.sstables"), property);
		return property.toString();
	}

	public void testEmpty() {
		assertTrue(db_ != null);
		String g = Get(new Slice("foo"), null).toString();
		assertTrue("NOT_FOUND".compareTo(g) == 0);
		assertTrue(Close());
	}

	public void testReadWrite() {
		assertTrue(Put("foo", "v1").ok());
		assertTrue("v1".compareTo(Get("foo").toString()) == 0);
		assertTrue(Put("bar", "v2").ok());
		assertTrue(Put("foo", "v3").ok());
		assertTrue("v3".compareTo(Get("foo").toString()) == 0);
		assertTrue("v2".compareTo(Get("bar").toString()) == 0);
		assertTrue(Close());
	}

	void ASSERT_OK(Status s) {
		assertTrue(s.ok());
	}

	public void testPutDeleteGet() {
		ASSERT_OK(db_
				.Put(new WriteOptions(), new Slice("foo"), new Slice("v1")));
		assertTrue("v1".compareTo(Get("foo").toString()) == 0);
		ASSERT_OK(db_
				.Put(new WriteOptions(), new Slice("foo"), new Slice("v2")));
		assertTrue("v2".compareTo(Get("foo").toString()) == 0);
		ASSERT_OK(db_.Delete(new WriteOptions(), new Slice("foo")));
		String g = Get(new Slice("foo")).toString();
		assertTrue("NOT_FOUND".compareTo(g) == 0);
		assertTrue(Close());
	}

	void ASSERT_EQ(String s, Slice sl) {
		assertTrue(s.compareTo(sl.toString()) == 0);
	}

	void ASSERT_EQ(String s, String s1) {
		ASSERT_EQ(s, new Slice(s1));
	}

	public void testGetFromImmutableLayer() {
		Options options = new Options();
		options.env = env_;
		options.write_buffer_size = 100000; // Small write buffer
		Reopen(options);

		ASSERT_OK(Put("foo", "v1"));
		ASSERT_EQ("v1", Get("foo"));

		env_.delay_sstable_sync_.Release_Store(env_); // Block sync calls
		Put("k1", TableTest.string(100000, 'x')); // Fill memtable
		Put("k2", TableTest.string(100000, 'y')); // Trigger compaction
		ASSERT_EQ("v1", Get("foo"));
		env_.delay_sstable_sync_.Release_Store(null); // Release sync calls
		assertTrue(Close());
	}

	public void testGetFromVersions() {
		ASSERT_OK(Put("foo", "v1"));
		dbfull().TEST_CompactMemTable();
		ASSERT_EQ("v1", Get("foo"));
		assertTrue(Close());
	}

	public void testGetSnapshot() {
		// Try with both a short key and a long key
		for (int i = 0; i < 2; i++) {
			String key = (i == 0) ? new String("foo") : TableTest.string(200,
					'x');
			ASSERT_OK(Put(key, "v1"));
			Snapshot s1 = db_.GetSnapshot();
			ASSERT_OK(Put(key, "v2"));
			ASSERT_EQ("v2", Get(key));
			ASSERT_EQ("v1", Get(key, s1));
			dbfull().TEST_CompactMemTable();
			ASSERT_EQ("v2", Get(key));
			ASSERT_EQ("v1", Get(key, s1));
			db_.ReleaseSnapshot(s1);
		}
	}

	public void testGetLevel0Ordering() {
		// Check that we process level-0 files in correct order. The code
		// below generates two level-0 files where the earlier one comes
		// before the later one in the level-0 file list since the earlier
		// one has a smaller "smallest" key.
		ASSERT_OK(Put("bar", "b"));
		ASSERT_OK(Put("foo", "v1"));
		dbfull().TEST_CompactMemTable();
		ASSERT_OK(Put("foo", "v2"));
		dbfull().TEST_CompactMemTable();
		ASSERT_EQ("v2", Get("foo"));
	}

	public void testGetOrderedByLevels() {
		ASSERT_OK(Put("foo", "v1"));
		Compact("a", "z");
		ASSERT_EQ("v1", Get("foo"));
		ASSERT_OK(Put("foo", "v2"));
		ASSERT_EQ("v2", Get("foo"));
		dbfull().TEST_CompactMemTable();
		ASSERT_EQ("v2", Get("foo"));
	}

	public void testGetPicksCorrectFile() {
		// Arrange to have multiple files in a non-level-0 level.
		ASSERT_OK(Put("a", "va"));
		Compact("a", "b");
		ASSERT_OK(Put("x", "vx"));
		Compact("x", "y");
		ASSERT_OK(Put("f", "vf"));
		Compact("f", "g");
		ASSERT_EQ("va", Get("a"));
		ASSERT_EQ("vf", Get("f"));
		ASSERT_EQ("vx", Get("x"));
	}

	public void testGetEncountersEmptyLevel() {
		// Arrange for the following to happen:
		// * sstable A in level 0
		// * nothing in level 1
		// * sstable B in level 2
		// Then do enough Get() calls to arrange for an automatic compaction
		// of sstable A. A bug would cause the compaction to be marked as
		// occuring at level 1 (instead of the correct level 0).

		// Step 1: First place sstables in levels 0 and 2
		int compaction_count = 0;
		while (NumTableFilesAtLevel(0) == 0 || NumTableFilesAtLevel(2) == 0) {
			assertTrue("could not fill levels 0 and 2", compaction_count <= 100);
			compaction_count++;
			Put("a", "begin");
			Put("z", "end");
			dbfull().TEST_CompactMemTable();
			System.out.println(compaction_count);
		}
		System.out.println("Have filled levels 0 and 2");
		StringBuffer sb = new StringBuffer();
		dbfull().GetProperty(new Slice("leveldb.stats"), sb);
		System.out.println(sb.toString());

		// Step 2: clear level 1 if necessary.
		dbfull().TEST_CompactRange(1, null, null);
		assertTrue(NumTableFilesAtLevel(0) == 1);
		assertTrue(NumTableFilesAtLevel(1) == 0);
		assertTrue(NumTableFilesAtLevel(2) == 1);

		// Step 3: read until level 0 compaction disappears.
		// TODO: Compation of level 0 is not triggered
		int read_count = 0;
		while (NumTableFilesAtLevel(0) > 0) {
			sb = new StringBuffer();
			dbfull().GetProperty(new Slice("leveldb.stats"), sb);
			System.out.println(sb.toString());
			assertTrue(/* "did not trigger level 0 compaction", */
			read_count <= 10000);
			read_count++;
			ASSERT_EQ("NOT_FOUND", Get("missing"));
		}
	}

	public void testIterEmpty() {
		Iterator iter = db_.NewIterator(new ReadOptions());

		iter.SeekToFirst();
		ASSERT_EQ(IterStatus(iter), "(invalid)");

		iter.SeekToLast();
		ASSERT_EQ(IterStatus(iter), "(invalid)");

		iter.Seek(new Slice("foo"));
		ASSERT_EQ(IterStatus(iter), "(invalid)");

		iter = null;
	}

	public void testIterSingle() {
		ASSERT_OK(Put("a", "va"));
		Iterator iter = db_.NewIterator(new ReadOptions());

		iter.SeekToFirst();
		ASSERT_EQ(IterStatus(iter), "a->va");
		iter.Next();
		ASSERT_EQ(IterStatus(iter), "(invalid)");
		iter.SeekToFirst();
		ASSERT_EQ(IterStatus(iter), "a->va");
		iter.Prev();
		ASSERT_EQ(IterStatus(iter), "(invalid)");

		iter.SeekToLast();
		ASSERT_EQ(IterStatus(iter), "a->va");
		iter.Next();
		ASSERT_EQ(IterStatus(iter), "(invalid)");
		iter.SeekToLast();
		ASSERT_EQ(IterStatus(iter), "a->va");
		iter.Prev();
		ASSERT_EQ(IterStatus(iter), "(invalid)");

		iter.Seek(new Slice(""));
		ASSERT_EQ(IterStatus(iter), "a->va");
		iter.Next();
		ASSERT_EQ(IterStatus(iter), "(invalid)");

		iter.Seek(new Slice("a"));
		ASSERT_EQ(IterStatus(iter), "a->va");
		iter.Next();
		ASSERT_EQ(IterStatus(iter), "(invalid)");

		iter.Seek(new Slice("b"));
		ASSERT_EQ(IterStatus(iter), "(invalid)");

		iter = null;
	}

	public void testIterMulti() {
		ASSERT_OK(Put("a", "va"));
		ASSERT_OK(Put("b", "vb"));
		ASSERT_OK(Put("c", "vc"));
		Iterator iter = db_.NewIterator(new ReadOptions());

		iter.SeekToFirst();
		ASSERT_EQ(IterStatus(iter), "a->va");
		iter.Next();
		ASSERT_EQ(IterStatus(iter), "b->vb");
		iter.Next();
		ASSERT_EQ(IterStatus(iter), "c->vc");
		iter.Next();
		ASSERT_EQ(IterStatus(iter), "(invalid)");
		iter.SeekToFirst();
		ASSERT_EQ(IterStatus(iter), "a->va");
		iter.Prev();
		ASSERT_EQ(IterStatus(iter), "(invalid)");

		iter.SeekToLast();
		ASSERT_EQ(IterStatus(iter), "c->vc");
		iter.Prev();
		ASSERT_EQ(IterStatus(iter), "b->vb");
		iter.Prev();
		ASSERT_EQ(IterStatus(iter), "a->va");
		iter.Prev();
		ASSERT_EQ(IterStatus(iter), "(invalid)");
		iter.SeekToLast();
		ASSERT_EQ(IterStatus(iter), "c->vc");
		iter.Next();
		ASSERT_EQ(IterStatus(iter), "(invalid)");

		iter.Seek(new Slice(""));
		ASSERT_EQ(IterStatus(iter), "a->va");
		iter.Seek(new Slice("a"));
		ASSERT_EQ(IterStatus(iter), "a->va");
		iter.Seek(new Slice("ax"));
		ASSERT_EQ(IterStatus(iter), "b->vb");
		iter.Seek(new Slice("b"));
		ASSERT_EQ(IterStatus(iter), "b->vb");
		iter.Seek(new Slice("z"));
		ASSERT_EQ(IterStatus(iter), "(invalid)");

		// Switch from reverse to forward
		iter.SeekToLast();
		iter.Prev();
		iter.Prev();
		iter.Next();
		ASSERT_EQ(IterStatus(iter), "b->vb");

		// Switch from forward to reverse
		iter.SeekToFirst();
		iter.Next();
		iter.Next();
		iter.Prev();
		ASSERT_EQ(IterStatus(iter), "b->vb");

		// Make sure iter stays at snapshot
		ASSERT_OK(Put("a", "va2"));
		ASSERT_OK(Put("a2", "va3"));
		ASSERT_OK(Put("b", "vb2"));
		ASSERT_OK(Put("c", "vc2"));
		ASSERT_OK(Delete(new Slice("b")));
		iter.SeekToFirst();
		ASSERT_EQ(IterStatus(iter), "a->va");
		iter.Next();
		ASSERT_EQ(IterStatus(iter), "b->vb");
		iter.Next();
		ASSERT_EQ(IterStatus(iter), "c->vc");
		iter.Next();
		ASSERT_EQ(IterStatus(iter), "(invalid)");
		iter.SeekToLast();
		ASSERT_EQ(IterStatus(iter), "c->vc");
		iter.Prev();
		ASSERT_EQ(IterStatus(iter), "b->vb");
		iter.Prev();
		ASSERT_EQ(IterStatus(iter), "a->va");
		iter.Prev();
		ASSERT_EQ(IterStatus(iter), "(invalid)");
	}

	public void testIterSmallAndLargeMix() {
		ASSERT_OK(Put("a", "va"));
		ASSERT_OK(Put("b", TableTest.string(100000, 'b')));
		ASSERT_OK(Put("c", "vc"));
		ASSERT_OK(Put("d", TableTest.string(100000, 'd')));
		ASSERT_OK(Put("e", TableTest.string(100000, 'e')));

		Iterator iter = db_.NewIterator(new ReadOptions());

		iter.SeekToFirst();
		ASSERT_EQ(IterStatus(iter), "a->va");
		iter.Next();
		ASSERT_EQ(IterStatus(iter), "b->" + TableTest.string(100000, 'b'));
		iter.Next();
		ASSERT_EQ(IterStatus(iter), "c->vc");
		iter.Next();
		ASSERT_EQ(IterStatus(iter), "d->" + TableTest.string(100000, 'd'));
		iter.Next();
		ASSERT_EQ(IterStatus(iter), "e->" + TableTest.string(100000, 'e'));
		iter.Next();
		ASSERT_EQ(IterStatus(iter), "(invalid)");

		iter.SeekToLast();
		ASSERT_EQ(IterStatus(iter), "e->" + TableTest.string(100000, 'e'));
		iter.Prev();
		ASSERT_EQ(IterStatus(iter), "d->" + TableTest.string(100000, 'd'));
		iter.Prev();
		ASSERT_EQ(IterStatus(iter), "c->vc");
		iter.Prev();
		ASSERT_EQ(IterStatus(iter), "b->" + TableTest.string(100000, 'b'));
		iter.Prev();
		ASSERT_EQ(IterStatus(iter), "a->va");
		iter.Prev();
		ASSERT_EQ(IterStatus(iter), "(invalid)");
	}

	public void testIterMultiWithDelete() {
		ASSERT_OK(Put("a", "va"));
		ASSERT_OK(Put("b", "vb"));
		ASSERT_OK(Put("c", "vc"));
		ASSERT_OK(Delete(new Slice("b")));
		ASSERT_EQ("NOT_FOUND", Get("b"));

		Iterator iter = db_.NewIterator(new ReadOptions());
		iter.Seek(new Slice("c"));
		ASSERT_EQ(IterStatus(iter), "c->vc");
		iter.Prev();
		ASSERT_EQ(IterStatus(iter), "a->va");
	}

	public void testRecover() {
		ASSERT_OK(Put("foo", "v1"));
		ASSERT_OK(Put("baz", "v5"));

		Reopen(null);
		ASSERT_EQ("v1", Get("foo"));

		ASSERT_EQ("v1", Get("foo"));
		ASSERT_EQ("v5", Get("baz"));
		ASSERT_OK(Put("bar", "v2"));
		ASSERT_OK(Put("foo", "v3"));

		Reopen(null);
		ASSERT_EQ("v3", Get("foo"));
		ASSERT_OK(Put("foo", "v4"));
		ASSERT_EQ("v4", Get("foo"));
		ASSERT_EQ("v2", Get("bar"));
		ASSERT_EQ("v5", Get("baz"));
	}

	public void testRecoveryWithEmptyLog() {
		ASSERT_OK(Put("foo", "v1"));
		ASSERT_OK(Put("foo", "v2"));
		Reopen();
		Reopen();
		ASSERT_OK(Put("foo", "v3"));
		Reopen();
		ASSERT_EQ("v3", Get("foo"));
	}

	public void testRecoverDuringMemtableCompaction() {
		Options options = new Options();
		options.env = env_;
		options.write_buffer_size = 1000000;
		Reopen(options);

		// Trigger a long memtable compaction and reopen the database during it
		ASSERT_OK(Put("foo", "v1")); // Goes to 1st log file
		ASSERT_OK(Put("big1", TableTest.string(10000000, 'x'))); // Fills
																	// memtable
		ASSERT_OK(Put("big2", TableTest.string(1000, 'y'))); // Triggers
																// compaction
		ASSERT_OK(Put("bar", "v2")); // Goes to new log file

		Reopen(options);
		ASSERT_EQ("v1", Get("foo"));
		ASSERT_EQ("v2", Get("bar"));
		ASSERT_EQ(TableTest.string(10000000, 'x'), Get("big1"));
		ASSERT_EQ(TableTest.string(1000, 'y'), Get("big2"));
	}

	static String Key(int i) {
		return "key" + String.format("%06d", i);
	}

	public void testKey() {
		System.out.println(Key(0));
		System.out.println(Key(1));
		System.out.println(Key(10));
		System.out.println(Key(123456));
		System.out.println(Key(1111111));
	}

	public void testMinorCompactionsHappen() {
		Options options = new Options();
		options.write_buffer_size = 10000;
		Reopen(options);

		int N = 500;

		int starting_num_tables = TotalTableFiles();
		for (int i = 0; i < N; i++) {
			ASSERT_OK(Put(Key(i), Key(i) + TableTest.string(1000, 'v')));
		}
		int ending_num_tables = TotalTableFiles();
		assertTrue(ending_num_tables > starting_num_tables);

		for (int i = 0; i < N; i++) {
			ASSERT_EQ(Key(i) + TableTest.string(1000, 'v'), Get(Key(i)));
		}

		Reopen();

		for (int i = 0; i < N; i++) {
			ASSERT_EQ(Key(i) + TableTest.string(1000, 'v'), Get(Key(i)));
		}
	}

	public void testRecoverWithLargeLog() {
		{
			Options options = new Options();
			Reopen(options);
			ASSERT_OK(Put("big1", TableTest.string(200000, '1')));
			ASSERT_OK(Put("big2", TableTest.string(200000, '2')));
			ASSERT_OK(Put("small3", TableTest.string(10, '3')));
			ASSERT_OK(Put("small4", TableTest.string(10, '4')));
			assertTrue(NumTableFilesAtLevel(0) == 0);
		}

		// Make sure that if we re-open with a small write buffer size that
		// we flush table files in the middle of a large log file.
		Options options = new Options();
		options.write_buffer_size = 100000;
		Reopen(options);
		assertTrue(NumTableFilesAtLevel(0) == 3);
		ASSERT_EQ(TableTest.string(200000, '1'), Get("big1"));
		ASSERT_EQ(TableTest.string(200000, '2'), Get("big2"));
		ASSERT_EQ(TableTest.string(10, '3'), Get("small3"));
		ASSERT_EQ(TableTest.string(10, '4'), Get("small4"));
		assertTrue(NumTableFilesAtLevel(0) > 1);
	}

	static String RandomString(Random rnd, int len) {
		return util.RandomString(rnd, len);
	}

	public void testCompactionsGenerateMultipleFiles() {
		Options options = new Options();
		options.write_buffer_size = 100000000; // Large write buffer
		Reopen(options);

		Random rnd = new Random(301);

		// Write 8MB (80 values, each 100K)
		assertTrue(NumTableFilesAtLevel(0) == 0);
		List<String> values = new ArrayList<String>();
		for (int i = 0; i < 80; i++) {
			values.add(RandomString(rnd, 100000));
			ASSERT_OK(Put(Key(i), values.get(i)));
		}

		System.out.println("put is over, begin reopen");
		// Reopening moves updates to level-0
		Reopen(options);

		System.out.println("reopen is over, begin compact level 0");
		dbfull().TEST_CompactRange(0, null, null);
		System.out.println("compact level 0 is over");
		assertTrue(NumTableFilesAtLevel(0) == 0);
		assertTrue(NumTableFilesAtLevel(1) > 1);
		for (int i = 0; i < 80; i++) {
			ASSERT_EQ(values.get(i), Get(Key(i)));
		}
	}

	public void testRepeatedWritesToSameKey() {
		Options options = new Options();
		options.env = env_;
		options.write_buffer_size = 100000; // Small write buffer
		Reopen(options);

		// We must have at most one file per level except for level-0,
		// which may have up to kL0_StopWritesTrigger files.
		int kMaxFiles = config.kNumLevels + config.kL0_StopWritesTrigger;

		Random rnd = new Random(301);
		String value = RandomString(rnd, 2 * options.write_buffer_size);
		for (int i = 0; i < 5 * kMaxFiles; i++) {
			Put("key", value);
			assertTrue(TotalTableFiles() < kMaxFiles);
			System.err.println("after " + (i + 1) + " : " + TotalTableFiles()
					+ "files");
		}
	}

	public void testSparseMerge() {
		Options options = new Options();
		options.compression = CompressionType.NoCompression;
		Reopen(options);

		FillLevels(new Slice("A"), new Slice("Z"));

		// Suppose there is:
		// small amount of data with prefix A
		// large amount of data with prefix B
		// small amount of data with prefix C
		// and that recent updates have made small changes to all three
		// prefixes.
		// Check that we do not do a compaction that merges all of B in one
		// shot.
		String value = TableTest.string(1000, 'x');
		Put("A", "va");
		// Write approximately 100MB of "B" values
		for (int i = 0; i < 100000; i++) {
			String key = (String.format("B%010d", i));
			Put(key, value);
		}
		Put("C", "vc");
		dbfull().TEST_CompactMemTable();
		dbfull().TEST_CompactRange(0, null, null);

		// Make sparse update
		Put("A", "va2");
		Put("B100", "bvalue2");
		Put("C", "vc2");
		dbfull().TEST_CompactMemTable();

		// Compactions should not cause us to create a situation where
		// a file overlaps too much data at the next level.
		assertTrue(dbfull().TEST_MaxNextLevelOverlappingBytes() <= 20 * 1048576);
		dbfull().TEST_CompactRange(0, null, null);
		assertTrue(dbfull().TEST_MaxNextLevelOverlappingBytes() <= 20 * 1048576);
		dbfull().TEST_CompactRange(1, null, null);
		assertTrue(dbfull().TEST_MaxNextLevelOverlappingBytes() <= 20 * 1048576);
	}

	public boolean Between(long val, long low, long high) {
		boolean result = (val >= low) && (val <= high);
		if (!result) {
			System.err.println("Value " + val + " is not in range [" + low
					+ ", " + high + "]\n");
		}
		return result;
	}

	public void testApproximateSizes() {
		Options options = new Options();
		options.write_buffer_size = 100000000; // Large write buffer
		options.compression = CompressionType.NoCompression;
		DestroyAndReopen(null);

		assertTrue(Between(Size("", "xyz"), 0, 0));
		Reopen(options);
		assertTrue(Between(Size("", "xyz"), 0, 0));

		// Write 8MB (80 values, each 100K)
		assertEquals(NumTableFilesAtLevel(0), 0);
		int N = 80;
		Random rnd = new Random(301);
		for (int i = 0; i < N; i++) {
			ASSERT_OK(Put(Key(i), util.RandomString(rnd, 100000)));
		}

		// 0 because GetApproximateSizes() does not account for memtable space
		assertTrue(Between(Size("", Key(50)), 0, 0));

		// Check sizes across recovery by reopening a few times
		for (int run = 0; run < 3; run++) {
			System.err.println("run = " + run);
			Reopen(options);

			for (int compact_start = 0; compact_start < N; compact_start += 10) {
				for (int i = 0; i < N; i += 10) {
					assertTrue(Between(Size("", Key(i)), 100000 * i,
							100000 * i + 10000));
					assertTrue(Between(Size("", Key(i) + ".suffix"),
							100000 * (i + 1), 100000 * (i + 1) + 10000));
					assertTrue(Between(Size(Key(i), Key(i + 10)), 100000 * 10,
							100000 * 10 + 10000));
				}
				assertTrue(Between(Size("", Key(50)), 5000000, 5010000));
				assertTrue(Between(Size("", Key(50) + ".suffix"), 5100000,
						5110000));

				String cstart_str = Key(compact_start);
				String cend_str = Key(compact_start + 9);
				Slice cstart = new Slice(cstart_str);
				Slice cend = new Slice(cend_str);
				dbfull().TEST_CompactRange(0, cstart, cend);
				System.out.println(cstart_str + "  " + cend_str + " Over");
			}

			assertEquals(NumTableFilesAtLevel(0), 0);
			assertTrue(NumTableFilesAtLevel(1) > 0);
			System.out.println("Over");
		}
	}

	public void testApproximateSizes_MixOfSmallAndLarge() {
		Options options = new Options();
		options.compression = CompressionType.NoCompression;
		Reopen();

		Random rnd = new Random(301);
		String big1 = RandomString(rnd, 100000);
		ASSERT_OK(Put(Key(0), RandomString(rnd, 10000)));
		ASSERT_OK(Put(Key(1), RandomString(rnd, 10000)));
		ASSERT_OK(Put(Key(2), big1));
		ASSERT_OK(Put(Key(3), RandomString(rnd, 10000)));
		ASSERT_OK(Put(Key(4), big1));
		ASSERT_OK(Put(Key(5), RandomString(rnd, 10000)));
		ASSERT_OK(Put(Key(6), RandomString(rnd, 300000)));
		ASSERT_OK(Put(Key(7), RandomString(rnd, 10000)));

		// Check sizes across recovery by reopening a few times
		for (int run = 0; run < 3; run++) {
			Reopen(options);

			assertTrue(Between(Size("", Key(0)), 0, 0));
			assertTrue(Between(Size("", Key(1)), 10000, 11000));
			assertTrue(Between(Size("", Key(2)), 20000, 21000));
			assertTrue(Between(Size("", Key(3)), 120000, 121000));
			assertTrue(Between(Size("", Key(4)), 130000, 131000));
			assertTrue(Between(Size("", Key(5)), 230000, 231000));
			assertTrue(Between(Size("", Key(6)), 240000, 241000));
			assertTrue(Between(Size("", Key(7)), 540000, 541000));
			assertTrue(Between(Size("", Key(8)), 550000, 551000));

			assertTrue(Between(Size(Key(3), Key(5)), 110000, 111000));

			dbfull().TEST_CompactRange(0, null, null);
		}
	}

	public void testIteratorPinsRef() {
		Put("foo", "hello");

		// Get iterator that will yield the current contents of the DB.
		Iterator iter = db_.NewIterator(new ReadOptions());

		// Write to force compactions
		Put("foo", "newvalue1");
		for (int i = 0; i < 100; i++) {
			ASSERT_OK(Put(Key(i), Key(i) + TableTest.string(100000, 'v'))); // 100K
																			// values
		}
		Put("foo", "newvalue2");

		iter.SeekToFirst();
		assertTrue(iter.Valid());
		ASSERT_EQ("foo", iter.key().toString());
		ASSERT_EQ("hello", iter.value().toString());
		iter.Next();
		assert (!iter.Valid());
	}

	public void testSnapshot() {
		Put("foo", "v1");
		Snapshot s1 = db_.GetSnapshot();
		Put("foo", "v2");
		Snapshot s2 = db_.GetSnapshot();
		Put("foo", "v3");
		Snapshot s3 = db_.GetSnapshot();

		Put("foo", "v4");
		ASSERT_EQ("v1", Get("foo", s1));
		ASSERT_EQ("v2", Get("foo", s2));
		ASSERT_EQ("v3", Get("foo", s3));
		ASSERT_EQ("v4", Get("foo"));

		db_.ReleaseSnapshot(s3);
		ASSERT_EQ("v1", Get("foo", s1));
		ASSERT_EQ("v2", Get("foo", s2));
		ASSERT_EQ("v4", Get("foo"));

		db_.ReleaseSnapshot(s1);
		ASSERT_EQ("v2", Get("foo", s2));
		ASSERT_EQ("v4", Get("foo"));

		db_.ReleaseSnapshot(s2);
		ASSERT_EQ("v4", Get("foo"));
	}

	public void testHiddenValuesAreRemoved() {
		Random rnd = new Random(301);
		FillLevels(new Slice("a"), new Slice("z"));

		String big = RandomString(rnd, 50000);
		Put("foo", big);
		Put("pastfoo", "v");
		Snapshot snapshot = db_.GetSnapshot();
		Put("foo", "tiny");
		Put("pastfoo2", "v2"); // Advance sequence number one more

		ASSERT_OK(dbfull().TEST_CompactMemTable());
		assertTrue(NumTableFilesAtLevel(0) > 0);

		ASSERT_EQ(big, Get("foo", snapshot));
		assertTrue(Between(Size("", "pastfoo"), 50000, 60000));
		db_.ReleaseSnapshot(snapshot);
		ASSERT_EQ(AllEntriesFor(new Slice("foo")), "[ tiny, " + big + " ]");
		Slice x = new Slice("x");
		dbfull().TEST_CompactRange(0, null, x);
		ASSERT_EQ(AllEntriesFor(new Slice("foo")), "[ tiny ]");
		assertTrue(NumTableFilesAtLevel(0) == 0);
		assertTrue(NumTableFilesAtLevel(1) >= 1);
		dbfull().TEST_CompactRange(1, null, x);
		ASSERT_EQ(AllEntriesFor(new Slice("foo")), "[ tiny ]");

		assertTrue(Between(Size("", "pastfoo"), 0, 1000));
	}

	public void testDeletionMarkers1() {
		Put("foo", "v1");
		ASSERT_OK(dbfull().TEST_CompactMemTable());
		int last = config.kMaxMemCompactLevel;
		assertTrue(NumTableFilesAtLevel(last) == 1); // foo => v1 is now in last
														// level

		// Place a table at level last-1 to prevent merging with preceding
		// mutation
		Put("a", "begin");
		Put("z", "end");
		dbfull().TEST_CompactMemTable();
		assertTrue(NumTableFilesAtLevel(last) == 1);
		assertTrue(NumTableFilesAtLevel(last - 1) == 1);

		Delete(new Slice("foo"));
		Put("foo", "v2");
		ASSERT_EQ(AllEntriesFor("foo"), "[ v2, DEL, v1 ]");
		ASSERT_OK(dbfull().TEST_CompactMemTable()); // Moves to level last-2
		ASSERT_EQ(AllEntriesFor("foo"), "[ v2, DEL, v1 ]");
		Slice z = new Slice("z");
		dbfull().TEST_CompactRange(last - 2, null, z);
		// DEL eliminated, but v1 remains because we aren't compacting that
		// level
		// (DEL can be eliminated because v2 hides v1).
		ASSERT_EQ(AllEntriesFor("foo"), "[ v2, v1 ]");
		dbfull().TEST_CompactRange(last - 1, null, null);
		// Merging last-1 w/ last, so we are the base level for "foo", so
		// DEL is removed. (as is v1).
		ASSERT_EQ(AllEntriesFor("foo"), "[ v2 ]");
	}

	public void testDeletionMarkers2() {
		Put("foo", "v1");
		ASSERT_OK(dbfull().TEST_CompactMemTable());
		int last = config.kMaxMemCompactLevel;
		assertTrue(NumTableFilesAtLevel(last) == 1); // foo => v1 is now in last
														// level

		// Place a table at level last-1 to prevent merging with preceding
		// mutation
		Put("a", "begin");
		Put("z", "end");
		dbfull().TEST_CompactMemTable();
		assertTrue(NumTableFilesAtLevel(last) == 1);
		assertTrue(NumTableFilesAtLevel(last - 1) == 1);

		Delete(new Slice("foo"));
		ASSERT_EQ(AllEntriesFor("foo"), "[ DEL, v1 ]");
		ASSERT_OK(dbfull().TEST_CompactMemTable()); // Moves to level last-2
		ASSERT_EQ(AllEntriesFor("foo"), "[ DEL, v1 ]");
		dbfull().TEST_CompactRange(last - 2, null, null);
		// DEL kept: "last" file overlaps
		ASSERT_EQ(AllEntriesFor("foo"), "[ DEL, v1 ]");
		dbfull().TEST_CompactRange(last - 1, null, null);
		// Merging last-1 w/ last, so we are the base level for "foo", so
		// DEL is removed. (as is v1).
		ASSERT_EQ(AllEntriesFor("foo"), "[ ]");
	}

	public void testOverlapInLevel0() {
		assertTrue("Fix test to match config", config.kMaxMemCompactLevel == 2);

		// Fill levels 1 and 2 to disable the pushing of new memtables to levels
		// > 0.
		ASSERT_OK(Put("100", "v100"));
		ASSERT_OK(Put("999", "v999"));
		dbfull().TEST_CompactMemTable();
		ASSERT_OK(Delete(new Slice("100")));
		ASSERT_OK(Delete(new Slice("999")));
		dbfull().TEST_CompactMemTable();
		ASSERT_EQ("0,1,1,0,0,0,0", FilesPerLevel());
		System.err.println(FilesPerLevel());

		// Make files spanning the following ranges in level-0:
		// files[0] 200 .. 900
		// files[1] 300 .. 500
		// Note that files are sorted by smallest key.
		ASSERT_OK(Put("300", "v300"));
		ASSERT_OK(Put("500", "v500"));
		dbfull().TEST_CompactMemTable();
		ASSERT_OK(Put("200", "v200"));
		ASSERT_OK(Put("600", "v600"));
		ASSERT_OK(Put("900", "v900"));
		dbfull().TEST_CompactMemTable();
		ASSERT_EQ("2,1,1,0,0,0,0", FilesPerLevel());
		System.err.println(FilesPerLevel());

		// Compact away the placeholder files we created initially
		dbfull().TEST_CompactRange(1, null, null);
		dbfull().TEST_CompactRange(2, null, null);
		ASSERT_EQ("2,0,0,0,0,0,0", FilesPerLevel());
		System.err.println(FilesPerLevel());

		// Do a memtable compaction. Before bug-fix, the compaction would
		// not detect the overlap with level-0 files and would incorrectly place
		// the deletion in a deeper level.
		ASSERT_OK(Delete(new Slice("600")));
		dbfull().TEST_CompactMemTable();
		ASSERT_EQ("3,0,0,0,0,0,0", FilesPerLevel());
		System.err.println(FilesPerLevel());
		ASSERT_EQ("NOT_FOUND", Get("600"));
	}

	public void testL0_CompactionBug_Issue44_a() {
		Reopen();
		ASSERT_OK(Put("b", "v"));
		Reopen();
		ASSERT_OK(Delete("b"));
		ASSERT_OK(Delete("a"));
		Reopen();
		ASSERT_OK(Delete("a"));
		Reopen();
		ASSERT_OK(Put("a", "v"));
		Reopen();
		Reopen();
		ASSERT_EQ("(a->v)", Contents());
		env_.SleepForMicroseconds(10); // Wait for compaction to finish
		ASSERT_EQ("(a->v)", Contents());
	}

	/**
	 * wlu, 2012-7-10: TODO: exception comes up <b>Sometimes</b>
	 */
	public void testL0_CompactionBug_Issue44_b() {
		Reopen();
		Put("", "");
		Reopen();
		Delete("e");
		Put("", "");
		Reopen();
		Put("c", "cv");
		Reopen();
		Put("", "");
		Reopen();
		Put("", "");
		env_.SleepForMicroseconds(10); // Wait for compaction to finish
		Reopen();
		Put("d", "dv");
		Reopen();
		Put("", "");
		Reopen();
		Delete("d");
		Delete("b");
		Reopen();
		ASSERT_EQ("(->)(c->cv)", Contents());
		env_.SleepForMicroseconds(10); // Wait for compaction to finish
		ASSERT_EQ("(->)(c->cv)", Contents());
	}

	public void testComparatorCheck() {
		class NewComparator extends Comparator {
			public String Name() {
				return "leveldb.NewComparator";
			}

			public int Compare(Slice a, Slice b) {
				return BytewiseComparatorImpl.getInstance().Compare(a, b);
			}

			public byte[] FindShortestSeparator(byte[] s, Slice l) {
				return BytewiseComparatorImpl.getInstance()
						.FindShortestSeparator(s, l);
			}

			public byte[] FindShortSuccessor(byte[] key) {
				return BytewiseComparatorImpl.getInstance().FindShortSuccessor(
						key);
			}
		}
		NewComparator cmp = new NewComparator();
		Options new_options = new Options();
		new_options.comparator = cmp;
		Status s_ = new Status();
		DB d = TryReopen(new_options, s_);
		assertTrue(d == null);
		System.out.println(s_);
		assertTrue(s_.toString(), s_.toString().contains("comparator"));

	}

	public void testCustomComparator() {
		class NumberComparator extends Comparator {
			public String Name() {
				return "test.NumberComparator";
			}

			public int Compare(Slice a, Slice b) {
				return ToNumber(a) - ToNumber(b);
			}

			public byte[] FindShortestSeparator(byte[] s, Slice l) {
				ToNumber(new Slice(s)); // Check format
				ToNumber(l); // Check format
				return s;
			}

			public byte[] FindShortSuccessor(byte[] key) {
				ToNumber(key); // Check format
				return key;
			}

			int ToNumber(Slice x) {
				// Check that there are no extra characters.
				assertTrue(x.size() >= 2 && x.get(0) == '['
						&& x.get(x.size() - 1) == ']');
				int val;
				String s = x.toString().substring(1, x.size() - 1);
				try {
					val = Integer.parseInt(s);
				} catch (Exception e) {
					// wlu, 2012-7-10, it's tricky here
					val = Integer.parseInt(s.substring(2), 16);
				}
				return val;
			}

			int ToNumber(byte[] x) {
				return ToNumber(new Slice(x));
			}
		}
		NumberComparator cmp = new NumberComparator();
		Options new_options = new Options();
		new_options.create_if_missing = true;
		new_options.comparator = cmp;
		new_options.write_buffer_size = 1000; // Compact more often
		DestroyAndReopen(new_options);
		ASSERT_OK(Put("[10]", "ten"));
		ASSERT_OK(Put("[20]", "twenty"));
		for (int i = 0; i < 2; i++) {
			ASSERT_EQ("ten", Get("[10]"));
			ASSERT_EQ("ten", Get("[0xa]"));
			ASSERT_EQ("twenty", Get("[20]"));
			ASSERT_EQ("twenty", Get("[0x14]"));
			Compact("[0]", "[9999]");
		}

		for (int run = 0; run < 2; run++) {
			for (int i = 0; i < 1000; i++) {
				String buf = String.format("[%d]", i * 10);
				// char buf[100];
				// snprintf(buf, sizeof(buf), "[%d]", i*10);
				ASSERT_OK(Put(buf, buf));
			}
			Compact("[0]", "[1000000]");
		}
	}

	public void testManualCompaction() {
		assertTrue("Need to update this test to match kMaxMemCompactLevel",
				config.kMaxMemCompactLevel == 2);

		MakeTables(3, "p", "q");
		ASSERT_EQ("1,1,1,0,0,0,0", FilesPerLevel());

		// Compaction range falls before files
		Compact("", "c");
		ASSERT_EQ("1,1,1,0,0,0,0", FilesPerLevel());

		// Compaction range falls after files
		Compact("r", "z");
		ASSERT_EQ("1,1,1,0,0,0,0", FilesPerLevel());

		// Compaction range overlaps files
		Compact("p1", "p9");
		ASSERT_EQ("0,0,1,0,0,0,0", FilesPerLevel());

		// Populate a different range
		MakeTables(3, "c", "e");
		ASSERT_EQ("1,1,2,0,0,0,0", FilesPerLevel());

		// Compact just the new range
		Compact("b", "f");
		ASSERT_EQ("0,0,2,0,0,0,0", FilesPerLevel());

		// Compact all
		MakeTables(1, "a", "z");
		ASSERT_EQ("0,1,2,0,0,0,0", FilesPerLevel());
		db_.CompactRange(null, null);
		ASSERT_EQ("0,0,1,0,0,0,0", FilesPerLevel());
	}

	public void testDBOpen_Options() {
		String dbname = TableTest.TmpDir() + "/db_options_test";
		DB.DestroyDB(dbname, new Options());

		// Does not exist, and create_if_missing == false: error
		DB db = null;
		Options opts = new Options();
		opts.create_if_missing = false;
		Status s = new Status();
		db = DB.Open(opts, dbname, s);
		assertTrue(s.toString().contains("does not exist"));
		assertTrue(db == null);

		// Does not exist, and create_if_missing == true: OK
		opts.create_if_missing = true;
		db = DB.Open(opts, dbname, s);
		ASSERT_OK(s);
		assertTrue(db != null);

		db.Close();
		db = null;

		// Does exist, and error_if_exists == true: error
		opts.create_if_missing = false;
		opts.error_if_exists = true;
		db = DB.Open(opts, dbname, s);
		assertTrue(s.toString().contains("exists"));
		assertTrue(db == null);

		// Does exist, and error_if_exists == false: OK
		opts.create_if_missing = true;
		opts.error_if_exists = false;
		db = DB.Open(opts, dbname, s);
		ASSERT_OK(s);
		assertTrue(db != null);

		db.Close();
		db = null;
	}

	public void testNoSpace() {
		Options options = new Options();
		options.env = env_;
		Reopen(options);

		ASSERT_OK(Put("foo", "v1"));
		ASSERT_EQ("v1", Get("foo"));
		Compact("a", "z");
		int num_files = CountFiles();
		env_.no_space_.Release_Store(env_); // Force out-of-space errors
		for (int i = 0; i < 10; i++) {
			for (int level = 0; level < config.kNumLevels - 1; level++) {
				dbfull().TEST_CompactRange(level, null, null);
			}
		}
		env_.no_space_.Release_Store(null);
		assertTrue(CountFiles() < num_files + 5);
	}

	// TODO: without reopenning, the number is not the same, guess: didn't delete files when compactings
	public void testFilesDeletedAfterCompaction() {
		ASSERT_OK(Put("foo", "v2"));
		Compact("a", "z");
		//Reopen();
		int num_files = CountFiles();
		for (int i = 0; i < 10; i++) {
			ASSERT_OK(Put("foo", "v2"));
			Compact("a", "z");
		}
		//Reopen();
		assertTrue(CountFiles() == num_files);
	}

	public static Test suite() {
		TestSuite suite = new TestSuite("DBTest Test");
		// suite.addTestSuite(TableTest.class);
		suite.addTestSuite(DBTest.class);
		return suite;
	}

	public static void main(String args[]) {
		TestRunner.run(suite());
	}

}
