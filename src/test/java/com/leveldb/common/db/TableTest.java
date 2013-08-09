package com.leveldb.common.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import com.leveldb.common.ByteVector;
import com.leveldb.common.Comparator;
import com.leveldb.common.Env;
import com.leveldb.common.Iterator;
import com.leveldb.common.Slice;
import com.leveldb.common.Status;
import com.leveldb.common.Table;
import com.leveldb.common.WriteBatch;
import com.leveldb.common.WriteBatchInternal;
import com.leveldb.common.config;
import com.leveldb.common.comparator.BytewiseComparatorImpl;
import com.leveldb.common.comparator.InternalKeyComparator;
import com.leveldb.common.db.DB;
import com.leveldb.common.db.InternalKey;
import com.leveldb.common.db.MemTable;
import com.leveldb.common.db.ParsedInternalKey;
import com.leveldb.common.db.Range;
import com.leveldb.common.file._RandomAccessFile;
import com.leveldb.common.file._WritableFile;
import com.leveldb.common.options.CompressionType;
import com.leveldb.common.options.Options;
import com.leveldb.common.options.ReadOptions;
import com.leveldb.common.options.WriteOptions;
import com.leveldb.common.table.Block;
import com.leveldb.common.table.BlockBuilder;
import com.leveldb.common.table.TableBuilder;
import com.leveldb.util.SequenceNumber;
import com.leveldb.util.ValueType;
import com.leveldb.util.util;

/**
 * find similar problem when num_entry from 2500 to 20000
 * 
 * @author Administrator
 * 
 */
public class TableTest extends TestCase {

	private static void ASSERT_TRUE(boolean b, String string) {
		if (!b) {
			System.out.println(string);
		} else {
			// System.out.println(":):)::):):)::):):):):):):):):):):):):):):)");
		}
		assertEquals(b, true);

	}

	// Return reverse of "key".
	// Used to test non-lexicographic comparators.
	static byte[] Reverse(Slice key) {
		byte[] rev = new byte[key.size()];
		for (int i = 0; i < key.size(); i++) {
			rev[key.size() - i - 1] = key.get(i);
		}

		return rev;
	}

	static byte[] Reverse(byte[] key) {
		byte[] r = new byte[key.length];
		int j = 0;
		for (int i = key.length - 1; i >= 0; i--) {
			r[j] = key[i];
			j++;
		}
		return r;
	}

	static class ReverseKeyComparator extends Comparator {
		public String Name() {
			return "leveldb.ReverseBytewiseComparator";
		}

		public int Compare(Slice a, Slice b) {
			return BytewiseComparatorImpl.getInstance().Compare(a, b);
		}

		public byte[] FindShortestSeparator(byte[] start, Slice limit) {
			byte[] s = Reverse(start);
			byte[] l = Reverse(limit);

			start = BytewiseComparatorImpl.getInstance().FindShortestSeparator(
					s, new Slice(l));
			// BytewiseComparator()->FindShortestSeparator(&s, l);
			return Reverse(start);
		}

		public byte[] FindShortSuccessor(byte[] key) {
			byte[] s = Reverse(key);
			s = BytewiseComparatorImpl.getInstance().FindShortSuccessor(s);
			return Reverse(s);
		}
	}

	static ReverseKeyComparator reverse_key_comparator = new ReverseKeyComparator();

	// TODO: not sure about this
	static byte[] Increment(Comparator cmp, byte[] key) {
		if (cmp == BytewiseComparatorImpl.getInstance()) {
			return util.add(key, new byte[] { 0 });
		} else {
			assert (cmp == reverse_key_comparator);
			byte[] rev = Reverse(key);
			rev = util.add(rev, new byte[] { 0 });
			return Reverse(rev);
		}
	}

	static class STLLessThan implements java.util.Comparator<byte[]> {
		Comparator cmp;

		public STLLessThan() {
			cmp = BytewiseComparatorImpl.getInstance();
		}

		public STLLessThan(final Comparator c) {
			cmp = c;
		}

		boolean less(String a, String b) {
			return cmp.Compare(new Slice(a), new Slice(b)) < 0;
		}

		@Override
		public int compare(byte[] a, byte[] b) {
			return cmp.Compare(new Slice(a), new Slice(b));
		}

	}

	static class StringSink extends _WritableFile {
		public byte[] contents() {
			return (contents_.getData());
		}

		public Status Close() {
			return Status.OK();
		}

		public Status Flush() {
			return Status.OK();
		}

		public Status Sync() {
			return Status.OK();
		}

		public Status Append(Slice data) {
			contents_.append(data.data());
			return Status.OK();
		}

		private ByteVector contents_ = new ByteVector();
	}

	static class StringSource extends _RandomAccessFile {
		public StringSource(Slice contents) {
			contents_.clear();
			contents_.append(contents.data());
		}

		public long Size() {
			return contents_.getSize();
		}

		/**
		 * n is # bytes to read
		 */
		public byte[] Read(long offset, int n, Slice result) {
			if (offset > contents_.getSize()) {
				return null;
				// return Status.InvalidArgument(new
				// Slice("invalid Read offset"), null);
			}
			if (offset + n > contents_.getSize()) {
				n = (int) (contents_.getSize() - offset);
			}

			result.setData_(util.subN(contents_.getRawRef(), (int) offset, n));
			return result.data();
		}

		private ByteVector contents_ = new ByteVector(128);

		@Override
		public void Close() {

		}

		@Override
		public String FileName() {
			return "StringSource";
		}
	}

	// Helper class for tests to unify the interface between
	// BlockBuilder/TableBuilder and Block/Table.
	static abstract class Constructor {
		public Constructor(Comparator cmp) {
			data_ = new TreeMap<byte[], byte[]>(new STLLessThan(cmp));
		}

		void Add(final byte[] key, Slice value) {
			// data_[key] = value.ToString();
			data_.put(key, value.data());

		}

		void Add(final String key, String value) {
			// data_[key] = value.ToString();
			data_.put(util.toBytes(key), util.toBytes(value));

		}

		// Finish constructing the data structure with all the keys that have
		// been added so far. Returns the keys in sorted order in "*keys"
		// and stores the key/value pairs in "*kvmap"
		void Finish(Options options, List<byte[]> keys,
				TreeMap<byte[], byte[]> kvmap) {
			kvmap.clear();
			kvmap.putAll(data_);
			TreeMap<byte[], byte[]> tmp_data_ = new TreeMap<byte[], byte[]>(
					data_.comparator());
			for (byte[] k_ : data_.keySet()) {
				tmp_data_.put(util.deepCopy(k_), data_.get(k_));
			}
			keys.clear();

			for (byte[] k : kvmap.keySet()) {
				keys.add(util.deepCopy(k));
			}

			data_.clear(); // wlu, 2012-6-16
			Status s = FinishImpl(options, kvmap);

			for (byte[] k_ : tmp_data_.keySet()) {
				kvmap.put(util.deepCopy(k_), tmp_data_.get(k_));
			}
			keys.clear();

			for (byte[] k : kvmap.keySet()) {
				keys.add(util.deepCopy(k));
			}

			ASSERT_TRUE(s.ok(), s.toString());
		}

		// Construct the data structure from the data in "data"
		abstract Status FinishImpl(Options options, TreeMap<byte[], byte[]> data);

		abstract int NumBytes();

		abstract Iterator NewIterator();

		TreeMap<byte[], byte[]> data() {
			return data_;
		}

		DB db() {
			return null;
		} // Overridden in DBConstructor

		private TreeMap<byte[], byte[]> data_;
	}

	static class BlockConstructor extends Constructor {
		public BlockConstructor(Comparator cmp) {
			super(cmp);
			comparator_ = cmp;
			block_size_ = -1;
			block_ = null;
		}

		Status FinishImpl(Options options, TreeMap<byte[], byte[]> data) {
			block_ = null;
			BlockBuilder builder = new BlockBuilder(options);

			for (byte[] it : data.keySet()) {
				builder.Add(new Slice(it), new Slice(data.get(it)));
			}

			// Open the block
			Slice block_data = builder.Finish();
			block_size_ = block_data.size();
			byte block_data_copy[] = new byte[block_size_];
			System.arraycopy(block_data.data(), 0, block_data_copy, 0,
					block_size_);
			block_ = new Block(block_data_copy, block_size_, true /*
																 * take
																 * ownership
																 */);
			return Status.OK();
		}

		int NumBytes() {
			return block_size_;
		}

		Iterator NewIterator() {
			return block_.NewIterator(comparator_);
		}

		private Comparator comparator_;
		int block_size_;
		Block block_;

	}

	static class TableConstructor extends Constructor {
		public TableConstructor(Comparator cmp) {
			super(cmp);
			source_ = null;
			table_ = null;
		}

		Status FinishImpl(Options options, TreeMap<byte[], byte[]> _data) {
			// need to deep copy the input data, 'cause key is changed inner
			TreeMap<byte[], byte[]> data = new TreeMap<byte[], byte[]>(
					_data.comparator());
			for (byte[] k : _data.keySet()) {
				// System.out.println(util.toString(k) +":" +
				// util.toString(_data.get(k)));
				data.put(util.deepCopy((k)), util.deepCopy(_data.get(k)));
			}
			Reset();
			StringSink sink = new StringSink();
			TableBuilder builder = new TableBuilder(options, sink);

			for (byte[] it : data.keySet()) {
				builder.Add(new Slice(it), new Slice(data.get(it)));
				ASSERT_TRUE(builder.status().ok(), builder.status().toString());
			}

			Status s = builder.Finish();
			ASSERT_TRUE(s.ok(), s.toString());

			ASSERT_TRUE(sink.contents().length == builder.FileSize(),
					"length not same");

			// Open the table
			source_ = new StringSource(new Slice(sink.contents()));
			Options table_options = new Options();
			table_options.comparator = options.comparator;
			table_ = Table.Open(table_options, source_, sink.contents().length);

			return Status.OK();
		}

		int NumBytes() {
			return (int) source_.Size();
		}

		Iterator NewIterator() {
			return table_.NewIterator(new ReadOptions());
		}

		long ApproximateOffsetOf(Slice key) {
			return table_.ApproximateOffsetOf(key);
		}

		long ApproximateOffsetOf(String key) {
			return table_.ApproximateOffsetOf(new Slice(key));
		}

		private void Reset() {
			table_ = null;
			source_ = null;
			table_ = null;
			source_ = null;
		}

		StringSource source_;
		Table table_;

	}

	// A helper class that converts internal format keys into user keys
	static class KeyConvertingIterator extends Iterator {
		public KeyConvertingIterator(Iterator iter) {
			iter_ = iter;
		}

		public boolean Valid() {
			return iter_.Valid();
		}

		public void Seek(Slice target) {
			ParsedInternalKey ikey = new ParsedInternalKey(target,
					SequenceNumber.MaxSequenceNumber, ValueType.TypeValue);
			byte[] encoded = new byte[0];
			encoded = InternalKey.AppendInternalKey(encoded, ikey);
			iter_.Seek(new Slice(encoded));
		}

		public void SeekToFirst() {
			iter_.SeekToFirst();
		}

		public void SeekToLast() {
			iter_.SeekToLast();
		}

		public void Next() {
			iter_.Next();
		}

		public void Prev() {
			iter_.Prev();
		}

		@Override
		public Slice key() {
			assert (Valid());
			ParsedInternalKey key = null;
			key = InternalKey.ParseInternalKey_(iter_.key());
			if (key == null) {
				status_ = Status.Corruption(
						new Slice("malformed internal key"), null);
				return new Slice("corrupted key");
			}
			return key.user_key;
		}

		public Slice value() {
			return iter_.value();
		}

		public Status status() {
			return status_.ok() ? iter_.status() : status_;
		}

		private Status status_;
		Iterator iter_;

		// No copying allowed
	}

	static class MemTableConstructor extends Constructor {
		public MemTableConstructor(Comparator cmp) {
			super(cmp);
			internal_comparator_ = new InternalKeyComparator(cmp);
			memtable_ = new MemTable(internal_comparator_);
			memtable_.Ref();
		}

		Status FinishImpl(Options options, TreeMap<byte[], byte[]> data) {
			memtable_.Unref();
			memtable_ = new MemTable(internal_comparator_);
			memtable_.Ref();
			int seq = 1;
			for (byte[] it : data.keySet()) {
				memtable_.Add(new SequenceNumber(seq), ValueType.kTypeValue,
						new Slice(it), new Slice(data.get(it)));
				seq++;
			}
			return Status.OK();
		}

		int NumBytes() {
			return (int) memtable_.ApproximateMemoryUsage();
		}

		Iterator NewIterator() {
			return new KeyConvertingIterator(memtable_.NewIterator());
		}

		private InternalKeyComparator internal_comparator_;
		MemTable memtable_;
	}

	static class DBConstructor extends Constructor {
		public DBConstructor(Comparator cmp) {
			super(cmp);
			comparator_ = cmp;
			db_ = null;
			// NewDB();
		}

		Status FinishImpl(Options options, TreeMap<byte[], byte[]> data) {
			db_ = null;
			NewDB();
			for (byte[] it : data.keySet()) {
				WriteBatch batch = new WriteBatch();
				batch.Put(new Slice(it), new Slice(data.get(it)));
				ASSERT_TRUE(db_.Write(new WriteOptions(), batch).ok(),
						"db_ Write batch error");
			}
			return Status.OK();
		}

		int NumBytes() {
			byte[] e = new byte[] { 127, 127 }; // max value of byte
			Range r = new Range(new Slice(""), new Slice(e));
			long size = db_.GetApproximateSizes(r);
			return (int) size;
		}

		Iterator NewIterator() {
			return db_.NewIterator(new ReadOptions());
		}

		DB db() {
			return db_;
		}

		private void NewDB() {
			String name = TmpDir() + "/table_testdb";

			Options options = new Options();
			options.comparator = comparator_;
			Status status = DB.DestroyDB(name, options);
			ASSERT_TRUE(status.ok(), status.toString());

			options.create_if_missing = true;
			options.error_if_exists = true;
			options.write_buffer_size = 1000; // Something small to force
												// merging
			db_ = DB.Open(options, name);
			// ASSERT_TRUE(status.ok()) << status.ToString();
		}

		Comparator comparator_;
		DB db_;
	}

	enum TestType {
		TABLE_TEST, BLOCK_TEST, MEMTABLE_TEST, DB_TEST
	}

	static class TestArgs {
		TestType type;
		boolean reverse_compare;
		int restart_interval; // WTF

		public TestArgs(TestType type_, boolean rev, int restart_interval_) {
			type = type_;
			reverse_compare = rev;
			restart_interval = restart_interval_;
		}
	}

	static TestArgs kTestArgList[] = {
			new TestArgs(TestType.TABLE_TEST, false, 16),
			new TestArgs(TestType.TABLE_TEST, false, 1),
			new TestArgs(TestType.TABLE_TEST, false, 1024),
			// ///////////////////////////////////////////////////
			new TestArgs(TestType.TABLE_TEST, false, 16),
			new TestArgs(TestType.TABLE_TEST, false, 1),
			new TestArgs(TestType.TABLE_TEST, false, 1024), // 5
			// ///////////////////////////////////////////////////

			new TestArgs(TestType.BLOCK_TEST, false, 16),
			new TestArgs(TestType.BLOCK_TEST, false, 1),
			new TestArgs(TestType.BLOCK_TEST, false, 1024),

			new TestArgs(TestType.BLOCK_TEST, true, 16),
			new TestArgs(TestType.BLOCK_TEST, true, 1), // 10
			new TestArgs(TestType.BLOCK_TEST, true, 1024),

			// Restart interval does not matter for memtables
			new TestArgs(TestType.MEMTABLE_TEST, false, 16), // 12
			new TestArgs(TestType.MEMTABLE_TEST, true, 16),

			// Do not bother with restart interval variations for DB
			new TestArgs(TestType.DB_TEST, false, 16),
			new TestArgs(TestType.DB_TEST, false, 16) }; // 15

	static final int kNumTestArgs = kTestArgList.length;

	

		void Init(TestArgs args) {
			constructor_ = null;
			options_ = new Options();

			options_.block_restart_interval = args.restart_interval;
			// Use shorter block size for tests to exercise block boundary
			// conditions more.
			options_.block_size = 256;
			if (args.reverse_compare) {
				options_.comparator = reverse_key_comparator;
			}
			switch (args.type) {
			case TABLE_TEST:
				constructor_ = new TableConstructor(options_.comparator);
				break;
			case BLOCK_TEST:
				constructor_ = new BlockConstructor(options_.comparator);
				break;
			case MEMTABLE_TEST:
				constructor_ = new MemTableConstructor(options_.comparator);
				break;
			case DB_TEST:
				constructor_ = new DBConstructor(options_.comparator);
				break;
			}
		}

		void Add(byte[] key, byte[] value) {
			constructor_.Add(key, new Slice(value));
		}

		void Test(Random rnd) {
			Test(rnd, true);
		}

		void Close() {
			DB d = constructor_.db();
			if (d != null) {
				d.Close();
			}
		}

		void Test(Random rnd, boolean b) {
			List<byte[]> keys = new ArrayList<byte[]>();
			TreeMap<byte[], byte[]> data = new TreeMap<byte[], byte[]>(
					new STLLessThan());
			constructor_.Finish(options_, keys, data);

			System.out.println("Forward Scan");
			TestForwardScan(keys, data);
			System.out.println("Backward Scan");
			TestBackwardScan(keys, data);
			System.out.println("Random Scan");
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			TestRandomAccess(rnd, keys, data);

			DB d = constructor_.db();
			if (b) {
				if (d != null) {
					d.Close();
				}
			}
		}

		void TestForwardScan(final List<byte[]> keys,
				final TreeMap<byte[], byte[]> data) {

			Iterator iter = constructor_.NewIterator();
			ASSERT_TRUE(!iter.Valid(), "iter not valid");
			iter.SeekToFirst();

			String orgstr[] = new String[data.size()];
			List<String> resstr = new ArrayList<String>(data.size());
			int i = 0;
			for (byte[] k : data.keySet()) {
				String str1 = ToString(k, data.get(k));
				orgstr[i] = str1;
				i++;

			}
			i = 0;
			while (iter.Valid()) {
				String str2 = ToString(iter);
				resstr.add(str2);
				i++;
				iter.Next();
			}
			// compare the String arrays
			ASSERT_TRUE(orgstr.length == resstr.size(), "NOT EQUAL");

			// for (String s : orgstr) {
			// System.out.println(s);
			// }
			// System.out.println("-------------------------------");
			// for (String s : resstr) {
			// System.out.println(s);
			// }

			for (String s : orgstr) {
				ASSERT_TRUE(resstr.contains(s),
						s + " [ !Equal ] " + resstr.get(resstr.indexOf(s)));
			}

			ASSERT_TRUE(!iter.Valid(), "iter not valid");
			iter = null;
		}

		String ToString(byte[] k, byte[] v) {
			return "'" + util.toString(k) + "->" + util.toString(v) + "'";
		}

		void TestBackwardScan(List<byte[]> keys, TreeMap<byte[], byte[]> data) {
			Iterator iter = constructor_.NewIterator();
			ASSERT_TRUE(!iter.Valid(), "iter not valid");
			iter.SeekToLast();
			// reverse keys of the map
			Set<byte[]> orgset = data.keySet();
			List<byte[]> reversedset2list = new ArrayList<byte[]>();
			for (byte[] s : orgset) {
				reversedset2list.add(s);
			}
			Collections.reverse(reversedset2list);

			// List<String> resstr = new ArrayList<String>(data.size());

			// reverse visit
			for (byte[] k : reversedset2list) {
				String str1 = ToString(k, data.get(k));
				String str2 = ToString(iter);
				ASSERT_TRUE(str1.compareTo(str2) == 0, str1 + " [ !Equal ] "
						+ str2);
				iter.Prev();
			}

			ASSERT_TRUE(!iter.Valid(), "iter not valid");
		}

		void TestRandomAccess(Random rnd, List<byte[]> keys,
				TreeMap<byte[], byte[]> data) {

			boolean kVerbose = false;
			Iterator iter = constructor_.NewIterator();
			ASSERT_TRUE(!iter.Valid(), "iter not valid");
			// KVMap::const_iterator model_iter = data.begin();
			Set<byte[]> keyset = data.keySet();
			byte[][] keyArray = new byte[keyset.size()][];
			int i_ = 0;
			for (byte[] b_ : keyset) {
				keyArray[i_] = util.deepCopy(b_);
				i_++;
			}

			int model_iter = 0;
			iter.SeekToFirst();

			if (kVerbose) {
				System.out.println("---");
			}

			TreeMap<byte[], byte[]> _data = new TreeMap<byte[], byte[]>(
					data.comparator());

			// iter.SeekToFirst();

			// deep copy the map for test
			for (byte[] k_ : data.keySet()) {
				_data.put(util.deepCopy(k_), util.deepCopy(data.get(k_)));
				// System.out.println(ToString(iter) + "" +
				// util.toString(k_)+"->"+util.toString(data.get(k_)));
				// iter.Next();
			}

			// _degug

			for (int i = 0; i < 200; i++) {
				int toss = Math.abs(rnd.nextInt()) % 5;
				switch (toss) {
				case 0: {
					if (iter.Valid()) {
						if (kVerbose) {
							System.out.println("Next");
						}
						iter.Next();
						++model_iter;
						// wlu:
						if (model_iter == keyArray.length) {
							ASSERT_TRUE(!iter.Valid(),
									"iter should be invalid when array @end");
							iter.SeekToFirst();
							model_iter = 0;
						}
						byte[] str = keyArray[model_iter];
						byte[] str2 = _data.get(str);
						String _str = ToString(str, str2);
						String _str2 = ToString(iter);
						ASSERT_TRUE(_str.compareTo(_str2) == 0, _str
								+ "[ !equal ] " + _str2);
					}
					break;
				}

				case 1: {
					if (kVerbose) {
						System.out.println("SeekToFirst");
					}
					iter.SeekToFirst();
					model_iter = 0;

					String str = "";
					if (model_iter == keyArray.length) {
						str = "END";
					} else {
						byte[] _str = keyArray[model_iter];
						byte[] str2 = _data.get(_str);
						str = ToString(_str, str2);
					}
					String str2 = ToString(iter);
					ASSERT_TRUE(str.compareTo(str2) == 0, str + "[ !equal ] "
							+ str2);

					break;
				}

				case 2: {
					byte[] key = PickRandomKey(rnd, keys);
					// for(byte[] k:_data.keySet()){
					// System.out.print(util.toString(k) + ":" +
					// util.toString(_data.get(k)));
					// }
					// for(byte[] k:keys){
					// System.out.println(util.toString(k));
					// }
					Entry<byte[], byte[]> kv = _data.floorEntry(key);
					// model_iter = data.lower_bound(key);
					if (kv == null) {
						break;
					}
					if (kVerbose) {
						System.out.println(kv + " = floor("
								+ util.toString(key) + ")");
						System.out.println("seek '"
								+ util.toString(kv.getKey()) + " = floor("
								+ util.toString(key) + ")" + "'");
					}

					iter.Seek(new Slice(kv.getKey()));
					for (i_ = 0; i_ < keyArray.length; i_++) {
						if (util.compareTo(keyArray[i_], kv.getKey()) == 0) {
							model_iter = i_;
						}
					}
					String str1 = "";
					if (kv == null) {
						str1 = "END";
					} else {
						str1 = ToString(kv.getKey(), kv.getValue());
					}
					String str2 = ToString(iter);
					ASSERT_TRUE(str1.compareTo(str2) == 0, str1
							+ " [ !Equal ] " + str2);
					break;
				}

				case 3: {
					if (iter.Valid()) {
						if (kVerbose) {
							System.out.println("Prev");
						}
						iter.Prev();
						if (model_iter == 0) {
							ASSERT_TRUE(!iter.Valid(),
									"iter should be invalid when array @begin");
							model_iter = keyArray.length - 1; // Wrap around to
																// invalid value
							//
							iter.SeekToLast();
						} else {
							--model_iter;
						}
						if (model_iter < 0) {
							continue;
						}

						byte[] str = keyArray[model_iter];
						byte[] str2 = _data.get(str);
						String _str = ToString(str, str2);
						String _str2 = ToString(iter);
						ASSERT_TRUE(_str.compareTo(_str2) == 0, _str
								+ "[ !equal ] " + _str2);
					}
					break;
				}

				case 4: {
					if (kVerbose) {
						System.out.println("SeekToLast");
					}
					iter.SeekToLast();
					model_iter = keyArray.length - 1;

					Entry<byte[], byte[]> kv = _data.lastEntry();

					String str1 = "";
					if (kv == null) {
						str1 = "END";
					} else {
						str1 = ToString(kv.getKey(), kv.getValue());
					}
					String str2 = ToString(iter);
					ASSERT_TRUE(str1.compareTo(str2) == 0, str1
							+ " [ !Equal ] " + str2);
					break;
				}
				}
			}
		}

		String ToString(Iterator it) {
			if (!it.Valid()) {
				return "END";
			} else {
				return "'" + it.key().toString() + "->" + it.value().toString()
						+ "'";
			}
		}

		byte[] PickRandomKey(Random rnd, List<byte[]> keys) {
			if (keys.isEmpty()) {
				return util.toBytes("foo");
			} else {
				int index = Math.abs(rnd.nextInt()) % (keys.size());
				byte[] result = keys.get(index);
				switch (Math.abs(rnd.nextInt()) % 3) {
				case 0:
					// Return an existing key
					break;
				case 1: {
					// Attempt to return something smaller than an existing key
					int len = result.length;
					if (len > 0 && (result[result.length - 1]) > 0) {
						result[result.length - 1] -= 1;
					}

					break;
				}
				case 2: {
					// Return something larger than an existing key
					Increment(options_.comparator, result);
					break;
				}
				}
				return result;
			}
		}

		// Returns NULL if not running against a DB
		DB db() {
			return constructor_.db();
		}

		private Options options_;
		Constructor constructor_;

		// /////////////////////////////////////////////////////////////////////////////////////
		// / Tests go here
		public void testSimpleEmptyKey() {
			for (int i = 0; i < kNumTestArgs; i++) {
				Init(kTestArgList[i]);

				Random rnd = new Random(100);// (test::RandomSeed() + 1);
				Add(util.toBytes(""), util.toBytes("v"));
				System.out.println(i);

				Test(rnd);
			}
		}

		public void testSimpleSingle() {
			for (int i = 0; i < kNumTestArgs; i++) {
				Init(kTestArgList[i]);
				Random rnd = new Random(100);// (test::RandomSeed() + 1);
				Add(util.toBytes("abc"), util.toBytes("v"));
				Test(rnd);
				System.out.println(i);
			}
		}

		public void testSimpleMulti() {
			for (int i = 0; i < kNumTestArgs; i++) {
				Init(kTestArgList[i]);

				Random rnd = new Random(100);
				Add(util.toBytes("abc"), util.toBytes("v"));
				Add(util.toBytes("abcd"), util.toBytes("v"));
				Add(util.toBytes("ac"), util.toBytes("v2"));
				Test(rnd);
				System.out.println(i);
			}
		}

		public void testSimpleSpecialKey() {
			for (int i = 0; i < kNumTestArgs; i++) {
				Init(kTestArgList[i]);
				Random rnd = new Random(100);
				Add(util.toBytes("\0xff\0xff"), util.toBytes("v3"));
				Test(rnd);
				System.out.println(i);
			}
		}

		public void testRandomized() {
			for (int i = 0; i < kNumTestArgs; i++) {
				// if (i != 14) { // _debug
				// continue;
				// }
				System.out.println(i);
				Init(kTestArgList[i]);
				Random rnd = new Random(100);
				for (int num_entries = 1; num_entries < 2000; num_entries += (num_entries < 50 ? 1
						: 200)) {
					// if (num_entries != 250) {// _debug
					// continue;
					// }
					if ((num_entries % 10) == 0) {
						System.out.println("case " + (i + 1) + "of "
								+ kNumTestArgs + ": num_entries = "
								+ num_entries);
					}

					for (int e = 0; e < num_entries; e++) {
						Add(util.RandomKey(rnd, 1 + Math.abs(rnd.nextInt()) % 4),
								(util.RandomKey(rnd,
										1 + Math.abs(rnd.nextInt()) % 5)));
					}

					Test(rnd);
				}

			}

		}

		public void testRandomized2() {
			for (int i = kNumTestArgs - 1; i < kNumTestArgs; i++) {
				System.out.println(i);
				Init(kTestArgList[i]);
				Random rnd = new Random(100);
				int num_entries = 1000;
				System.out.println("case " + (i + 1) + "of " + kNumTestArgs
						+ ": num_entries = " + num_entries);
				// }

				for (int e = 0; e < num_entries; e++) {
					Add(util.RandomKey(rnd, 1 + Math.abs(rnd.nextInt()) % 4),
							(util.RandomKey(rnd,
									1 + Math.abs(rnd.nextInt()) % 5)));
				}

				Test(rnd);

			}

		}

		public void testRandomizedLongDB() {
			Random rnd = new Random(100);
			TestArgs args = new TestArgs(TestType.DB_TEST, false, 16);
			Init(args);
			int num_entries = 100000;
			for (int e = 0; e < num_entries; e++) {
				Add(util.RandomKey(rnd, 1 + Math.abs(rnd.nextInt()) % 4),
						util.toBytes(util.RandomString(rnd,
								1 + Math.abs(rnd.nextInt()) % 5)));
			}
			Test(rnd, false);

			// We must have created enough data to force merging
			int files = 0;
			for (int level = 0; level < config.kNumLevels; level++) {
				StringBuffer value = new StringBuffer();
				String name = "leveldb.num-files-at-level" + level;
				ASSERT_TRUE(db().GetProperty(new Slice(name), value), "fail");
				files += Integer.parseInt(value.toString());
			}
			ASSERT_TRUE(true, "#Files = " + files);
			System.out.println("#Files = " + files);

			StringBuffer value = new StringBuffer();
			db().GetProperty(new Slice("leveldb.stats"), value);
			System.out.println(value.toString());

			Close();
		}


	public static String TmpDir() {
		String dir = Env.Default().GetTestDirectory();

		return dir;
	}

	/**
	 * Test MemTable
	 */
	public void testSimple() {
		InternalKeyComparator cmp = new InternalKeyComparator(
				BytewiseComparatorImpl.getInstance());
		MemTable memtable = new MemTable(cmp);
		memtable.Ref();
		WriteBatch batch = new WriteBatch();
		WriteBatchInternal.SetSequence(batch, 100);
		batch.Put(new Slice("k1"), new Slice("v1"));
		batch.Put(new Slice("k2"), new Slice("v2"));
		batch.Put(new Slice("k3"), new Slice("v3"));
		batch.Put(new Slice("largekey"), new Slice("vlarge"));
		ASSERT_TRUE(WriteBatchInternal.InsertInto(batch, memtable).ok(),
				"insert to batch wrong");

		Iterator iter = memtable.NewIterator();
		iter.SeekToFirst();
		while (iter.Valid()) {
			System.err.println("key: '" + iter.key() + "' -> " + iter.value());
			iter.Next();
		}

		memtable.Unref();
	}

	static boolean Between(long val, long low, long high) {
		boolean result = (val >= low) && (val <= high);
		if (result) {
			System.err.println("Value " + val + " is  in range [" + low + ", "
					+ high + "]");
		}
		return result;
	}

	public static String string(int n, char c) {
		StringBuffer s = new StringBuffer(n);
		for (int i = 0; i < n; i++) {
			s.append(c);
		}
		return s.toString();
	}

	public void testApproximateOffsetOfPlain() {
		TableConstructor c = new TableConstructor(
				BytewiseComparatorImpl.getInstance());
		c.Add("k01", "hello");
		c.Add("k02", "hello2");
		c.Add("k03", string(10000, 'x'));
		c.Add("k04", string(200000, 'x'));
		c.Add("k05", string(300000, 'x'));
		c.Add("k06", "hello3");
		c.Add("k07", string(100000, 'x'));
		List<byte[]> keys = new ArrayList<byte[]>();
		TreeMap<byte[], byte[]> kvmap = new TreeMap<byte[], byte[]>(
				new STLLessThan());
		Options options = new Options();
		options.block_size = 1024;
		options.compression = CompressionType.NoCompression;
		c.Finish(options, keys, kvmap);

		ASSERT_TRUE(Between(c.ApproximateOffsetOf("abc"), 0, 0), "abc");
		ASSERT_TRUE(Between(c.ApproximateOffsetOf("k01"), 0, 0), "k01");
		ASSERT_TRUE(Between(c.ApproximateOffsetOf("k01a"), 0, 0), "k01a");
		ASSERT_TRUE(Between(c.ApproximateOffsetOf("k02"), 0, 0), "k02");
		ASSERT_TRUE(Between(c.ApproximateOffsetOf("k03"), 0, 0), "k03");
		ASSERT_TRUE(Between(c.ApproximateOffsetOf("k04"), 10000, 11000), "k04");
		ASSERT_TRUE(Between(c.ApproximateOffsetOf("k04a"), 210000, 211000),
				"k04a");
		ASSERT_TRUE(Between(c.ApproximateOffsetOf("k05"), 210000, 211000),
				"k05");
		ASSERT_TRUE(Between(c.ApproximateOffsetOf("k06"), 510000, 511000),
				"k06");
		ASSERT_TRUE(Between(c.ApproximateOffsetOf("k07"), 510000, 511000),
				"k07");
		ASSERT_TRUE(Between(c.ApproximateOffsetOf("xyz"), 610000, 611000),
				"xyz");

	}

	public static Test suite() {
		TestSuite suite = new TestSuite("TestSuite Test");
		// suite.addTestSuite(TableTest.class);
		suite.addTestSuite(TableTest.class);
		return suite;
	}

	public static void main(String args[]) {
		// harness.SimpleEmptyKey();
		// System.out.println("-------------------");
		// harness.SimpleSingle();
		// System.out.println("-------------------");
		// harness.SimpleMulti();
		// System.out.println("-------------------");
		// harness.SimpleSpecialKey();
		// harness.Randomized();

		// harness.RandomizedLongDB();

		// MemTableTest mtbl = new MemTableTest();
		// mtbl.Simple();

		// tt.ApproximateOffsetOfPlain();

		TestRunner.run(suite());
	}
}
