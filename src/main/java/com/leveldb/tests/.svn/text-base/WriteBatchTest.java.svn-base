package com.leveldb.tests;

import junit.textui.TestRunner;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.leveldb.common.Iterator;
import com.leveldb.common.Slice;
import com.leveldb.common.Status;
import com.leveldb.common.WriteBatch;
import com.leveldb.common.WriteBatchInternal;
import com.leveldb.common.comparator.BytewiseComparatorImpl;
import com.leveldb.common.comparator.InternalKeyComparator;
import com.leveldb.common.db.InternalKey;
import com.leveldb.common.db.MemTable;
import com.leveldb.common.db.ParsedInternalKey;
import com.leveldb.util.ValueType;

public class WriteBatchTest extends TestCase {
	static void ASSERT_TRUE(boolean b) {
		if (!b) {
			System.out.println(b);
		}
		assertEquals(b, true);
	}

	static void ASSERT_TRUE(boolean b, String error) {
		if (!b) {
			System.out.println(error);
		}
		assertEquals(b, true);
	}

	static String PrintContents(WriteBatch b) {
		InternalKeyComparator cmp = new InternalKeyComparator(
				BytewiseComparatorImpl.getInstance());
		MemTable mem = new MemTable(cmp);
		mem.Ref();
		StringBuffer state = new StringBuffer();
		Status s = WriteBatchInternal.InsertInto(b, mem);
		int count = 0;
		Iterator iter = mem.NewIterator();
		for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
			ParsedInternalKey ikey = InternalKey.ParseInternalKey_(iter.key());
			ASSERT_TRUE(ikey != null);
			switch (ikey.type.value) {
			case ValueType.kTypeValue:
				state.append("Put(");
				state.append(ikey.user_key.toString());
				state.append(", ");
				state.append(iter.value().toString());
				state.append(")");
				count++;
				break;
			case ValueType.kTypeDeletion:
				state.append("Delete(");
				state.append(ikey.user_key.toString());
				state.append(")");
				count++;
				break;
			}
			state.append("@");
			state.append(ikey.sequence.value);
		}
		iter = null;
		if (!s.ok()) {
			state.append("ParseError()");
		} else if (count != WriteBatchInternal.Count(b)) {
			state.append("CountMismatch()");
		}
		mem.Unref();
		return state.toString();
	}

	public void testEmpty() {
		WriteBatch batch = new WriteBatch();
		ASSERT_TRUE("".compareTo(PrintContents(batch)) == 0);
		ASSERT_TRUE(0 == WriteBatchInternal.Count(batch));
	}

	public void testMultiple() {
		WriteBatch batch = new WriteBatch();
		batch.Put(new Slice("foo"), new Slice("bar"));
		batch.Delete(new Slice("box"));
		batch.Put(new Slice("baz"), new Slice("boo"));
		WriteBatchInternal.SetSequence(batch, 100);
		ASSERT_TRUE(100 == WriteBatchInternal.Sequence(batch).value);
		ASSERT_TRUE(3 == WriteBatchInternal.Count(batch));
		ASSERT_TRUE(
				"Put(baz, boo)@102Delete(box)@101Put(foo, bar)@100"
						.compareTo(PrintContents(batch)) == 0,
				PrintContents(batch));
	}

	/**
	 * This test throws an Exception: java.lang.ArrayIndexOutOfBoundsException.
	 * Because the buffer is manually corrupted!
	 */
	public void testCorruption() {
		WriteBatch batch = new WriteBatch();
		batch.Put(new Slice("foo"), new Slice("bar"));
		batch.Delete(new Slice("box"));
		WriteBatchInternal.SetSequence(batch, 200);
		Slice contents = WriteBatchInternal.Contents(batch);
		WriteBatchInternal.SetContents(batch, new Slice(contents.data(),
				contents.size() - 1));
		ASSERT_TRUE(
				"Put(foo, bar)@200ParseError()".compareTo(PrintContents(batch)) == 0,
				PrintContents(batch));
	}

	public void testAppend() {
		WriteBatch b1 = new WriteBatch(), b2 = new WriteBatch();
		WriteBatchInternal.SetSequence(b1, 200);
		WriteBatchInternal.SetSequence(b2, 300);
		WriteBatchInternal.Append(b1, b2);
		ASSERT_TRUE("".compareTo(PrintContents(b1)) == 0);
		b2.Put(new Slice("a"), new Slice("va"));
		WriteBatchInternal.Append(b1, b2);
		ASSERT_TRUE("Put(a, va)@200".compareTo(PrintContents(b1)) == 0);
		b2.Clear();
		b2.Put(new Slice("b"), new Slice("vb"));
		WriteBatchInternal.Append(b1, b2);
		ASSERT_TRUE("Put(a, va)@200Put(b, vb)@201".compareTo(PrintContents(b1)) == 0);
		b2.Delete(new Slice("foo"));
		WriteBatchInternal.Append(b1, b2);
		ASSERT_TRUE("Put(a, va)@200Put(b, vb)@202Put(b, vb)@201Delete(foo)@203"
				.compareTo(PrintContents(b1)) == 0);
	}

	public static void main(String args[]) {
		TestSuite t = new TestSuite(WriteBatchTest.class.getName());
		t.addTestSuite(WriteBatchTest.class);
		TestRunner.run(t);
	}
}
