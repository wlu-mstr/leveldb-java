package com.leveldb.common;

import com.leveldb.common.WriteBatch.MemTableInserter;
import com.leveldb.common.db.MemTable;
import com.leveldb.util.SequenceNumber;
import com.leveldb.util.ValueType;
import com.leveldb.util.util;

// this is a Wrapper of WriteBatch
public class WriteBatchInternal {

	/**
	 * Return the number of entries in the batch.
	 * The 8~11 bytes
	 * @param batch
	 * @return
	 */
	public static int Count(WriteBatch batch) {
		return util.toInt(batch.rep_.bytes, 8);
	}

	// Set the count for the number of entries in the batch.
	public static void SetCount(WriteBatch batch, int n) {
		util.putInt(batch.rep_.bytes, 8, n);
	}

	// Return the seqeunce number for the start of this batch.
	public static SequenceNumber Sequence(WriteBatch batch) {
		return new SequenceNumber(util.toLong(batch.rep_.bytes));
	}

	// Store the specified number as the seqeunce number for the start of
	// this batch.
	public static void SetSequence(WriteBatch batch, SequenceNumber seq) {
		util.putLong(batch.rep_.bytes, 0, seq.value);
	}

	public static void SetSequence(WriteBatch batch, long seq) {
		util.putLong(batch.rep_.bytes, 0, seq);
	}

	public static Slice Contents(WriteBatch batch) {
		return new Slice(batch.rep_.bytes);
	}

	public static int ByteSize(WriteBatch batch) {
		return batch.rep_.bytes.length;
	}

	public static void SetContents(WriteBatch b, Slice contents) {
		assert (contents.size() >= WriteBatch.kHeader);
		b.rep_.bytes = contents.data();
	}

	public static Status InsertInto(WriteBatch b, MemTable memtable) {
		MemTableInserter inserter = b.inserter;
		inserter.sequence_ = WriteBatchInternal.Sequence(b);
		inserter.mem_ = memtable;
		return b.Iterate(inserter);
	}

	public static void Append(WriteBatch dst, WriteBatch src) {
		SetCount(dst, Count(dst) + Count(src));
		assert (src.rep_.bytes.length >= WriteBatch.kHeader);
		byte[] src_ = src.rep_.bytes;
		dst.rep_.bytes = util.add(dst.rep_.bytes, 
				util.end(src_, src_.length - WriteBatch.kHeader)); //  remove Header of src 
	}
}
