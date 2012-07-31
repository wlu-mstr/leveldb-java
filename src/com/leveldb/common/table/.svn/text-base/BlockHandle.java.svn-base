package com.leveldb.common.table;

import com.leveldb.common.ByteCollection;
import com.leveldb.util.coding;
import com.leveldb.util.util;

/**
 * Description of a Block {offset in the sst file; size_ of the Block}
 * @author wlu
 *
 */
public class BlockHandle {
	public BlockHandle() {
		offset_ = -1;
		size_ = -1;
	}

	// The offset of the block in the file.
	public long offset() {
		return offset_;
	}

	public void set_offset(long offset) {
		offset_ = offset;
	}

	// The size of the stored block
	long size() {
		return size_;
	}

	void set_size(long size) {
		size_ = size;
	}

	public byte[] EncodeTo() {
		byte[] dst = new byte[0];
		return util.add(dst, coding.PutVarint64(offset_), coding.PutVarint64(size_));
	}

	//I use ByteCollection as input rather than Slice, because this will make everything easier
	public int DecodeFrom(ByteCollection input) {
		//ByteCollection bc = new ByteCollection(input.data(), 0);
		offset_ = coding.GetVarint64(input);
		size_ = coding.GetVarint64(input);
		return input.curr_pos;// return the current pos (as length)
	}

	// Maximum encoding length of a BlockHandle
	public static int kMaxEncodedLength = 20;

	private long offset_;
	private long size_;

}
