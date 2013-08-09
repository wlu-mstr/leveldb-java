package com.leveldb.common.table;

import com.leveldb.common.ByteCollection;
import com.leveldb.common.Slice;
import com.leveldb.common.Status;
import com.leveldb.util.util;

//Footer encapsulates the fixed information stored at the tail
//end of every table file.

public class Footer {
	// kTableMagicNumber was picked by running
	// echo http://code.google.com/p/leveldb/ | sha1sum
	// and taking the leading 64 bits.
	public static long kTableMagicNumber = 0x4775248b80fb57l;
	// 1-byte type + 32-bit crc
	public static int kBlockTrailerSize = 5;

	public Footer() {
		metaindex_handle_ = new BlockHandle();
		index_handle_ = new BlockHandle();
	}

	// The block handle for the metaindex block of the table
	public BlockHandle metaindex_handle() {
		return metaindex_handle_;
	}

	public void set_metaindex_handle(BlockHandle h) {
		metaindex_handle_ = h;
	}

	// The block handle for the index block of the table
	public BlockHandle index_handle() {
		return index_handle_;
	}

	public void set_index_handle(BlockHandle h) {
		index_handle_ = h;
	}

	// TODO: maybe change to bytevector for better ...
	public byte[] EncodeTo(byte[] dst) {
		int original_size = dst.length;

		byte[] mt = metaindex_handle_.EncodeTo();
		byte[] ix = index_handle_.EncodeTo();
		byte padding[] = new byte[2 * BlockHandle.kMaxEncodedLength - mt.length
				- ix.length];// Padding
		dst = util.addN(dst, mt, ix, padding,
				util.toBytes((int) (kTableMagicNumber & 0xffffffff)),
				util.toBytes((int) (kTableMagicNumber >> 32)));
		// dst.resize(2 * BlockHandle.kMaxEncodedLength);

		assert (dst.length == original_size + kEncodedLength);
		return dst;
	}

	/**
	 * I use ByteCollection as input rather than Slice, because this will make everything easier
	 * @param input
	 * @return
	 */
	public Status DecodeFrom(ByteCollection input) {
		//byte[] input_ = input.data();
		int magic_ptr = kEncodedLength - 8;
		int org_pos = input.curr_pos;
		int magic_lo = util.toInt(input.bytes, input.curr_pos + magic_ptr); // magic low
		int magic_hi = util.toInt(input.bytes, input.curr_pos + magic_ptr + 4); // magic high
		int real_lo = (int) (kTableMagicNumber & 0xffffffff);
		int real_hi = (int) (kTableMagicNumber >> 32);
		if ((magic_lo != real_lo) || (magic_hi != real_hi)) {
			return Status.InvalidArgument(new Slice(
					"not an sstable (bad magic number)"), null);
		}

		metaindex_handle_.DecodeFrom(input);
		//input.setOffset(input.getOffset() + loffset);
		index_handle_.DecodeFrom(input);
		// We skip over any leftover data (just padding for now) in "input"
		input.curr_pos = org_pos + kEncodedLength;
		return Status.OK();
	}

	// Encoded length of a Footer. Note that the serialization of a
	// Footer will always occupy exactly this many bytes. It consists
	// of two block handles and a magic number.
	public static int kEncodedLength = 2 * BlockHandle.kMaxEncodedLength + 8;

	private BlockHandle metaindex_handle_;
	private BlockHandle index_handle_;
};