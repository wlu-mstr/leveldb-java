package com.leveldb.common.table;

import com.leveldb.common.ByteCollection;
import com.leveldb.common.Comparator;
import com.leveldb.common.Iterator;
import com.leveldb.common.Slice;
import com.leveldb.common.Status;
import com.leveldb.common.file._RandomAccessFile;
import com.leveldb.common.options.CompressionType;
import com.leveldb.common.options.ReadOptions;
import com.leveldb.util.coding;
import com.leveldb.util.crc32java;
import com.leveldb.util.util;

import de.jarnbjo.jsnappy.SnappyDecompressor;

// 2012-4-5, need to test...
// 2012-4-10, reviewed
public class Block {

	private int NumRestarts() {
		assert (size_ >= 2 * util.SIZEOF_INT);
		return util.toInt(data_, size_ - util.SIZEOF_INT);
	}

	byte[] data_;
	int size_;
	int restart_offset_; // Offset in data_ of restart array
	boolean owned_; // Block owns data_[]

	// No copying allowed

	public byte[] getData_() {
		return data_;
	}

	public void setData_(byte[] data_) {
		this.data_ = data_;
	}

	public int getSize_() {
		return size_;
	}

	public void setSize_(int size_) {
		this.size_ = size_;
	}

	public int getRestart_offset_() {
		return restart_offset_;
	}

	public void setRestart_offset_(int restart_offset_) {
		this.restart_offset_ = restart_offset_;
	}

	public boolean isOwned_() {
		return owned_;
	}

	public void setOwned_(boolean owned_) {
		this.owned_ = owned_;
	}

	/**
	 * iterator over one Block ('s entries)
	 * 
	 * @author wlu
	 * 
	 */
	public class Iter extends Iterator {
		Comparator comparator_;
		byte[] data_; // underlying block contents
		int restarts_; // Offset of restart array (list of fixed32) . Seems to
						// be fixed after construction
		int num_restarts_; // Number of uint32_t entries in restart array .
							// Seems to be fixed after construction

		// current_ is offset in data_ of current entry. >= restarts_ if !Valid
		int current_; // offset of restarts
		int restart_index_; // Index of restart block in which current_ falls
		Slice key_ = new Slice();
		Slice value_ = new Slice(); // current value
		// int value_pos; // current value start position
		Status status_ = new Status();

		int Compare(Slice a, Slice b) {
			return comparator_.Compare(a, b);
		}

		// Return the offset in data_ just past the end of the current entry.
		int NextEntryOffset() {
			// size() is not always 0
			return value_.getOffset() + value_.size();
			// return (value_.data() + value_.size()) - data_;
		}

		/**
		 * get the index-th restart's block offset
		 * */
		int GetRestartPoint(int index) {
			assert (index < num_restarts_);
			return util.toInt(data_, restarts_ + index * util.SIZEOF_INT);
			// return DecodeFixed32(data_ + restarts_ + index *
			// sizeof(uint32_t));
		}

		/**
		 * Seek to index-th restart block: 1) set #restart_index# to index; 2)
		 * get the #offset# of the index-th restart block; 3) set #value_# to be
		 * a dummy Slice with offset = #offset#
		 **/
		void SeekToRestartPoint(int index) {
			key_.clear();// .clear();
			restart_index_ = index;
			// current_ will be fixed by ParseNextKey();

			// ParseNextKey() starts at the end of value_, so set value_
			// accordingly
			int offset = GetRestartPoint(index);
			value_ = new Slice(data_, offset, 0); // this is rediculas
		}

		Iter(Comparator comparator, byte[] data, int restarts, int num_restarts) {
			comparator_ = comparator;
			data_ = data;
			restarts_ = restarts;
			num_restarts_ = num_restarts;
			current_ = restarts_;
			restart_index_ = num_restarts_;
			assert (num_restarts_ > 0);
		}

		public boolean Valid() {
			return current_ < restarts_;
		}

		public Status status() {
			return status_;
		}

		public Slice key() {
			assert (Valid());
			return key_;
		}

		public Slice value() {
			assert (Valid());
			return value_;
		}

		public void Next() {
			assert (Valid());
			ParseNextKey();
		}

		public void Prev() {
			assert (Valid());

			// Scan backwards to a restart point before current_
			int original = current_;
			// 1) go prev by jumping, this is coarse scale seek !so smart!
			// may jump over the actual prev
			while (GetRestartPoint(restart_index_) >= original) {
				if (restart_index_ == 0) {
					// No more entries
					current_ = restarts_;
					restart_index_ = num_restarts_;
					return;
				}
				restart_index_--;
			}
			// 2) set value_'s offset
			SeekToRestartPoint(restart_index_);
			// 3) go next 'cause 1) might have jump over the actual pos. this is
			// fine scale.
			do {
				// Loop until end of current entry hits the start of original
				// entry
			} while (ParseNextKey() && NextEntryOffset() < original);
		}

		/**
		 * Binary search in restart array to find the first restart point with a
		 * key >= target
		 */
		public void Seek(Slice target) {
			int left = 0;
			int right = num_restarts_ - 1;
			while (left < right) {
				int mid = (left + right + 1) / 2;
				int region_offset = GetRestartPoint(mid);
				int shared, non_shared;
				threeInt l3ints = new threeInt();
				ByteCollection lbcol = new ByteCollection(data_, 0);
				boolean OK = DecodeEntry(lbcol, region_offset, restarts_,
						l3ints);
				shared = l3ints.shared;
				non_shared = l3ints.non_shared;
				if (!OK || (shared != 0)) {
					CorruptionError();
					return;
				}
				// !!!#shared# is 0 when a new restart Block starts
				Slice mid_key = new Slice(data_, lbcol.curr_pos, non_shared);
				if (Compare(mid_key, target) < 0) {
					// Key at "mid" is smaller than "target". Therefore all
					// blocks before "mid" are uninteresting.
					left = mid;
				} else {
					// Key at "mid" is >= "target". Therefore all blocks at or
					// after "mid" are uninteresting.
					right = mid - 1;
				}
			}

			// Linear search (within restart block) for first key >= target
			SeekToRestartPoint(left);
			while (true) {
				if (!ParseNextKey()) {
					return;
				}
				if (Compare(key_, target) >= 0) {
					return;
				}
			}
		}

		public void SeekToFirst() {
			SeekToRestartPoint(0);
			ParseNextKey();
		}

		public void SeekToLast() {
			SeekToRestartPoint(num_restarts_ - 1);
			while (ParseNextKey() && NextEntryOffset() < restarts_) {
				// Keep skipping
			}
		}

		private void CorruptionError() {
			current_ = restarts_;
			restart_index_ = num_restarts_;
			status_ = Status.Corruption(new Slice("bad entry in block"), null);
			key_.clear();
			value_.clear();
		}

		/**
		 * 1) set current_ as next block's start offset 2) decode current entry
		 * 3) get key_ (by combine shared and non shared) and also value_
		 * !!!#shared# is 0 when a new restart Block starts 4) update
		 * restart_index_ (so, update ahead...)
		 * 
		 * @return true -- no exception
		 */
		boolean ParseNextKey() {
			// 1. set current_
			current_ = NextEntryOffset();
			int p = current_;
			int limit = restarts_; // Restarts come right after data
			if (p >= limit) {
				// No more entries to return. Mark as invalid.
				current_ = restarts_;
				restart_index_ = num_restarts_;
				return false;
			}

			// 2. Decode next entry
			int shared, non_shared, value_length;
			threeInt l3ints = new threeInt();
			ByteCollection lbcol = new ByteCollection(data_, 0);
			// p.curr_pos is updated to nonshared data
			boolean OK = DecodeEntry(lbcol, p, limit, l3ints);
			shared = l3ints.shared;
			non_shared = l3ints.non_shared;
			value_length = l3ints.value_length;

			if (!OK || key_.size() < shared) {
				CorruptionError();
				return false;
			} else {
				// need to truncate to get shared part
				byte[] key_data = key_.data();
				// key_data = util.head(key_data, shared);
				//
				// byte[] key_add = new byte[non_shared];// I am not sure
				// whether
				// // byte concat is right
				// System.arraycopy(lbcol.bytes, lbcol.curr_pos, key_add, 0,
				// non_shared);
				//
				// key_data = util.add(key_data, key_add); // concat key_ with
				// // non-shared data
				byte[] key_data_ = new byte[shared + non_shared];
				// truncate the first #shared# bytes
				System.arraycopy(key_data, 0, key_data_, 0, shared);
				// get the #non_shared# from back end
				System.arraycopy(lbcol.bytes, lbcol.curr_pos, key_data_,
						shared, non_shared);
				key_ = new Slice(key_data_);
				// skip #non_shared# bytes and get value data
				value_ = new Slice(lbcol.bytes, lbcol.curr_pos + non_shared,
						value_length);
				// always not into the loop
				while (restart_index_ + 1 < num_restarts_
						&& GetRestartPoint(restart_index_ + 1) < current_) {
					++restart_index_;
				}
				return true;
			}
		}
	}

	/* shared|non_shared|value_len|non_shared_value|value */
	private class threeInt {
		public int shared, non_shared, value_length;
	}

	// Initialize the block with the specified contents.
	// Takes ownership of data[] and will delete[] it when done iff
	// "take_ownership is true.
	public Block(byte[] data, int size, boolean take_ownership) {
		data_ = data;
		size_ = size;
		owned_ = take_ownership;
		if (size < util.SIZEOF_INT) {
			size_ = 0; // Error marker
		} else {
			// go prev from end, skip last one int, and #value_of last int# int
			restart_offset_ = size_ - (1 + NumRestarts()) * util.SIZEOF_INT;
			if (restart_offset_ > size_ - util.SIZEOF_INT) {
				// The size is too small for NumRestarts() and therefore
				// restart_offset_ wrapped around.
				size_ = 0;
			}
		}
	}

	public void _Block(byte[] data, int size, boolean take_ownership) {
		data_ = data;
		size_ = size;
		owned_ = take_ownership;
		if (size < util.SIZEOF_INT) {
			size_ = 0; // Error marker
		} else {
			// go prev from end, skip last one int, and #value_of last int# int
			restart_offset_ = size_ - (1 + NumRestarts()) * util.SIZEOF_INT;
			if (restart_offset_ > size_ - util.SIZEOF_INT) {
				// The size is too small for NumRestarts() and therefore
				// restart_offset_ wrapped around.
				size_ = 0;
			}
		}
	}

	public int size() {
		return size_;
	}

	public Iterator NewIterator(Comparator cmp) {
		if (size_ < 2 * util.SIZEOF_INT) {
			return Iterator.NewErrorIterator(Status.Corruption(new Slice(
					"bad block contents"), null));
		}
		int num_restarts = NumRestarts();
		if (num_restarts == 0) {
			return Iterator.NewEmptyIterator();
		} else {
			return new Iter(cmp, data_, restart_offset_, num_restarts);
		}
	}

	/**
	 * Helper routine: decode the next block entry starting at "p", storing the
	 * number of shared key bytes, non_shared key bytes, and the length of the
	 * value in "*shared", "*non_shared", and "*value_length", respectively.
	 * Will not derefence past "limit".
	 * 
	 * If any errors are detected, returns NULL. Otherwise, returns a pointer to
	 * the key delta (just past the three decoded values).
	 * 
	 * In a Block Entry:
	 * {shrd,nonshrd_len<1>,val_len<2>,|nonshrd_key_data<1>,val_data<2>}
	 * 
	 * p------------------------------>p--------------------------------^limit
	 * 
	 **/
	static boolean DecodeEntry(ByteCollection p, int beg, int limit,
			threeInt io3ints) {
		if (limit - beg < 3)
			return false;
		p.curr_pos = beg;
		// wlu, 2012-7-7, bugfix: should not directly convert byte to int, need
		// to deal with byte values >= 128
		int shared = p.bytes[p.curr_pos] & 0xff;
		int non_shared = p.bytes[p.curr_pos + 1] & 0xff;
		int value_length = p.bytes[p.curr_pos + 2] & 0xff;
		if ((shared | non_shared | value_length) < 128) {
			// Fast path: all three values are encoded in one byte each
			p.curr_pos += 3;
		} else {
			shared = coding.GetVarint32(p);
			if (p.curr_pos > limit)
				return false;
			non_shared = coding.GetVarint32(p);
			if (p.curr_pos > limit)
				return false;
			value_length = coding.GetVarint32(p);
			if (p.curr_pos > limit)
				return false;
		}

		// refer to comment for the data structure
		if ((limit - p.curr_pos) < (non_shared + value_length)) {
			return false;
		}
		io3ints.shared = shared;
		io3ints.non_shared = non_shared;
		io3ints.value_length = value_length;
		return true;
	}

	/**
	 * 
	 * @param file
	 *            the file containing the Block
	 * @param options
	 * @param handle
	 *            {offset, size} of the Block
	 * @param block
	 *            be set as output, (read to this block)
	 * @return
	 * @throws Exception
	 *             read the Block with Footer(5 bytes)
	 */
	public static boolean ReadBlock(_RandomAccessFile file,
			ReadOptions options, BlockHandle handle, Block block)
			throws Exception {
		boolean may_cache = false;
		// *block = NULL;

		// Read the block contents as well as the type/crc footer.
		// See table_builder.java for the code that built this structure.
		int n = (int) handle.size();
		byte[] buf = new byte[n + Footer.kBlockTrailerSize];
		Slice iocontents = new Slice();
		buf = file.Read(handle.offset(), n + Footer.kBlockTrailerSize,
				iocontents); // set contents

		if (iocontents.size() != n + Footer.kBlockTrailerSize) {

			throw new Exception("truncated block read");
		}

		// Check the crc of the type and the block contents
		byte[] data = iocontents.data(); // Pointer to where Read put the data
		if (options.verify_checksums) {
			// get the value @ ofs n+1
			long crc = crc32java.Unmask(util.toInt(data, n + 1));
			crc32java crc32 = new crc32java();
			// calculate the value from 0 to n
			long actual = crc32.Value(data, n + 1);
			if (actual != crc) {
				throw new Exception("block checksum mismatch");
			}
		}

		/* compression or not */
		switch (data[n]) {
		case CompressionType.kNoCompression:
			if (data != buf) {
				// File implementation gave us pointer to some other data.
				// Use it directly under the assumption that it will be live
				// while the file is open.
				buf = null;
				block._Block(data, n, false);
				// block.setData_(data);
				// block.setSize_(n);
				// block.setOwned_(false);
				// block = new Block(data, n, false /* do not take ownership
				// */);
				may_cache = false; // Do not double-cache
			} else {
				block._Block(buf, n, true);
				// block.setData_(buf);
				// block.setSize_(n);
				// block.setOwned_(true);
				// block = new Block(buf, n, true /* take ownership */);
				may_cache = true;
			}

			// Ok
			break;
		// case CompressionType.kSnappyCompression:
		// int ulength = coding.GetVarint32(data, n); // get the vint32 as
		// // length
		//
		// // if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
		// // buf = null;
		// // throw new Exception("corrupted compressed block contents");
		// // }
		// byte[] ubuf = new byte[ulength];
		// ubuf = SnappyDecompressor.decompress(data, 0, n).toByteArray();
		// // if (!port::Snappy_Uncompress(data, n, ubuf)) {
		// // buf = null;
		// // dubuf = null;
		// // throw new Exception("corrupted compressed block contents");
		// // }
		// buf = null;
		// block._Block(ubuf, ulength, true);
		// // block.setData_(ubuf);
		// // block.setSize_(ulength);
		// // block.setOwned_(true);
		// // *block = new Block(ubuf, ulength, true /* take ownership */);
		// may_cache = true;
		// break;

		default:
			buf = null;
			throw new Exception("bad block type");
		}

		return may_cache;
	}
}
