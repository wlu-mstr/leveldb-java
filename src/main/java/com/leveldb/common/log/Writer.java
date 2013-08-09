package com.leveldb.common.log;

import com.leveldb.common.Slice;
import com.leveldb.common.Status;
import com.leveldb.common.file._WritableFile;
import com.leveldb.util.crc32java;
import com.leveldb.util.util;

public class Writer {
	crc32java crc = new crc32java();
	// Create a writer that will append data to "*dest".
	// "*dest" must be initially empty.
	// "*dest" must remain live while this Writer is in use.
	public Writer(_WritableFile dest) {
		dest_ = dest;
		block_offset_ = 0;
		for (int i = 0; i <= logformat.kMaxRecordType; i++) {
			byte[] b = new byte[] { (byte) i };
			type_crc_[i] = crc.Value(b, 1);
		}
	}

	public Status AddRecord(Slice slice) {
		byte[] data = slice.data();
		int ptr = 0;
		int left = slice.size();

		// Fragment the record if necessary and emit it. Note that if slice
		// is empty, we still want to iterate once to emit a single
		// zero-length record
		Status s;
		boolean begin = true;
		do {
			int leftover = logformat.kBlockSize - block_offset_;
			assert (leftover >= 0);
			if (leftover < logformat.kHeaderSize) {
				// Switch to a new block
				if (leftover > 0) {
					// Fill the trailer with 0s(literal below relies on
					// kHeaderSize being 7)
					//assert (logformat.kHeaderSize == 7);
					dest_.Append(new Slice(new byte[] { 0, 0, 0, 0, 0, 0 },
							leftover));
				}
				block_offset_ = 0;
			}

			// Invariant: we never leave < kHeaderSize bytes in a block.
			assert (logformat.kBlockSize - block_offset_
					- logformat.kHeaderSize >= 0);

			int avail = logformat.kBlockSize - block_offset_
					- logformat.kHeaderSize;
			int fragment_length = (left < avail) ? left : avail;

			int type;
			boolean end = (left == fragment_length);
			if (begin && end) {
				type = RecordType.kFullType;
			} else if (begin) {
				type = RecordType.kFirstType;
			} else if (end) {
				type = RecordType.kLastType;
			} else {
				type = RecordType.kMiddleType;
			}

			// TODO: think about it: do not copy the bytes, add offset here
			byte[] tmp = util.subN(data, ptr, fragment_length);
			s = EmitPhysicalRecord(new RecordType(type), tmp, fragment_length);
			
			ptr += fragment_length;
			left -= fragment_length;
			begin = false;
		} while (s.ok() && left > 0);
		return s;
	}

	private _WritableFile dest_;
	private int block_offset_; // Current offset in block

	// crc32c values for all supported record types. These are
	// pre-computed to reduce the overhead of computing the crc of the
	// record type stored in the header.
	int[] type_crc_ = new int[logformat.kMaxRecordType + 1];

	/** header: crc(0..3)|n(4..5)|rectype(6)
	 * <p>
	 * write data to file dest_
	 * @param t
	 * @param ptr
	 * @param n
	 * @return
	 */
	Status EmitPhysicalRecord(RecordType t, byte[] ptr, int n) {
		assert (n <= 0x7fff); // Must fit in two bytes
		assert (block_offset_ + logformat.kHeaderSize + n <= logformat.kBlockSize);

		// Format the header
		byte[] buf = new byte[logformat.kHeaderSize];
		buf[4] = (byte) (n & 0xff);
		buf[5] = (byte) (n >> 8);
		buf[6] = (byte) (t.val);

		// Compute the crc of the record type and the payload.
		int crc_ = crc.Extend(type_crc_[t.val], ptr, n);
		crc_ = crc32java.Mask(crc_); // Adjust for storage
		util.putInt(buf, 0, crc_);

		// Write the header and the payload
		Status s = dest_.Append(new Slice(buf, logformat.kHeaderSize));
		if (s.ok()) {
			s = dest_.Append(new Slice(ptr, n));
			if (s.ok()) {
				s = dest_.Flush();
			}
		}
		block_offset_ += logformat.kHeaderSize + n;
		return s;
	}

	// No copying allowed
}
