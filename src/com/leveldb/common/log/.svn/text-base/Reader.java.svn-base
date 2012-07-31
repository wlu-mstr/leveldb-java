package com.leveldb.common.log;

import com.leveldb.common.Slice;
import com.leveldb.common.Status;
import com.leveldb.common.file._SequentialFile;
import com.leveldb.util.crc32java;
import com.leveldb.util.util;

public class Reader {
	crc32java crc = new crc32java();

	// Interface for reporting errors.
	public abstract static class Reporter {
		public Reporter() {
		}

		// Some corruption was detected. "size" is the approximate number
		// of bytes dropped due to the corruption.
		public abstract void Corruption(int bytes, Status status);
	}

	// Create a reader that will return log records from "*file".
	// "*file" must remain live while this Reader is in use.
	//
	// If "reporter" is non-NULL, it is notified whenever some data is
	// dropped due to a detected corruption. "*reporter" must remain
	// live while this Reader is in use.
	//
	// If "checksum" is true, verify checksums if available.
	//
	// The Reader will start reading at the first record located at physical
	// position >= initial_offset within the file.
	public Reader(_SequentialFile file, Reporter reporter, boolean checksum,
			long initial_offset) {
		file_ = file;
		reporter_ = reporter;
		checksum_ = checksum;
		backing_store_ = new byte[logformat.kBlockSize];
		buffer_ = new Slice();
		eof_ = false;
		last_record_offset_ = 0;
		end_of_buffer_offset_ = 0;
		initial_offset_ = initial_offset;
	}

	// ~Reader();

	// Read the next record into record. Returns true if read
	// successfully, false if we hit end of the input. May use
	// "scratch" as temporary storage. The contents filled in record
	// will only be valid until the next mutating operation on this
	// reader or the next mutation to scratch.
	// </br>
	// wlu, 2012-7-8, bugfx: scrach should not be String, actually std::string
	// of CPP should be equal to
	// Java's byte[].
	public boolean ReadRecord(Slice record, byte[] scratch) {
		if (last_record_offset_ < initial_offset_) {
			if (!SkipToInitialBlock()) {
				return false;
			}
		}

		scratch = new byte[0];
		record.clear();
		boolean in_fragmented_record = false;
		// Record offset of the logical record that we're reading
		// 0 is a dummy value to make compilers happy
		long prospective_record_offset = 0;

		Slice fragment = new Slice();
		while (true) {
			long physical_record_offset = end_of_buffer_offset_
					- buffer_.size();
			int record_type = ReadPhysicalRecord(fragment);
			switch (record_type) {
			// full fragment
			case RecordType.kFullType:
				if (in_fragmented_record) {
					// Handle bug in earlier versions of log::Writer where
					// it could emit an empty kFirstType record at the tail end
					// of a block followed by a kFullType or kFirstType record
					// at the beginning of the next block.
					if (scratch.length == 0) {
						in_fragmented_record = false;
					} else {
						ReportCorruption(scratch.length,
								"partial record without end(1)");
					}
				}
				prospective_record_offset = physical_record_offset;
				scratch = new byte[0];
				record.setData_(fragment.data());
				last_record_offset_ = prospective_record_offset;
				return true;

				// first part
			case RecordType.kFirstType:
				if (in_fragmented_record) {
					// Handle bug in earlier versions of log::Writer where
					// it could emit an empty kFirstType record at the tail end
					// of a block followed by a kFullType or kFirstType record
					// at the beginning of the next block.
					if (scratch.length == 0) {
						in_fragmented_record = false;
					} else {
						ReportCorruption(scratch.length,
								"partial record without end(2)");
					}
				}
				prospective_record_offset = physical_record_offset;
				scratch = fragment.data();
				in_fragmented_record = true;
				break;

			// middle part
			case RecordType.kMiddleType:
				if (!in_fragmented_record) {
					ReportCorruption(fragment.size(),
							"missing start of fragmented record(1)");
				} else {
					scratch = util.add(scratch, fragment.data());
					// scratch += util.toString(fragment.data());
				}
				break;

			// the last part
			case RecordType.kLastType:
				if (!in_fragmented_record) {
					ReportCorruption(fragment.size(),
							"missing start of fragmented record(2)");
				} else {
					scratch = util.add(scratch, fragment.data());
					// scratch += util.toString(fragment.data());
					record.setData_(scratch);
					last_record_offset_ = prospective_record_offset;
					return true;
				}
				break;

			case kEof:
				if (in_fragmented_record) {
					ReportCorruption(scratch.length,
							"partial record without end(3)");
					scratch = new byte[0];
				}
				return false;

			case kBadRecord:
				if (in_fragmented_record) {
					ReportCorruption(scratch.length,
							"error in middle of record");
					in_fragmented_record = false;
					scratch = new byte[0];
				}
				break;

			default: {
				String buf = "unknown record type " + record_type;
				ReportCorruption(
						(fragment.size() + (in_fragmented_record ? scratch.length
								: 0)), buf);
				in_fragmented_record = false;
				scratch = new byte[0];
				break;
			}
			}
		}
	}

	// Returns the physical offset of the last record returned by ReadRecord.
	//
	// Undefined before the first call to ReadRecord.
	public long LastRecordOffset() {
		return last_record_offset_;
	}

	private _SequentialFile file_;
	Reporter reporter_;
	boolean checksum_;
	byte[] backing_store_;
	Slice buffer_;
	boolean eof_; // Last Read() indicated EOF by returning < kBlockSize

	// Offset of the last record returned by ReadRecord.
	long last_record_offset_;
	// Offset of the first location past the end of buffer_.
	long end_of_buffer_offset_;

	// Offset at which to start looking for the first record to return
	long initial_offset_;

	// Extend record types with the following special values
	static final int kEof = logformat.kMaxRecordType + 1,
	// Returned whenever we find an invalid physical record.
	// Currently there are three situations in which this happens:
	// * The record has an invalid CRC (ReadPhysicalRecord reports a drop)
	// * The record is a 0-length record (No drop is reported)
	// * The record is below constructor's initial_offset (No drop is reported)
			kBadRecord = logformat.kMaxRecordType + 2;

	/**
	 * Skips all blocks that are completely before "initial_offset_". Returns
	 * true on success. Handles reporting.
	 * <p>
	 * There might be byts before initial_offset_ in the block it locates.
	 * </p>
	 * |B0|B1|...|Bi|... in Bi, {b0,b1,...,bk,...} where bk in the byte at
	 * initial_offset, then B0~Bi-1 are skiped, file is skipped to b0@Bi
	 */
	boolean SkipToInitialBlock() {
		// offset in the block that initial_offset_ locates in
		int offset_in_block = (int) (initial_offset_ % logformat.kBlockSize);
		// offset of the 1st byte of the block that initial_offset_ locates in
		long block_start_location = initial_offset_ - offset_in_block;

		// Don't search a block if we'd be in the trailer of the block
		// move to the next block
		if (offset_in_block > logformat.kBlockSize - 6) {
			offset_in_block = 0;
			block_start_location += logformat.kBlockSize;
		}

		end_of_buffer_offset_ = block_start_location;

		// Skip the file to start of first block that can contain the initial
		// record
		if (block_start_location > 0) {
			Status skip_status = file_.Skip(block_start_location);
			if (!skip_status.ok()) {
				ReportDrop((int) block_start_location, skip_status);
				return false;
			}
		}

		return true;
	}

	/*
	 * Return type, or one of the preceding special values
	 */
	int ReadPhysicalRecord(Slice result) {
		while (true) {
			if (buffer_.size() < logformat.kHeaderSize) {
				if (!eof_) {
					// Last read was a full read, so this is a trailer to skip
					buffer_.clear();
					// read the whole block
					backing_store_ = file_.Read(logformat.kBlockSize, buffer_);
					Status status = new Status();
					end_of_buffer_offset_ += buffer_.size();
					if (backing_store_ == null) {
						buffer_.clear();
						ReportDrop(logformat.kBlockSize, status);
						eof_ = true;
						return kEof;
					} else if (buffer_.size() < logformat.kBlockSize) {
						eof_ = true;
					}
					continue;
				} else if (buffer_.size() == 0) {
					// End of file
					return kEof;
				} else {
					int drop_size = buffer_.size();
					buffer_.clear();
					ReportCorruption(drop_size,
							"truncated record at end of file");
					return kEof;
				}
			}

			// Parse the header
			byte[] header = buffer_.data();
			int a = (int) (header[4]) & 0xff;
			int b = (int) (header[5]) & 0xff;
			int type = header[6];
			int length = a | (b << 8);
			if (logformat.kHeaderSize + length > buffer_.size()) {
				int drop_size = buffer_.size();
				buffer_.clear();
				ReportCorruption(drop_size, "bad record length");
				return kBadRecord;
			}

			if (type == RecordType.kZeroType && length == 0) {
				// Skip zero length record without reporting any drops since
				// such records are produced by the mmap based writing code in
				// env_posix.cc that preallocates file regions.
				buffer_.clear();
				return kBadRecord;
			}

			// Check crc
			if (checksum_) {
				int expected_crc = crc32java.Unmask(util.toInt(header));
				int actual_crc = crc.Value(header, 6, 1 + length);
				if (actual_crc != expected_crc) {
					// Drop the rest of the buffer since "length" itself may
					// have
					// been corrupted and if we trust it, we could find some
					// fragment of a real log record that just happens to look
					// like a valid log record.
					int drop_size = buffer_.size();
					buffer_.clear();
					ReportCorruption(drop_size, "checksum mismatch");
					return kBadRecord;
				}
			}

			buffer_.remove_prefix(logformat.kHeaderSize + length);

			// Skip physical record that started before initial_offset_
			if (end_of_buffer_offset_ - buffer_.size() - logformat.kHeaderSize
					- length < initial_offset_) {
				result.clear();
				return kBadRecord;
			}

			result.setData_(header, logformat.kHeaderSize, length);
			return type;
		}

	}

	// Reports dropped bytes to the reporter.
	// buffer_ must be updated to remove the dropped bytes prior to invocation.
	void ReportCorruption(int bytes, String reason) {
		ReportDrop(bytes, Status.Corruption(new Slice(reason), null));
	}

	void ReportDrop(int bytes, Status reason) {
		if (reporter_ != null
				&& end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
			reporter_.Corruption(bytes, reason);
		}
	}

	// No copying allowed
}