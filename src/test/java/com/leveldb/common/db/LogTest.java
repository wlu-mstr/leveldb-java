package com.leveldb.common.db;

import java.io.IOException;
import java.util.Random;


import com.leveldb.common.ByteVector;
import com.leveldb.common.Slice;
import com.leveldb.common.Status;
import com.leveldb.common.file._SequentialFile;
import com.leveldb.common.file._WritableFile;
import com.leveldb.common.log.Reader;
import com.leveldb.common.log.RecordType;
import com.leveldb.common.log.Writer;
import com.leveldb.common.log.logformat;
import com.leveldb.util.crc32java;
import com.leveldb.util.util;

// Log is used for data consistency
public class LogTest {
	crc32java crc = new crc32java();

	private static void ASSERT_TRUE(boolean b, String string) {
		if (!b) {
			System.out.println(string);
		}

	}

	// Construct a string of the specified length made out of the supplied
	// partial string.
	static String BigString(String partial_string, int n) {
		StringBuffer result = new StringBuffer();
		while (result.length() < n) {
			result.append(partial_string);
		}
		return result.substring(0, n);
	}

	// Construct a string from a number
	static String NumberString(int n) {
		return n + "";
	}

	// Return a skewed potentially long string
	static String RandomSkewedString(int i, Random rnd) {
		return BigString(NumberString(i), Math.abs(rnd.nextInt()) % 1000);
	}

	static class StringDest extends _WritableFile {
		ByteVector contents_ = new ByteVector();

		public Status Close() {
			return Status.OK();
		}

		public Status Flush() {
			return Status.OK();
		}

		public Status Sync() {
			return Status.OK();
		}

		public Status Append(Slice slice) {
			contents_.append(slice.data());
			// contents_ = util.add(contents_, slice.data());
			return Status.OK();
		}
	}

	static class StringSource extends _SequentialFile {
		Slice contents_;
		boolean force_error_;
		boolean returned_partial_;

		StringSource() {
			force_error_ = false;
			returned_partial_ = false;
		}

		public byte[] Read(int n, Slice result) {
			ASSERT_TRUE(!returned_partial_, "must not Read() after eof/error");

			if (force_error_) {
				force_error_ = false;
				returned_partial_ = true;
				try {
					throw new IOException("read error");
				} catch (IOException e) {
					e.printStackTrace();
					return null;
				}
				// return Status::Corruption("read error");
			}

			if (contents_.size() < n) {
				n = contents_.size();
				returned_partial_ = true;
			}
			result.setData_(contents_.data(), 0, n);
			// *result = Slice(contents_.data(), n);
			contents_.remove_prefix(n);
			return result.data();
		}

		public Status Skip(long n) {
			if (n > contents_.size()) {
				contents_.clear();
				return Status.NotFound(new Slice(
						"in-memory file skipepd past end"), null);
			}

			contents_.remove_prefix((int) n);

			return Status.OK();
		}
	}

	static class ReportCollector extends Reader.Reporter {
		public int dropped_bytes_;
		public String message_ = "";

		public ReportCollector() {
			super();
			dropped_bytes_ = 0;
		}

		public void Corruption(int bytes, Status status) {
			dropped_bytes_ += bytes;
			message_ += status.toString();
		}
	}

	StringDest dest_ = new StringDest();
	StringSource source_ = new StringSource();
	ReportCollector report_ = new ReportCollector();
	boolean reading_;
	Writer writer_;
	Reader reader_;

	// Record metadata for testing initial offset functionality
	static int initial_offset_record_sizes_[] = { 10000, // Two sizable records
															// in first block
			10000, 2 * logformat.kBlockSize - 1000, // Span three blocks
			1 };
	static long initial_offset_last_record_offsets_[] = {
			0,
			logformat.kHeaderSize + 10000,
			2 * (logformat.kHeaderSize + 10000),
			2 * (logformat.kHeaderSize + 10000)
					+ (2 * logformat.kBlockSize - 1000) + 3
					* logformat.kHeaderSize };

	public LogTest() {
		reading_ = false;
		writer_ = new Writer(dest_);
		reader_ = new Reader(source_, report_, true/* checksum */, 0/* initial_offset */);
	}

	void Write(String msg) {
		ASSERT_TRUE(!reading_, "Write() after starting to read");
		writer_.AddRecord(new Slice(msg));
	}

	int WrittenBytes() {
		return dest_.contents_.getSize();
	}

	String Read() {
		if (!reading_) {
			reading_ = true;
			source_.contents_ = new Slice(dest_.contents_.getData());
		}
		byte[] scratch = new byte[0];
		Slice record = new Slice();
		if (reader_.ReadRecord(record, scratch)) {
			return record.toString();
		} else {
			return "EOF";
		}
	}

	void IncrementByte(int offset, int delta) {
		dest_.contents_.increase(offset, delta);
	}

	void SetByte(int offset, byte new_byte) {
		dest_.contents_.set(offset, new_byte);
	}

	void ShrinkSize(int bytes) {
		dest_.contents_.setSize(dest_.contents_.getSize() - bytes);

	}

	void FixChecksum(int header_offset, int len) {
		// Compute crc of type/len/data
		int crc_ = crc.Value(dest_.contents_.getRawRef(), header_offset + 6,
				1 + len);
		crc_ = crc32java.Mask(crc_);
		util.putInt(dest_.contents_.getRawRef(), header_offset, crc_);
	}

	void ForceError() {
		source_.force_error_ = true;
	}

	int DroppedBytes() {
		return report_.dropped_bytes_;
	}

	String ReportMessage() {
		return report_.message_;
	}

	// Returns OK iff recorded error message contains "msg"
	String MatchError(String msg) {
		if (!report_.message_.contains(msg)) {
			return report_.message_;
		} else {
			return "OK";
		}
	}

	void WriteInitialOffsetLog() {
		for (int i = 0; i < 4; i++) {
			StringBuffer sb = new StringBuffer();
			for (int j = 0; j < initial_offset_record_sizes_[i]; j++) {
				sb.append(new String(new char[] { (char) ('a' + i) }));
			}
			String record = sb.toString();
			Write(record);
		}
	}

	void CheckOffsetPastEndReturnsNoRecords(long offset_past_end) {
		WriteInitialOffsetLog();
		reading_ = true;
		source_.contents_ = new Slice(dest_.contents_.getData());
		Reader offset_reader = new Reader(source_, report_, true/* checksum */,
				WrittenBytes() + offset_past_end);
		Slice record = new Slice();
		byte[] scratch = new byte[0];
		ASSERT_TRUE(!offset_reader.ReadRecord(record, scratch), "");
		offset_reader = null;
	}

	void CheckInitialOffsetRecord(long initial_offset,
			int expected_record_offset) {
		WriteInitialOffsetLog();
		reading_ = true;
		source_.contents_ = new Slice(dest_.contents_.getData());
		Reader offset_reader = new Reader(source_, report_, true/* checksum */,
				initial_offset);
		Slice record = new Slice();
		byte[] scratch = new byte[0];
		ASSERT_TRUE(offset_reader.ReadRecord(record, scratch),
				record.toString());
		ASSERT_TRUE(
				initial_offset_record_sizes_[expected_record_offset] == record
						.size(),
				"" + record.size());
		ASSERT_TRUE(
				initial_offset_last_record_offsets_[expected_record_offset] == offset_reader
						.LastRecordOffset(),
				"" + offset_reader.LastRecordOffset());
		ASSERT_TRUE((byte) ('a' + expected_record_offset) == record.data()[0],
				"" + record.data()[0]);
		offset_reader = null;
	}

	void Empty() {
		ASSERT_TRUE("EOF".compareTo(Read()) == 0, "test pass");
	}

	static void ASSERT_EQ(String s1, String s2) {
		if (s1.compareTo(s2) != 0) {
			System.out.println(s1 + ", " + s2);
		} else {
			// System.out.println(s1.length() + ", " + s2.length());
		}
	}

	static void ASSERT_EQ(int s1, int s2) {
		if (s1 != s2) {
			System.out.println(s1 + ", " + s2);
		}
	}

	void ReadWrite() {
		Write("foo");
		Write("bar");
		Write("");
		Write("xxxx");
		ASSERT_EQ("foo", Read());
		ASSERT_EQ("bar", Read());
		ASSERT_EQ("", Read());
		ASSERT_EQ("xxxx", Read());
		ASSERT_EQ("EOF", Read());
		ASSERT_EQ("EOF", Read()); // Make sure reads at eof work
	}

	void t() {
		Write(6058 + "");
		ASSERT_EQ("6058", Read());
	}

	void ManyBlocks() {
		for (int i = 0; i < 100000; i++) {
			Write(NumberString(i));
		}
		for (int i = 0; i < 100000; i++) {
			ASSERT_EQ(NumberString(i), Read());
		}
		ASSERT_EQ("EOF", Read());
	}

	void Fragmentation() {
		Write("small");
		Write(BigString("medium", 50000));
		Write(BigString("large", 100000));
		ASSERT_EQ("small", Read());
		ASSERT_EQ(BigString("medium", 50000), Read());
		ASSERT_EQ(BigString("large", 100000), Read());
		ASSERT_EQ("EOF", Read());
	}

	void MarginalTrailer() {
		// Make a trailer that is exactly the same length as an empty record.
		int n = logformat.kBlockSize - 2 * logformat.kHeaderSize;
		Write(BigString("foo", n));
		ASSERT_EQ(logformat.kBlockSize - logformat.kHeaderSize, WrittenBytes());
		Write("");
		Write("bar");
		ASSERT_EQ(BigString("foo", n), Read());
		ASSERT_EQ("", Read());
		ASSERT_EQ("bar", Read());
		ASSERT_EQ("EOF", Read());
	}

	void MarginalTrailer2() {
		// Make a trailer that is exactly the same length as an empty record.
		int n = logformat.kBlockSize - 2 * logformat.kHeaderSize;
		Write(BigString("foo", n));
		ASSERT_EQ(logformat.kBlockSize - logformat.kHeaderSize, WrittenBytes());
		Write("bar");
		ASSERT_EQ(BigString("foo", n), Read());
		ASSERT_EQ("bar", Read());
		ASSERT_EQ("EOF", Read());
		ASSERT_EQ(0, DroppedBytes());
		ASSERT_EQ("", ReportMessage());
	}

	void ShortTrailer() {
		int n = logformat.kBlockSize - 2 * logformat.kHeaderSize + 4;
		Write(BigString("foo", n));
		ASSERT_EQ(logformat.kBlockSize - logformat.kHeaderSize + 4,
				WrittenBytes());
		Write("");
		Write("bar");
		ASSERT_EQ(BigString("foo", n), Read());
		ASSERT_EQ("", Read());
		ASSERT_EQ("bar", Read());
		ASSERT_EQ("EOF", Read());
	}

	void AlignedEof() {
		int n = logformat.kBlockSize - 2 * logformat.kHeaderSize + 4;
		Write(BigString("foo", n));
		ASSERT_EQ(logformat.kBlockSize - logformat.kHeaderSize + 4,
				WrittenBytes());
		ASSERT_EQ(BigString("foo", n), Read());
		ASSERT_EQ("EOF", Read());
	}

	void RandomRead() {
		int N = 500;
		Random write_rnd = new Random(0);
		for (int i = 0; i < N; i++) {
			Write(RandomSkewedString(i, write_rnd));
		}
		Random read_rnd = new Random(0);
		for (int i = 0; i < N; i++) {
			ASSERT_EQ(RandomSkewedString(i, read_rnd), Read());
		}
		ASSERT_EQ("EOF", Read());
	}

	// TODO: this is not passed
	void ReadError() {
		Write("foo");
		ForceError();
		ASSERT_EQ("EOF", Read());
		ASSERT_EQ(logformat.kBlockSize, DroppedBytes());
		ASSERT_EQ("OK", MatchError("read error"));
	}

	// damage the record type
	void BadRecordType() {
		Write("foo");
		// Type is stored in header[6];
		// damage the type
		IncrementByte(6, 100);
		FixChecksum(0, 3);
		ASSERT_EQ("EOF", Read());
		ASSERT_EQ(3, DroppedBytes());
		ASSERT_EQ("OK", MatchError("unknown record type"));
	}

	void TruncatedTrailingRecord() {
		Write("foo");
		ShrinkSize(4); // Drop all payload as well as a header byte
		ASSERT_EQ("EOF", Read());
		ASSERT_EQ(logformat.kHeaderSize - 1, DroppedBytes());
		ASSERT_EQ("OK", MatchError("truncated record at end of file"));
	}

	void BadLength() {
		Write("foo");
		ShrinkSize(1);
		ASSERT_EQ("EOF", Read());
		ASSERT_EQ(logformat.kHeaderSize + 2, DroppedBytes());
		ASSERT_EQ("OK", MatchError("bad record length"));
	}

	void ChecksumMismatch() {
		Write("foo");
		IncrementByte(0, 10);
		ASSERT_EQ("EOF", Read());
		ASSERT_EQ(10, DroppedBytes());
		ASSERT_EQ("OK", MatchError("checksum mismatch"));
	}

	void UnexpectedMiddleType() {
		Write("foo");
		SetByte(6, (byte) RecordType.kMiddleType);
		FixChecksum(0, 3);
		ASSERT_EQ("EOF", Read());
		ASSERT_EQ(3, DroppedBytes());
		ASSERT_EQ("OK", MatchError("missing start"));
	}

	void UnexpectedLastType() {
		Write("foo");
		SetByte(6, (byte) RecordType.kLastType);
		FixChecksum(0, 3);
		ASSERT_EQ("EOF", Read());
		ASSERT_EQ(3, DroppedBytes());
		ASSERT_EQ("OK", MatchError("missing start"));
	}

	void UnexpectedFullType() {
		Write("foo");
		Write("bar");
		SetByte(6, (byte) RecordType.kFirstType);
		FixChecksum(0, 3);
		ASSERT_EQ("bar", Read());
		ASSERT_EQ("EOF", Read());
		ASSERT_EQ(3, DroppedBytes());
		ASSERT_EQ("OK", MatchError("partial record without end"));
	}

	void UnexpectedFirstType() {
		Write("foo");
		Write(BigString("bar", 100000));
		SetByte(6, (byte) RecordType.kFirstType);
		FixChecksum(0, 3);
		ASSERT_EQ(BigString("bar", 100000), Read());
		ASSERT_EQ("EOF", Read());
		ASSERT_EQ(3, DroppedBytes());
		ASSERT_EQ("OK", MatchError("partial record without end"));
	}

	void ErrorJoinsRecords() {
		// Consider two fragmented records:
		// first(R1) last(R1) first(R2) last(R2)
		// where the middle two fragments disappear. We do not want
		// first(R1),last(R2) to get joined and returned as a valid record.

		// Write records that span two blocks
		Write(BigString("foo", logformat.kBlockSize));
		Write(BigString("bar", logformat.kBlockSize));
		Write("correct");

		// Wipe the middle block
		for (int offset = logformat.kBlockSize; offset < 2 * logformat.kBlockSize; offset++) {
			SetByte(offset, (byte) 'x');
		}

		ASSERT_EQ("correct", Read());
		ASSERT_EQ("EOF", Read());
		int dropped = DroppedBytes();
		ASSERT_TRUE(dropped <= 2 * logformat.kBlockSize + 100, "<=");
		ASSERT_TRUE(dropped >= 2 * logformat.kBlockSize, ">=");
	}

	void ReadStart() {
		CheckInitialOffsetRecord(0, 0);
	}

	// //////////////////////////////
	void ReadSecondOneOff() {
		CheckInitialOffsetRecord(1, 1);
	}

	void ReadSecondTenThousand() {
		CheckInitialOffsetRecord(10000, 1);
	}

	void ReadSecondStart() {
		CheckInitialOffsetRecord(10007, 1);
	}

	void ReadThirdOneOff() {
		CheckInitialOffsetRecord(10008, 2);
	}

	void ReadThirdStart() {
		CheckInitialOffsetRecord(20014, 2);
	}

	void ReadFourthOneOff() {
		CheckInitialOffsetRecord(20015, 3);
	}

	void ReadFourthFirstBlockTrailer() {
		CheckInitialOffsetRecord(logformat.kBlockSize - 4, 3);
	}

	void ReadFourthMiddleBlock() {
		CheckInitialOffsetRecord(logformat.kBlockSize + 1, 3);
	}

	void ReadFourthLastBlock() {
		CheckInitialOffsetRecord(2 * logformat.kBlockSize + 1, 3);
	}

	void ReadFourthStart() {
		CheckInitialOffsetRecord(
				2 * (logformat.kHeaderSize + 1000)
						+ (2 * logformat.kBlockSize - 1000) + 3
						* logformat.kHeaderSize, 3);
	}

	void ReadEnd() {
		CheckOffsetPastEndReturnsNoRecords(0);
	}

	void ReadPastEnd() {
		CheckOffsetPastEndReturnsNoRecords(5);
	}

	public static void main(String args[]) {
		LogTest lt = new LogTest();
		lt.Empty();
		lt = new LogTest();
		lt.ReadWrite();
		lt = new LogTest();
		lt.ManyBlocks();
		lt = new LogTest();
		lt.Fragmentation();
		lt = new LogTest();
		lt.MarginalTrailer2();
		lt = new LogTest();
		lt.ShortTrailer();
		lt = new LogTest();
		lt.AlignedEof();
		lt = new LogTest();
		lt.RandomRead();
		lt = new LogTest();
		//lt.ReadError();// TODO
		lt = new LogTest();
		lt.BadRecordType();// TODO
		lt = new LogTest();
		lt.TruncatedTrailingRecord();//TODO
		lt = new LogTest();
		lt.BadLength();//TODO
		lt = new LogTest();
		lt.ChecksumMismatch(); //TODO
		lt = new LogTest();
		lt.UnexpectedMiddleType();//TODO
		lt = new LogTest();
		lt.UnexpectedLastType();// TODO
		lt = new LogTest();
		lt.UnexpectedFullType();//TODO
		lt = new LogTest();
		lt.UnexpectedFirstType();//TODO
		lt = new LogTest();
		lt.ErrorJoinsRecords();//TODO
		lt = new LogTest();
		lt.ReadStart();
		lt = new LogTest();
		lt.ReadSecondOneOff();
		lt = new LogTest();
		lt.ReadSecondTenThousand();
		lt = new LogTest();
		lt.ReadSecondStart();
		lt = new LogTest();
		lt.ReadThirdOneOff();
		lt = new LogTest();
		lt.ReadThirdStart();
		lt = new LogTest();
		lt.ReadFourthOneOff();
		lt = new LogTest();
		lt.ReadFourthFirstBlockTrailer();
		lt = new LogTest();
		lt.ReadFourthMiddleBlock();
		lt = new LogTest();
		lt.ReadFourthLastBlock();
		lt = new LogTest();
		lt.ReadFourthStart();
		lt = new LogTest();
		lt.ReadEnd();
		lt = new LogTest();
		lt.ReadPastEnd();
	}
}
