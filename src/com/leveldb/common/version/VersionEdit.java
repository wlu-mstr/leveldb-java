package com.leveldb.common.version;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.leveldb.common.ByteCollection;
import com.leveldb.common.Slice;
import com.leveldb.common.Status;
import com.leveldb.common.config;
import com.leveldb.common.db.FileMetaData;
import com.leveldb.common.db.InternalKey;
import com.leveldb.util.Pair;
import com.leveldb.util.SequenceNumber;
import com.leveldb.util.Tag;
import com.leveldb.util.ValueType;
import com.leveldb.util.coding;
import com.leveldb.util.logging;
import com.leveldb.util.util;


/**
 * 2012-4-12: add test for this class
 * @author wlu
 *
 */
public class VersionEdit {
	public VersionEdit() {
		Clear();
	}

	public void Clear() {
		comparator_ = "";
		log_number_ = 0;
		prev_log_number_ = 0;
		last_sequence_ = new SequenceNumber(0);
		next_file_number_ = 0;
		has_comparator_ = false;
		has_log_number_ = false;
		has_prev_log_number_ = false;
		has_next_file_number_ = false;
		has_last_sequence_ = false;
		deleted_files_ = new HashSet<Pair<Integer, Long>>();
		new_files_ = new ArrayList<Pair<Integer, FileMetaData>>();
		compact_pointers_ = new ArrayList<Pair<Integer,InternalKey>>();
	};

	public void SetComparatorName(Slice name) {
		has_comparator_ = true;
		comparator_ = name.toString();
	}

	public void SetLogNumber(long num) {
		has_log_number_ = true;
		log_number_ = num;
	}

	public void SetPrevLogNumber(long num) {
		has_prev_log_number_ = true;
		prev_log_number_ = num;
	}

	public void SetNextFile(long num) {
		has_next_file_number_ = true;
		next_file_number_ = num;
	}

	public void SetLastSequence(SequenceNumber seq) {
		has_last_sequence_ = true;
		last_sequence_ = seq;
	}

	public void SetCompactPointer(int level, InternalKey key) {
		compact_pointers_.add(new Pair<Integer, InternalKey>(level, key));
	}

	// Add the specified file at the specified number.
	// REQUIRES: This version has not been saved (see VersionSet::SaveTo)
	// REQUIRES: "smallest" and "largest" are smallest and largest keys in file
	public void AddFile(int level, long file, long file_size, InternalKey smallest,
			InternalKey largest) {
		FileMetaData f = new FileMetaData();
		f.setNumber(file);
		f.setFile_size(file_size);
		f.setSmallest(smallest);
		f.setLargest(largest);
		new_files_.add(new Pair<Integer, FileMetaData>(level, f));
	}

	// Delete the specified "file" from the specified "level".
	public void DeleteFile(int level, long file) {
		deleted_files_.add(new Pair<Integer, Long>(level, file));
	}

	public byte[] EncodeTo() {
		byte[] ret = new byte[0];
		if (has_comparator_) {
			ret = util.add(ret, coding.PutVarint32(Tag.kComparator),
					coding.PutLengthPrefixedSlice(new Slice(comparator_)));
		}
		if (has_log_number_) {
			ret = util.add(ret, coding.PutVarint32(Tag.kLogNumber),
					coding.PutVarint64(log_number_));
		}
		if (has_prev_log_number_) {
			ret = util.add(ret, coding.PutVarint32(Tag.kPrevLogNumber),
					coding.PutVarint64(prev_log_number_));
		}
		if (has_next_file_number_) {
			ret = util.add(ret, coding.PutVarint32(Tag.kNextFileNumber),
					coding.PutVarint64(next_file_number_));
		}
		if (has_last_sequence_) {
			ret = util.add(ret, coding.PutVarint32(Tag.kLastSequence),
					coding.PutVarint64(last_sequence_.value));
		}

		for (int i = 0; i < compact_pointers_.size(); i++) {
			ret = util.addN(
					ret,
					coding.PutVarint32(Tag.kCompactPointer),
					coding.PutVarint32(compact_pointers_.get(i).getFirst()
							.intValue()),
					coding.PutLengthPrefixedSlice(compact_pointers_.get(i)
							.getSecond().Encode()));
			// level
		}

		java.util.Iterator<Pair<Integer, Long>> iter = deleted_files_
				.iterator();
		while (iter.hasNext()) {
			Pair<Integer, Long> p = iter.next();
			ret = util.addN(ret, coding.PutVarint32(Tag.kDeletedFile),
					coding.PutVarint32(p.getFirst().intValue()),
					coding.PutVarint64(p.getSecond().longValue()));
		}

		java.util.Iterator<Pair<Integer, FileMetaData>> iter2 = new_files_
				.iterator();
		while (iter2.hasNext()) {
			Pair<Integer, FileMetaData> p = iter2.next();
			FileMetaData f = p.getSecond();
			ret = util.addN(ret, coding.PutVarint32(Tag.kNewFile),
					coding.PutVarint32(p.getFirst().intValue()),
					coding.PutVarint64(f.getNumber()),
					coding.PutVarint64(f.getFile_size()),
					coding.PutLengthPrefixedSlice(f.getSmallest().Encode()),
					coding.PutLengthPrefixedSlice(f.getLargest().Encode()));

		}

		return ret;

	}

	public Status DecodeFrom(Slice src) {
		Clear();
		// Slice input = src;
		ByteCollection input = new ByteCollection(src.data(), 0);
		String msg = null;
		int tag;

		// Temporary storage for parsing
		int level;
		long number;
		FileMetaData f = null;
		Slice str;
		InternalKey key;

		while (msg == null && !input.STOP()) {
			tag = coding.GetVarint32(input);
			if (!input.OK()) {
				return Status.Corruption(
						new Slice("VersionEdit Decode Error: "), new Slice(
								"Hint: Array outof boundary?"));
			}
			switch (tag) {
			case Tag.kComparator:
				str = coding.GetLengthPrefixedSlice(input);
				if (input.OK()) {
					comparator_ = str.toString();
					has_comparator_ = true;
				} else {
					msg = "comparator name";
				}
				break;

			case Tag.kLogNumber:
				log_number_ = coding.GetVarint64(input);
				if (input.OK()) {
					has_log_number_ = true;
				} else {
					msg = "log number";
				}
				break;

			case Tag.kPrevLogNumber:
				prev_log_number_ = coding.GetVarint64(input);
				if (input.OK()) {
					has_prev_log_number_ = true;
				} else {
					msg = "previous log number";
				}
				break;

			case Tag.kNextFileNumber:
				next_file_number_ = coding.GetVarint64(input);
				if (input.OK()) {
					has_next_file_number_ = true;
				} else {
					msg = "next file number";
				}
				break;

			case Tag.kLastSequence:
				last_sequence_ = new SequenceNumber(coding.GetVarint64(input));
				if (input.OK()) {
					has_last_sequence_ = true;
				} else {
					msg = "last sequence number";
				}
				break;

			case Tag.kCompactPointer:
				level = GetLevel(input);
				key = GetInternalKey(input);
				if (input.OK()) {
					compact_pointers_.add(new Pair<Integer, InternalKey>(level, key));
				} else {
					msg = "compaction pointer";
				}
				break;

			case Tag.kDeletedFile:
				level = GetLevel(input);
				number = coding.GetVarint64(input);
				if (input.OK()) {
					deleted_files_.add(new Pair<Integer, Long>(level, number));
				} else {
					msg = "deleted file";
				}
				break;

			case Tag.kNewFile:
				level = GetLevel(input);
				f = new FileMetaData();
				f.setNumber(coding.GetVarint64(input));
				f.setFile_size(coding.GetVarint64(input));
				f.setSmallest(GetInternalKey(input));
				f.setLargest(GetInternalKey(input));
				if (input.OK()) {
					new_files_.add(new Pair<Integer, FileMetaData>(level, f));
				} else {
					msg = "new-file entry";
				}
				break;

			default:
				msg = "unknown tag";
				break;
			}
		}

		if (msg == null && !input.STOP()) {
			msg = "invalid tag";
		}

		Status result = Status.OK();
		if (msg != null) {
			result = Status
					.Corruption(new Slice("VersionEdit"), new Slice(msg));
		}
		return result;
	}

	public String DebugString() {
		StringBuffer r = new StringBuffer();
		r.append("VersionEdit {");
		if (has_comparator_) {
			r.append("\n  Comparator: ");
			r.append(comparator_);
		}
		if (has_log_number_) {
			r.append("\n  LogNumber: ");
			r.append(logging.AppendNumberTo(log_number_));
		}
		if (has_prev_log_number_) {
			r.append("\n  PrevLogNumber: ");
			r.append(logging.AppendNumberTo(prev_log_number_));
		}
		if (has_next_file_number_) {
			r.append("\n  NextFile: ");
			r.append(logging.AppendNumberTo(next_file_number_));
		}
		if (has_last_sequence_) {
			r.append("\n  LastSeq: ");
			r.append(logging.AppendNumberTo(last_sequence_.value));
		}
		for (int i = 0; i < compact_pointers_.size(); i++) {
			r.append("\n  CompactPointer: ");

			r.append(logging
					.AppendNumberTo(compact_pointers_.get(i).getFirst()));
			r.append(" ");
			r.append(compact_pointers_.get(i).getSecond().DebugString());
		}
		java.util.Iterator<Pair<Integer, Long>> itr = deleted_files_.iterator();
		while (itr.hasNext()) {
			Pair<Integer, Long> p = itr.next();
			r.append("\n  DeleteFile: ");
			r.append(logging.AppendNumberTo(p.getFirst()));
			r.append(" ");
			r.append(logging.AppendNumberTo(p.getSecond()));
		}

		for (int i = 0; i < new_files_.size(); i++) {
			FileMetaData f = new_files_.get(i).getSecond();
			r.append("\n  AddFile: ");
			r.append(logging.AppendNumberTo(new_files_.get(i).getFirst()));
			r.append(" ");
			r.append(logging.AppendNumberTo(f.getNumber()));
			r.append(" ");

			r.append(f.getFile_size());
			r.append(" ");
			r.append(f.getSmallest().DebugString());
			r.append(" .. ");
			r.append(f.getLargest().DebugString());
		}
		r.append("\n}\n");
		return r.toString();
	}

	// typedef std::set< std::pair<int, uint64_t> > DeletedFileSet;

	public String comparator_;
	public long log_number_;
	public long prev_log_number_;
	public long next_file_number_;
	public SequenceNumber last_sequence_;
	public boolean has_comparator_;
	public boolean has_log_number_;
	public boolean has_prev_log_number_;
	public boolean has_next_file_number_;
	public boolean has_last_sequence_;

	List<Pair<Integer, InternalKey>> compact_pointers_;
	Set<Pair<Integer, Long>> deleted_files_;
	List<Pair<Integer, FileMetaData>> new_files_;

	private int GetLevel(ByteCollection input) {
		int v = coding.GetVarint32(input);
		if (input.OK() && v < config.kNumLevels) {
			return v;
		} else {
			input.ok = false;
			return -1; // invalid
		}
	}

	private InternalKey GetInternalKey(ByteCollection input) {
		Slice s = coding.GetLengthPrefixedSlice(input);
		if (s == null)
			return null;
		InternalKey det = new InternalKey();
		det.DecodeFrom(s);
		return det;
	}
	
	
	
	
}