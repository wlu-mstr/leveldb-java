package com.leveldb.common.version;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.leveldb.common.Comparator;
import com.leveldb.common.Iterator;
import com.leveldb.common.Slice;
import com.leveldb.common.Status;
import com.leveldb.common.config;
import com.leveldb.common.comparator.InternalKeyComparator;
import com.leveldb.common.db.FileMetaData;
import com.leveldb.common.db.InternalKey;
import com.leveldb.common.db.LookupKey;
import com.leveldb.common.db.ParsedInternalKey;
import com.leveldb.common.db.TableCache;
import com.leveldb.common.options.ReadOptions;
import com.leveldb.common.table.TwoLevelIterator;
import com.leveldb.util.SequenceNumber;
import com.leveldb.util.ValueType;
import com.leveldb.util.util;

public class Version {
	// constant values
	public static final int kTargetFileSize = 2 * 1048576; // 2MB

	public static long MaxFileSizeForLevel(int level) {
		return kTargetFileSize; // We could vary per level to reduce number of
								// files?
	}

	// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
	// stop building a single file in a level->level+1 compaction.
	public static final long kMaxGrandParentOverlapBytes = 10 * kTargetFileSize; // 20MB

	// Maximum number of bytes in all compacted files. We avoid expanding
	// the lower level file set of a compaction if it would make the
	// total compaction cover more than this many bytes.
	public static final long kExpandedCompactionByteSizeLimit = 25 * kTargetFileSize;

	// / member
	public VersionSet vset_; // VersionSet to which this Version belongs
	public Version next_; // Next version in linked list
	public Version prev_; // Previous version in linked list
	public int refs_; // Number of live refs to this version

	// List of files per level
	public List<List<FileMetaData>> files_;

	// Next file to compact based on seek stats.
	public FileMetaData file_to_compact_;
	public int file_to_compact_level_;

	// Level that should be compacted next and its compaction score.
	// Score < 1 means compaction is not strictly needed. These fields
	// are initialized by Finalize().
	public double compaction_score_;
	public int compaction_level_;

	public static long TotalFileSize(List<FileMetaData> files) {
		long sum = 0;
		for (int i = 0; i < files.size(); i++) {
			sum += files.get(i).getFile_size();
		}
		return sum;
	}

	/*
	 * An internal iterator. For a given version/level pair, yields information
	 * about the files in the level. For a given entry, key() is the largest key
	 * that occurs in the file, and value() is an 16-byte value containing the
	 * file number and file size, both encoded using EncodeFixed64.
	 * 
	 * Key: largest key of the file; Value: file number | file size; NOTE: for
	 * level > 0; largest key of the file is enough 'cause no overlap of keys
	 */
	static class LevelFileNumIterator extends Iterator {
		LevelFileNumIterator(InternalKeyComparator icmp,
				List<FileMetaData> flist) {
			icmp_ = icmp;
			flist_ = flist;
			index_ = flist.size(); // Marks as invalid
		}

		public boolean Valid() {
			return index_ < flist_.size();
		}

		/*
		 * seek to the file containing target key(non-Javadoc)
		 * 
		 * @see com.leveldb.common.Iterator#Seek(com.leveldb.common.Slice)
		 */
		public void Seek(Slice target) {
			index_ = FindFile(icmp_, flist_, target);
		}

		public void SeekToFirst() {
			index_ = 0;
		}

		public void SeekToLast() {
			index_ = flist_.isEmpty() ? 0 : flist_.size() - 1;
		}

		public void Next() {
			assert (Valid());
			index_++;
		}

		public void Prev() {
			assert (Valid());
			if (index_ == 0) {
				index_ = flist_.size(); // Marks as invalid
			} else {
				index_--;
			}
		}

		public Slice key() {
			assert (Valid());
			return flist_.get(index_).getLargest().Encode();
		}

		public Slice value() {
			assert (Valid());
			util.putLong(value_buf_, 0, flist_.get(index_).getNumber());
			util.putLong(value_buf_, 8, flist_.get(index_).getFile_size());
			return new Slice(value_buf_);
		}

		public Status status() {
			return Status.OK();
		}

		private InternalKeyComparator icmp_;
		private List<FileMetaData> flist_;
		private int index_;

		// Backing store for value(). Holds the file number and size.
		private byte[] value_buf_ = new byte[16];
	}

	/*
	 * return iterator over file with #number | size# iterator from TableCache
	 * from Table
	 */
	static class GetFileIteratorBlockFunction implements
			TwoLevelIterator.BlockFunction {
		/*
		 * @param arg a TableCache
		 * 
		 * @param file_value #file number | file size#
		 * 
		 * refer to TableCache, try to hit the table (file with #file number |
		 * file size#)
		 */
		@Override
		public Iterator exec(Object arg, ReadOptions options, Slice file_value) {
			TableCache cache = (TableCache) arg;
			if (file_value.size() != 16) { // number | size
				return Iterator.NewErrorIterator(Status.Corruption(new Slice(
						"FileReader invoked with unexpected value"), null));
			} else {
				byte number_size[] = file_value.data();
				return cache.NewIterator(options, util.toLong(number_size, 0),
						util.toLong(number_size, 8), null);
			}
		}

	}

	// Append to iters a sequence of iterators that will
	// yield the contents of this Version when merged together.
	// REQUIRES: This version has been saved (see VersionSet::SaveTo)
	public void AddIterators(ReadOptions readoption, List<Iterator> iters) {
		// Merge all level zero files together since they may overlap
		for (int i = 0; i < files_.get(0).size(); i++) {
			iters.add(vset_.table_cache_.NewIterator(readoption, files_.get(0)
					.get(i).getNumber(), files_.get(0).get(i).getFile_size(),
					null));
		}

		// For levels > 0, we can use a concatenating iterator that sequentially
		// walks through the non-overlapping files in the level, opening them
		// lazily.
		for (int level = 1; level < config.kNumLevels; level++) {
			if (!files_.get(level).isEmpty()) {
				iters.add(NewConcatenatingIterator(readoption, level));
			}
		}
	}

	/*
	 * Lookup the value for key. If found, store it in *val and return OK. Else
	 * return a non-OK status. Fills *stats. REQUIRES: lock is not held
	 */
	public static class GetStats {
		FileMetaData seek_file;
		int seek_file_level;
	}

	/*
	 * If "iter" points at a value or deletion for user_key, store either the
	 * value, or a NotFound error and return true. Else return false.
	 */
	boolean GetValue(Comparator cmp, Iterator iter, Slice user_key,
			Slice value, Status[] s) {
		if (!iter.Valid()) {
			return false; // not stop
		}
		ParsedInternalKey parsed_key = InternalKey
				.ParseInternalKey_(iter.key());

		if (parsed_key == null) {
			s[0] = Status.Corruption(new Slice("corrupted key for "), user_key);
			return true; // stop with error
		}
		if (cmp.Compare(parsed_key.user_key, user_key) != 0) {
			return false; // not stop
		}
		switch (parsed_key.type.value) {
		case ValueType.kTypeDeletion:
			s[0] = Status.NotFound(new Slice(), null);
			// Use an empty error message for speed
			break; // stop with error
		case ValueType.kTypeValue: {
			Slice v = iter.value();
			value.setData_(v.data());
			// value->assign(v.data(), v.size());
			break; // stop and set the data, but ...
		}
		}
		return true;
	}

	// sort a list of FileMetaData according to number and return
	// the sorted array
	FileMetaData[] sort(List<FileMetaData> ifilemd) {
		FileMetaData ifmd_[] = new FileMetaData[ifilemd.size()];
		int idx = 0;
		for (FileMetaData fd : ifilemd) {
			ifmd_[idx++] = fd;
		}
		Arrays.sort(ifmd_, new java.util.Comparator<FileMetaData>() {

			@Override
			public int compare(FileMetaData arg0, FileMetaData arg1) {
				// wlu, 2012-7-7, bugfix: sequence number larger should be at
				// the front
				return (-1) * (int) (arg0.getNumber() - arg1.getNumber());
			}
		});

		return ifmd_;
	}

	/**
	 * Binary search to find earliest index whose largest key >= ikey.
	 * 
	 * @param icmp
	 * @param files
	 *            list of files
	 * @param ikey
	 *            target key
	 * @return earliest index whose largest key >= ikey. so ikey locates inner
	 *         index-th file
	 */
	public static int FindFile(InternalKeyComparator icmp,
			List<FileMetaData> files, Slice ikey) {
		int left = 0;
		int right = files.size();
		while (left < right) {
			int mid = (left + right) / 2;
			FileMetaData f = files.get(mid);
			if (icmp.Compare(f.getLargest().Encode(), ikey) < 0) {
				// Key at "mid.largest" is < "target". Therefore all
				// files at or before "mid" are uninteresting.
				left = mid + 1;
			} else {
				// Key at "mid.largest" is >= "target". Therefore all files
				// after "mid" are uninteresting.
				right = mid;
			}
		}
		return right;
	}

	/*
	 * Get the value of a given lookup key from all files on each level return
	 * the value as a byte array s must be an array with length 1
	 */
	public byte[] Get(ReadOptions options, LookupKey k, GetStats stats,
			Status[] s) {
		Slice value = new Slice();
		byte value_array[] = new byte[0];
		Slice ikey = k.internal_key();
		Slice user_key = k.user_key();
		Comparator ucmp = vset_.icmp_.user_comparator();
		// Status[] s = new Status[1];

		stats.seek_file = null;
		stats.seek_file_level = -1;
		FileMetaData last_file_read = null;
		int last_file_read_level = -1;

		// We can search level-by-level since entries never hop across
		// levels. Therefore we are guaranteed that if we find data
		// in an smaller level, later levels are irrelevant.
		List<FileMetaData> tmp = new ArrayList<FileMetaData>();
		FileMetaData tmp2;
		for (int level = 0; level < config.kNumLevels; level++) {
			int num_files = files_.get(level).size();
			if (num_files == 0)
				continue;

			// Get the list of files to search in this level
			List<FileMetaData> files = files_.get(level);
			FileMetaData[] files2 = null;
			if (level == 0) {
				// Level-0 files may overlap each other. Find all files that
				// overlap user_key and process them in order from newest to
				// oldest.
				// tmp.reserve(num_files);
				for (int i = 0; i < num_files; i++) {
					FileMetaData f = files.get(i);
					if (ucmp.Compare(user_key, f.getSmallest().user_key()) >= 0
							&& ucmp.Compare(user_key, f.getLargest().user_key()) <= 0) {
						tmp.add(f);
					}
				}
				if (tmp.isEmpty())
					continue;

				files2 = sort(tmp);
				// files = &tmp[0];
				num_files = tmp.size(); // Or file2.length
			} else {
				// Binary search to find earliest index whose largest key >=
				// ikey.
				int index = FindFile(vset_.icmp_, files_.get(level), ikey);
				if (index >= num_files) {
					files = null;
					num_files = 0;
				} else {
					tmp2 = files.get(index);
					if (ucmp.Compare(user_key, tmp2.getSmallest().user_key()) < 0) {
						// All of "tmp2" is past any data for user_key
						files = null;
						num_files = 0;
					} else {
						files2 = new FileMetaData[1];
						files2[0] = tmp2;
						num_files = 1;
					}
				}
			}

			for (int i = 0; i < num_files; ++i) {
				if (last_file_read != null && stats.seek_file == null) {
					// We have had more than one seek for this read. Charge the
					// 1st file.
					stats.seek_file = last_file_read;
					stats.seek_file_level = last_file_read_level;
				}

				FileMetaData f = files2[i];
				last_file_read = f;
				last_file_read_level = level;

				Iterator iter = vset_.table_cache_.NewIterator(options,
						f.getNumber(), f.getFile_size(), null);
				iter.Seek(ikey);

				boolean done = GetValue(ucmp, iter, user_key, value, s);
				value_array = util.add(value_array, value.data()); // add value
																	// each loop
				if (!iter.status().ok()) {
					s[0] = iter.status();
					iter = null;
					return value_array;
				} else {
					iter = null;
					if (done) { // Here is the normal return. I think only one
								// value is found, which is the 1st one
						return value_array;
					}
				}
			}
		}

		s[0] = Status.NotFound(new Slice(), null); // Use an empty error message
													// for speed
		return null;
	}

	// Adds "stats" into the current state. Returns true if a new
	// compaction may need to be triggered, false otherwise.
	// REQUIRES: lock is held
	public boolean UpdateStats(GetStats stats) {
		FileMetaData f = stats.seek_file;
		if (f != null) {
			f.setAllowed_seeks(f.getAllowed_seeks() - 1);
			if (f.getAllowed_seeks() <= 0 && file_to_compact_ == null) {
				file_to_compact_ = f;
				file_to_compact_level_ = stats.seek_file_level;
				return true;
			}
		}
		return false;
	}

	// Reference count management (so Versions do not disappear out from
	// under live iterators)
	public void Ref() {
		++refs_;
	}

	public void Unref() {
		--refs_;
		if (refs_ == 0) {
			// wlu, 2012-7-7
			vset_.Close();
		}
	}

	/**
	 * add all files @level that are overlapped with given begin-end; as for
	 * level-0, it is pretty tricky: file2 overlaps with file1 but not with
	 * given begin-end && file1 overlaps with the given b-e; then both file1 and
	 * file2 are added to #input#
	 * 
	 * @param level
	 *            get from files @level
	 * @param begin
	 *            left boundary
	 * @param end
	 * @param inputs
	 *            get and save to this <code>List</code>
	 */
	public void GetOverlappingInputs(int level, InternalKey begin, // NULL means
																	// before
			// all keys
			InternalKey end, // NULL means after all keys
			List<FileMetaData> inputs) {
		inputs.clear();
		Slice user_begin = null, user_end = null;
		if (begin != null) {
			user_begin = begin.user_key();
		}
		if (end != null) {
			user_end = end.user_key();
		}
		Comparator user_cmp = vset_.icmp_.user_comparator();
		for (int i = 0; i < files_.get(level).size();) {
			FileMetaData f = files_.get(level).get(i++);
			Slice file_start = f.getSmallest().user_key();
			Slice file_limit = f.getLargest().user_key();
			if (begin != null && user_cmp.Compare(file_limit, user_begin) < 0) {
				// "f" is completely before specified range; skip it
			} else if (end != null
					&& user_cmp.Compare(file_start, user_end) > 0) {
				// "f" is completely after specified range; skip it
			} else {
				// "f" is partialy/completely inner the range
				inputs.add(f);
				if (level == 0) {
					// Level-0 files may overlap each other. So check if the
					// newly
					// added file has expanded the range. If so, restart search.
					if (begin != null
							&& user_cmp.Compare(file_start, user_begin) < 0) {
						user_begin = file_start;
						inputs.clear();
						i = 0;
					} else if (end != null
							&& user_cmp.Compare(file_limit, user_end) > 0) {
						user_end = file_limit;
						inputs.clear();
						i = 0;
					}
				}
			}
		}
	}

	// user_key is larger than the largest user_key of the file f
	public static boolean AfterFile(Comparator ucmp, Slice user_key,
			FileMetaData f) {
		// NULL user_key occurs before all keys and is therefore never after *f
		return (user_key != null && ucmp.Compare(user_key, f.getLargest()
				.user_key()) > 0);
	}

	// user_key is smaller than the smallest user_key of the file f
	public static boolean BeforeFile(Comparator ucmp, Slice user_key,
			FileMetaData f) {
		// NULL user_key occurs after all keys and is therefore never before *f
		return (user_key != null && ucmp.Compare(user_key, f.getSmallest()
				.user_key()) < 0);
	}

	//
	public static boolean SomeFileOverlapsRange(InternalKeyComparator icmp,
			boolean disjoint_sorted_files, List<FileMetaData> files,
			Slice smallest_user_key, Slice largest_user_key) {
		Comparator ucmp = icmp.user_comparator();
		if (!disjoint_sorted_files) {
			// Need to check against all files
			for (int i = 0; i < files.size(); i++) {
				FileMetaData f = files.get(i);
				if (AfterFile(ucmp, smallest_user_key, f)
						|| BeforeFile(ucmp, largest_user_key, f)) {
					// No overlap
				} else {
					return true; // Overlap
				}
			}
			return false;
		}

		// Binary search over file list
		int index = 0;
		if (smallest_user_key != null) {
			// Find the earliest possible internal key for smallest_user_key
			InternalKey small = new InternalKey(smallest_user_key,
					new SequenceNumber(SequenceNumber.kMaxSequenceNumber),
					new ValueType(ValueType.kValueTypeForSeek));
			index = FindFile(icmp, files, small.Encode());
		}

		if (index >= files.size()) {
			// beginning of range is after all files, so no overlap.
			return false;
		}

		return !BeforeFile(ucmp, largest_user_key, files.get(index));
	}

	// Returns true iff some file in the specified level overlaps
	// some part of [*smallest_user_key,*largest_user_key].
	// smallest_user_key==NULL represents a key smaller than all keys in the DB.
	// largest_user_key==NULL represents a key largest than all keys in the DB.
	public boolean OverlapInLevel(int level, Slice smallest_user_key,
			Slice largest_user_key) {
		return SomeFileOverlapsRange(vset_.icmp_, (level > 0),
				files_.get(level), smallest_user_key, largest_user_key);
	}

	// Return the level at which we should place a new memtable compaction
	// result that covers the range [smallest_user_key,largest_user_key].
	// constrains:
	// 1) not too high, level should be 0 or 1;
	// 2) overlap with grand parent should not larger than 20MB (this is the
	// situation on level-1,
	// and the overlap wtih level-2 should not be larger than 20MB)
	public int PickLevelForMemTableOutput(Slice smallest_user_key,
			Slice largest_user_key) {
		int level = 0;
		// if is not overlapped with any file in level-0 (cool!), just
		// go to higher level
		if (!OverlapInLevel(0, smallest_user_key, largest_user_key)) {
			// Push to next level if there is no overlap in next level,
			// and the #bytes overlapping in the level after that are limited.
			InternalKey start = new InternalKey(smallest_user_key,
					new SequenceNumber(SequenceNumber.kMaxSequenceNumber),
					new ValueType(ValueType.kValueTypeForSeek));
			InternalKey limit = new InternalKey(largest_user_key,
					new SequenceNumber(0), new ValueType((byte) 0));
			List<FileMetaData> overlaps = new ArrayList<FileMetaData>();
			while (level < config.kMaxMemCompactLevel) { // not too high
				if (OverlapInLevel(level + 1, smallest_user_key,
						largest_user_key)) {
					break;
				}
				GetOverlappingInputs(level + 2, start, limit, overlaps);
				long sum = TotalFileSize(overlaps);
				if (sum > kMaxGrandParentOverlapBytes) {
					break; // over lap in grand parent level is too large.. just
							// stop here
				}
				level++;
			}
		}
		return level;
	}

	public int NumFiles(int level) {
		return files_.get(level).size();
	}

	// Return a human readable string that describes this version's contents.
	public String DebugString() {
		StringBuffer r = new StringBuffer();
		for (int level = 0; level < config.kNumLevels; level++) {
			// E.g.,
			// --- level 1 ---
			// 17:123['a' .. 'd']
			// 20:43['e' .. 'g']
			r.append("--- level ");
			r.append(level);
			r.append(" ---\n");
			List<FileMetaData> files = files_.get(level);
			for (int i = 0; i < files.size(); i++) {
				r.append(' ');

				r.append(files.get(i).getNumber());
				r.append(':');

				r.append(files.get(i).getFile_size());

				r.append("[");
				r.append(files.get(i).getSmallest().DebugString());
				r.append(" .. ");
				r.append(files.get(i).getLargest().DebugString());
				r.append("]\n");
			}
		}
		return r.toString();
	}

	public String toString() {
		return DebugString();
	}

	public Iterator NewConcatenatingIterator(ReadOptions readoptions, int level) {
		return TwoLevelIterator.NewTwoLevelIterator(new LevelFileNumIterator(
				vset_.icmp_, files_.get(level)),
				new GetFileIteratorBlockFunction(), vset_.table_cache_,
				readoptions);
	}

	public Version(VersionSet vset) {
		vset_ = vset;
		next_ = this;
		prev_ = this;
		refs_ = 0;
		file_to_compact_ = null;
		file_to_compact_level_ = -1;
		compaction_score_ = -1;
		compaction_level_ = -1;
		files_ = new ArrayList<List<FileMetaData>>(config.kNumLevels);
		for (int i = 0; i < config.kNumLevels; i++) {
			files_.add(new ArrayList<FileMetaData>());
		}

	}

	// No copying allowed
}
