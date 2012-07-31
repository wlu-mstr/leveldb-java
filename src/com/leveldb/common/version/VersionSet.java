package com.leveldb.common.version;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.leveldb.common.Env;
import com.leveldb.common.Iterator;
import com.leveldb.common.Slice;
import com.leveldb.common.Status;
import com.leveldb.common.Table;
import com.leveldb.common.config;
import com.leveldb.common.comparator.InternalKeyComparator;
import com.leveldb.common.db.Compaction;
import com.leveldb.common.db.FileMetaData;
import com.leveldb.common.db.InternalKey;
import com.leveldb.common.db.TableCache;
import com.leveldb.common.file._SequentialFile;
import com.leveldb.common.file._WritableFile;
import com.leveldb.common.file.filename;
import com.leveldb.common.log.Reader;
import com.leveldb.common.log.Writer;
import com.leveldb.common.options.Options;
import com.leveldb.common.options.ReadOptions;
import com.leveldb.common.table.MergingIterator;
import com.leveldb.common.table.TwoLevelIterator;
import com.leveldb.util.Pair;
import com.leveldb.util.SequenceNumber;

/* 2012-4-24, test cases are added; find: SequenceNumber's
 * Max value is not right 
 */
public class VersionSet {

	Log LOG = LogFactory.getLog(VersionSet.class);

	public void Close() {
		// wlu, 2012-7-10, bugFix: check before operating
		if (descriptor_file_ != null) {
			descriptor_file_.Close();
		}
	}

	// A helper class so we can efficiently apply a whole sequence
	// of edits to a particular state without creating intermediate
	// Versions that contain full copies of the intermediate state.
	static class Builder {
		// Helper to sort by v->files_[file_number].smallest

		// typedef Set<FileMetaData , BySmallestKey> FileSet;
		class LevelState {
			Set<Long> deleted_files = new HashSet<Long>();
			TreeSet<InnerFileMataData> added_files = new TreeSet<VersionSet.Builder.InnerFileMataData>();

		}

		/**
		 * a wrapper of FileMetaData with comparable (BySmallestKey), to be used
		 * in <code>SortedSet</code>
		 * 
		 * @author wlu
		 * 
		 */
		class InnerFileMataData extends FileMetaData implements
				Comparable<InnerFileMataData> {

			// below is what I added
			public InternalKeyComparator internal_comparator = vset_.icmp_;

			@Override
			public int compareTo(InnerFileMataData f2) {
				int r = internal_comparator.Compare(this.getSmallest(),
						f2.getSmallest());
				if (r != 0) {
					return r;
				} else {
					// Break ties by file number
					return (int) (this.getNumber() - f2.getNumber());
				}
			}

			// deep copy
			public InnerFileMataData(FileMetaData ifmd) {
				refs = ifmd.refs;
				allowed_seeks = ifmd.allowed_seeks; // Seeks allowed until
													// compaction
				number = ifmd.number;
				file_size = ifmd.file_size; // File size in bytes
				smallest = ifmd.smallest; // Smallest internal key served by
											// table
				largest = ifmd.largest; // Largest internal key served by table
			}

			// compare to a FileMetaData, used when merge
			public int compareTo(FileMetaData f2) {
				int r = internal_comparator.Compare(this.smallest, f2.smallest);
				if (r != 0) {
					return r;
				} else {
					// Break ties by file number
					return (int) (this.getNumber() - f2.getNumber());
				}
			}

		}

		VersionSet vset_;
		Version base_;
		LevelState[] levels_ = new LevelState[config.kNumLevels];

		// Initialize a builder with the files from base and other info from
		// vset
		Builder(VersionSet vset, Version base) {
			vset_ = vset;
			base_ = base;
			base_.Ref();
			// BySmallestKey cmp;
			// cmp.internal_comparator = vset_.icmp_;
			for (int level = 0; level < config.kNumLevels; level++) {
				levels_[level] = new LevelState();
				levels_[level].added_files = new TreeSet<InnerFileMataData>();// new
																				// FileSet(cmp);
			}

			// ~Builder() {
			// for (int level = 0; level < config::kNumLevels; level++) {
			// const FileSet* added = levels_[level].added_files;
			// std::vector<FileMetaData*> to_unref;
			// to_unref.reserve(added->size());
			// for (FileSet::const_iterator it = added->begin();
			// it != added->end(); ++it) {
			// to_unref.push_back(*it);
			// }
			// delete added;
			// for (uint32_t i = 0; i < to_unref.size(); i++) {
			// FileMetaData* f = to_unref[i];
			// f->refs--;
			// if (f->refs <= 0) {
			// delete f;
			// }
			// }
			// }
			// base_->Unref();
		}

		// Apply all of the edits in *edit to the current state.
		void Apply(VersionEdit edit) {
			// Update compaction pointers
			for (int i = 0; i < edit.compact_pointers_.size(); i++) {
				int level = edit.compact_pointers_.get(i).getFirst();
				vset_.compact_pointer_[level] = edit.compact_pointers_.get(i)
						.getSecond().Encode().toString();
			}

			// Delete files
			Set<Pair<Integer, Long>> del = edit.deleted_files_;
			java.util.Iterator<Pair<Integer, Long>> iter = del.iterator();
			while (iter.hasNext()) {
				Pair<Integer, Long> s = iter.next();
				int lLevel = s.getFirst();
				long lNumber = s.getSecond();
				levels_[lLevel].deleted_files.add(lNumber);
			}

			// Add new files
			for (int i = 0; i < edit.new_files_.size(); i++) {
				int level = edit.new_files_.get(i).getFirst();
				InnerFileMataData f = new InnerFileMataData(edit.new_files_
						.get(i).getSecond());
				f.setRefs(1);

				// We arrange to automatically compact this file after
				// a certain number of seeks. Let's assume:
				// (1) One seek costs 10ms
				// (2) Writing or reading 1MB costs 10ms (100MB/s)
				// (3) A compaction of 1MB does 25MB of IO:
				// 1MB read from this level
				// 10-12MB read from next level (boundaries may be misaligned)
				// 10-12MB written to next level
				// This implies that 25 seeks cost the same as the compaction
				// of 1MB of data. I.e., one seek costs approximately the
				// same as the compaction of 40KB of data. We are a little
				// conservative and allow approximately one seek for every 16KB
				// of data before triggering a compaction.
				f.allowed_seeks = (int) (f.file_size / 16384);
				if (f.allowed_seeks < 100) {
					f.allowed_seeks = 100;
				}

				levels_[level].deleted_files.remove(f.number);
				levels_[level].added_files.add(f);
			}
		}

		// Save the current state in v. Merge base files and added files
		void SaveTo(Version v) throws Exception {
			// BySmallestKey cmp;
			// cmp.internal_comparator = &vset_->icmp_;
			for (int level = 0; level < config.kNumLevels; level++) {
				// Merge the set of added files with the set of pre-existing
				// files. Drop any deleted files. Store the result in v.
				// base files are sorted too!!! This algorithm is not exactly
				// the same as cpp version
				List<FileMetaData> base_files = base_.files_.get(level);
				java.util.Iterator<FileMetaData> base_iter = base_files
						.iterator();
				TreeSet<InnerFileMataData> added = levels_[level].added_files;
				List<InnerFileMataData> addedt = new ArrayList<VersionSet.Builder.InnerFileMataData>();
				java.util.Iterator<InnerFileMataData> added_itert = added
						.iterator();
				while (added_itert.hasNext()) {
					addedt.add(added_itert.next());
				}

				java.util.Iterator<InnerFileMataData> added_iter = added
						.iterator();
				boolean base_added = true;
				FileMetaData lbase = null;
				for (; added_iter.hasNext();) {
					InnerFileMataData ladded = added_iter.next();
					for (; (base_added && base_iter.hasNext()) || !base_added;) {
						if (base_added) {
							lbase = base_iter.next();
						}
						if (ladded.compareTo(lbase) > 0) {
							MaybeAddFile(v, level, lbase);
							base_added = true;
						} else {
							base_added = false;
							break;
						}
					}
					MaybeAddFile(v, level, ladded);
				}
				// Add remaining base files
				for (; (base_added && base_iter.hasNext()) || !base_added;) {
					if (base_added) {
						lbase = base_iter.next();
					}
					MaybeAddFile(v, level, lbase);
					base_added = true;
				}

				// Make sure there is no overlap in levels > 0
				if (level > 0) {
					for (int i = 1; i < v.files_.get(level).size(); i++) {
						InternalKey prev_end = v.files_.get(level).get(i - 1).largest;
						InternalKey this_begin = v.files_.get(level).get(i).smallest;
						if (vset_.icmp_.Compare(prev_end, this_begin) >= 0) {

							throw new Exception(
									"overlapping ranges in same level "
											+ prev_end.DebugString() + " vs. "
											+ this_begin.DebugString());
							// abort();
						}
					}
				}
			}
		}

		/**
		 * <li>f is not in deleted_files</li> <li>f is not in overlapped with
		 * any file in the level</li>
		 */
		void MaybeAddFile(Version v, int level, FileMetaData f) {
			if (levels_[level].deleted_files.contains(f.number)) {
				// File is deleted: do nothing
			} else {
				List<FileMetaData> files = v.files_.get(level);
				if (level > 0 && !files.isEmpty()) {
					// Must not overlap
					if (vset_.icmp_.Compare(
							files.get(files.size() - 1).largest, f.smallest) >= 0) {
						System.err.println();
					}
				}
				f.refs++;
				files.add(f);
			}
		}
	}

	public VersionSet(String dbname, Options options, TableCache table_cache,
			InternalKeyComparator cmp) {
		env_ = options.env;
		dbname_ = dbname;
		options_ = options;
		table_cache_ = table_cache;
		icmp_ = cmp;
		next_file_number_ = 2;
		manifest_file_number_ = 0; // Filled by Recover = )
		last_sequence_ = new SequenceNumber(0);
		log_number_ = 0;
		prev_log_number_ = 0;
		descriptor_file_ = null;
		descriptor_log_ = null;
		dummy_versions_ = new Version(null); // tricky
		current_ = new Version(null);
		AppendVersion(new Version(this));
		for (int i = 0; i < config.kNumLevels; i++) {
			compact_pointer_[i] = "";
		}
	}

	/**
	 * Apply *edit to the current version to form a new descriptor that is both
	 * saved to persistent state and installed as the new current version. Will
	 * release *mu while actually writing to the file.
	 * <p>
	 * </p>
	 * REQUIRES: mu is held on entry. REQUIRES: no other thread concurrently
	 * calls LogAndApply()
	 * 
	 * @param edit
	 * @param mu
	 * @return
	 */
	public Status LogAndApply(VersionEdit edit, ReentrantLock mu) {
		if (edit.has_log_number_) {
			assert (edit.log_number_ >= log_number_);
			assert (edit.log_number_ < next_file_number_);
		} else {
			edit.SetLogNumber(log_number_);
		}

		if (!edit.has_prev_log_number_) {
			edit.SetPrevLogNumber(prev_log_number_);
		}

		edit.SetNextFile(next_file_number_);
		edit.SetLastSequence(last_sequence_);

		Version v = new Version(this);
		{
			Builder builder = new Builder(this, current_);
			builder.Apply(edit);
			try {
				builder.SaveTo(v);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		Finalize(v);

		// Initialize new descriptor log file if necessary by creating
		// a temporary file that contains a snapshot of the current version.
		String new_manifest_file = null;
		Status s = Status.OK();
		if (descriptor_log_ == null) {
			// No reason to unlock *mu here since we only hit this path in the
			// first call to LogAndApply (when opening the database).
			assert (descriptor_file_ == null);
			new_manifest_file = filename.DescriptorFileName(dbname_,
					manifest_file_number_);
			edit.SetNextFile(next_file_number_);
			descriptor_file_ = env_.NewWritableFile(new_manifest_file);
			descriptor_log_ = new Writer(descriptor_file_);
			s = WriteSnapshot(descriptor_log_);
		}

		// Unlock during expensive MANIFEST log write
		{
			mu.unlock();

			// Write new record to MANIFEST log
			if (s.ok()) {
				byte[] record = edit.EncodeTo();
				s = descriptor_log_.AddRecord(new Slice(record));
				if (s.ok()) {
					s = descriptor_file_.Sync();
				}
			}

			// If we just created a new descriptor file, install it by writing a
			// new CURRENT file that points to it.
			if (s.ok() && new_manifest_file != "") {
				s = filename.SetCurrentFile(env_, dbname_,
						manifest_file_number_);
			}

			mu.lock();
		}

		// Install the new version
		if (s.ok()) {
			AppendVersion(v);
			log_number_ = edit.log_number_;
			prev_log_number_ = edit.prev_log_number_;
		} else {
			v = null;
			if (new_manifest_file != "") {
				descriptor_log_ = null;
				descriptor_file_.Close();
				descriptor_file_ = null;
				env_.DeleteFile(new_manifest_file);
			}
		}

		return s;
	}

	// Recover the last saved descriptor from persistent storage.
	public Status Recover() {
		class LogReporter extends Reader.Reporter {
			Status status;

			public void Corruption(int bytes, Status s) {
				if (status.ok()) {
					status = s;
				}
			}
		}

		// Read "CURRENT" file, which contains a pointer to the current manifest
		// file
		String current = Env.ReadFileToString(env_,
				filename.CurrentFileName(dbname_));

		if (current == null || current.length() == 0
				|| current.charAt(current.length() - 1) != '\n') {
			return Status.Corruption(new Slice(
					"CURRENT file does not end with newline"), null);
		}
		current = current.substring(0, current.length() - 1);

		String dscname = dbname_ + "/" + current;
		_SequentialFile file = env_.NewSequentialFile(dscname);

		boolean have_log_number = false;
		boolean have_prev_log_number = false;
		boolean have_next_file = false;
		boolean have_last_sequence = false;
		long next_file = 0;
		SequenceNumber last_sequence = new SequenceNumber(0);
		long log_number = 0;
		long prev_log_number = 0;
		VersionSet.Builder builder = new VersionSet.Builder(this, current_);

		Status s = Status.OK();
		{
			LogReporter reporter = new LogReporter();
			reporter.status = Status.OK();
			Reader reader = new Reader(file, reporter, true/* checksum */, 0/* initial_offset */);
			Slice record = new Slice();
			byte[] scratch = new byte[0];

			while (reader.ReadRecord(record, scratch) && s.ok()) {
				VersionEdit edit = new VersionEdit();
				s = edit.DecodeFrom(record);
				if (s.ok()) {
					if (edit.has_comparator_
							&& edit.comparator_.compareTo(icmp_
									.user_comparator().Name()) != 0) {
						s = Status.InvalidArgument(new Slice(edit.comparator_
								+ "does not match existing comparator "),
								new Slice(icmp_.user_comparator().Name()));
					}
				}

				if (s.ok()) {
					builder.Apply(edit);
				}

				if (edit.has_log_number_) {
					log_number = edit.log_number_;
					have_log_number = true;
				}

				if (edit.has_prev_log_number_) {
					prev_log_number = edit.prev_log_number_;
					have_prev_log_number = true;
				}

				if (edit.has_next_file_number_) {
					next_file = edit.next_file_number_;
					have_next_file = true;
				}

				if (edit.has_last_sequence_) {
					last_sequence = edit.last_sequence_;
					have_last_sequence = true;
				}
			}
		}

		if (s.ok()) {
			if (!have_next_file) {
				s = Status.Corruption(new Slice(
						"no meta-nextfile entry in descriptor"), null);
			} else if (!have_log_number) {
				s = Status.Corruption(new Slice(
						"no meta-lognumber entry in descriptor"), null);
			} else if (!have_last_sequence) {
				s = Status.Corruption(new Slice(
						"no last-sequence-number entry in descriptor"), null);
			}

			if (!have_prev_log_number) {
				prev_log_number = 0;
			}

			MarkFileNumberUsed(prev_log_number);
			MarkFileNumberUsed(log_number);
		}

		if (s.ok()) {
			Version v = new Version(this);
			try {
				builder.SaveTo(v);
			} catch (Exception e) {
				e.printStackTrace();
			}
			// Install recovered version
			Finalize(v);
			AppendVersion(v);
			manifest_file_number_ = next_file;
			next_file_number_ = next_file + 1;
			last_sequence_ = last_sequence;
			log_number_ = log_number;
			prev_log_number_ = prev_log_number;
		}

		file.Close();
		return s;
	}

	// Return the current version.
	public Version current() {
		return current_;
	}

	// Return the current manifest file number
	public long ManifestFileNumber() {
		return manifest_file_number_;
	}

	// Allocate and return a new file number
	public long NewFileNumber() {
		return next_file_number_++;
	}

	// Return the number of Table files at the specified level.
	public int NumLevelFiles(int level) {
		assert (level >= 0 && level < config.kNumLevels);
		return current_.files_.get(level).size();
	}

	long TotalFileSize(List<FileMetaData> files) {
		long sum = 0;
		for (int i = 0; i < files.size(); i++) {
			sum += files.get(i).file_size;
		}
		return sum;
	}

	// Return the combined file size of all files at the specified level.
	public long NumLevelBytes(int level) {
		assert (level >= 0);
		assert (level < config.kNumLevels);
		return TotalFileSize(current_.files_.get(level));
	}

	// Return the last sequence number.
	public SequenceNumber LastSequence() {
		return last_sequence_;
	}

	// Set the last sequence number to s.
	public void SetLastSequence(SequenceNumber s) {
		assert (s.value >= last_sequence_.value);
		last_sequence_ = s;
	}

	// Mark the specified file number as used.
	public void MarkFileNumberUsed(long number) {
		if (next_file_number_ <= number) {
			next_file_number_ = number + 1;
		}
	}

	// Return the current log file number.
	public long LogNumber() {
		return log_number_;
	}

	// Return the log file number for the log file that is currently
	// being compacted, or zero if there is no such log file.
	public long PrevLogNumber() {
		return prev_log_number_;
	}

	/**
	 * Pick level and inputs for a new compaction. Returns NULL if there is no
	 * compaction to be done. Otherwise returns a pointer to a heap-allocated
	 * object that describes the compaction. Caller should delete the result.
	 * 
	 * <li>what is current_.compaction_score_?</li> <li>what is
	 * current_.file_to_compact_, current_.file_to_compact_level_?</li> <li>what
	 * is current_.compaction_level_?</li> <li>what is compact_pointer_[level]?</li>
	 * 
	 * @return
	 */
	public Compaction PickCompaction() {
		Compaction c;
		int level;

		// We prefer compactions triggered by too much data in a level over
		// the compactions triggered by seeks.
		boolean size_compaction = (current_.compaction_score_ >= 1);
		boolean seek_compaction = (current_.file_to_compact_ != null);
		if (size_compaction) {
			level = current_.compaction_level_;
			assert (level >= 0);
			assert (level + 1 < config.kNumLevels);
			c = new Compaction(level);

			// Pick the first file that comes after compact_pointer_[level]
			for (int i = 0; i < current_.files_.get(level).size(); i++) {
				FileMetaData f = current_.files_.get(level).get(i);
				if (compact_pointer_[level] == ""
						|| icmp_.Compare(f.largest.Encode(), new Slice(
								compact_pointer_[level])) > 0) {
					c.inputs_.get(0).add(f);
					break;
				}
			}
			if (c.inputs_.get(0).isEmpty()) {
				// Wrap-around to the beginning of the key space
				c.inputs_.get(0).add(current_.files_.get(level).get(0));
			}
		} else if (seek_compaction) {
			level = current_.file_to_compact_level_;
			c = new Compaction(level);
			c.inputs_.get(0).add(current_.file_to_compact_);
		} else {
			return null;
		}

		c.input_version_ = current_;
		c.input_version_.Ref();

		// Files in level 0 may overlap each other, so pick up all overlapping
		// ones
		if (level == 0) {
			InternalKey smallest = new InternalKey();
			InternalKey largest = new InternalKey();
			GetRange(c.inputs_.get(0), smallest, largest);
			// Note that the next call will discard the file we placed in
			// c->inputs_[0] earlier and replace it with an overlapping set
			// which will include the picked file.
			current_.GetOverlappingInputs(0, smallest, largest,
					c.inputs_.get(0));
			assert (!c.inputs_.get(0).isEmpty());
		}

		// set files for "level + 1"
		SetupOtherInputs(c);

		return c;
	}

	/**
	 * Return a compaction object for compacting the range [begin,end] in the
	 * specified level. Returns NULL if there is nothing in that level that
	 * overlaps the specified range. Caller should delete the result.
	 * 
	 * @param level
	 *            for compaction @ this level
	 * @param begin
	 *            begin InternalKey
	 * @param end
	 * @return a compaction for compacting [begin, end] @ level, <li>step1:
	 *         input set as a list of files @ level keys \in [begin, end];</li>
	 *         <li>step2: expand by calling <code>SetupOtherInput</code></li>
	 */
	public Compaction CompactRange(int level, InternalKey begin, InternalKey end) {
		List<FileMetaData> inputs = new ArrayList<FileMetaData>();
		current_.GetOverlappingInputs(level, begin, end, inputs);
		if (inputs.isEmpty()) {
			return null;
		}

		// Avoid compacting too much in one shot in case the range is large.
		long limit = Version.MaxFileSizeForLevel(level);
		long total = 0;
		for (int i = 0; i < inputs.size(); i++) {
			long s = inputs.get(i).file_size;
			total += s;
			if (total >= limit) {
				// inputs.resize(i + 1);
				break;
			}
		}

		Compaction c = new Compaction(level);
		c.input_version_ = current_;
		c.input_version_.Ref();
		c.inputs_.set(0, inputs);
		// might need to construct :
		c.inputs_.set(1, new ArrayList<FileMetaData>());
		// expend
		SetupOtherInputs(c);
		return c;
	}

	// Return the maximum overlapping data (in bytes) at next level for any
	// file at a level >= 1.
	public long MaxNextLevelOverlappingBytes() {
		return -1;
	}

	/**
	 * Create an iterator that reads over the compaction inputs for "c". The
	 * caller should delete the iterator when no longer needed. logic here is
	 * similar as <code>Version AddIterators(...</code>
	 * 
	 * @param c
	 * @return
	 */
	public Iterator MakeInputIterator(Compaction c) {
		ReadOptions options = new ReadOptions();
		options.verify_checksums = options_.paranoid_checks;
		options.fill_cache = false;

		// Level-0 files have to be merged together. For other levels,
		// we will make a concatenating iterator per level.
		// TODO(opt): use concatenating iterator for level-0 if there is no
		// overlap
		int space = (c.level() == 0 ? c.inputs_.get(0).size() + 1 : 2);
		Iterator[] list = new Iterator[space];
		int num = 0;
		for (int which = 0; which < 2; which++) {
			if (!c.inputs_.get(which).isEmpty()) {
				if (c.level() + which == 0) {
					List<FileMetaData> files = c.inputs_.get(which);
					for (int i = 0; i < files.size(); i++) {
						list[num++] = table_cache_.NewIterator(options,
								files.get(i).number, files.get(i).file_size,
								null);
					}
				} else {
					// Create concatenating iterator for the files from this
					// level
					// similar as Version.NewConcatenatingIterator(ReadOptions,
					// int)
					list[num++] = TwoLevelIterator.NewTwoLevelIterator(
							new Version.LevelFileNumIterator(icmp_, c.inputs_
									.get(which)),
							new Version.GetFileIteratorBlockFunction(),
							table_cache_, options);
				}
			}
		}
		assert (num <= space);
		Iterator result = MergingIterator.NewMergingIterator(icmp_, list, num);

		return result;
	}

	// Returns true iff some level needs a compaction.
	public boolean NeedsCompaction() {
		Version v = current_;
		return (v.compaction_score_ >= 1) || (v.file_to_compact_ != null);
	}

	// Add all files listed in any live version to *live.
	// May also mutate some internal state.
	// wlu, 2012-7-7, this is ridiculous because this function is left as empty
	// before, shit!
	public void AddLiveFiles(Set<Long> live) {
		for (Version v = dummy_versions_.next_; v != dummy_versions_; v = v.next_) {
			for (int level = 0; level < config.kNumLevels; level++) {
				List<FileMetaData> files = v.files_.get(level);
				for (int i = 0; i < files.size(); i++) {
					live.add(files.get(i).number);
				}
			}
		}
	}

	// / wlu: 2012-6-2, not test it yet
	// Return the approximate offset in the database of the data for
	// "key" as of version "v".
	public long ApproximateOffsetOf(Version v, InternalKey ikey) {
		long result = 0;
		for (int level = 0; level < config.kNumLevels; level++) {
			List<FileMetaData> files = v.files_.get(level);
			for (int i = 0; i < files.size(); i++) {
				if (icmp_.Compare(files.get(i).largest, ikey) <= 0) {
					// Entire file is before "ikey", so just add the file size
					result += files.get(i).file_size;
				} else if (icmp_.Compare(files.get(i).smallest, ikey) > 0) {
					// Entire file is after "ikey", so ignore
					if (level > 0) {
						// Files other than level 0 are sorted by
						// meta->smallest, so
						// no further files in this level will contain data for
						// "ikey".
						break;
					}
				} else {
					// "ikey" falls in the range for this table. Add the
					// approximate offset of "ikey" within the table.
					Table tableptr[] = new Table[1];
					table_cache_.NewIterator(new ReadOptions(),
							files.get(i).number, files.get(i).file_size,
							tableptr);
					if (tableptr != null) {
						result += tableptr[0]
								.ApproximateOffsetOf(ikey.Encode());
					}
				}
			}
		}
		return result;
	}

	// Return a human-readable short (single-line) summary of the number
	// of files per level. Uses *scratch as backing store.
	public static class LevelSummaryStorage {
		String buffer;
	}

	/**
	 * Return file size @ each level
	 * 
	 * @param scratch
	 * @return
	 */
	public String LevelSummary(LevelSummaryStorage scratch) {
		// Update code if kNumLevels changes
		assert (config.kNumLevels == 7);
		scratch.buffer = "files[ " + (current_.files_.get(0).size()) + " "
				+ (current_.files_.get(0).size()) + " "
				+ (current_.files_.get(0).size()) + " "
				+ (current_.files_.get(0).size()) + " "
				+ (current_.files_.get(0).size()) + " "
				+ (current_.files_.get(0).size()) + " "
				+ (current_.files_.get(0).size()) + "]";
		return scratch.buffer;
	}

	static double MaxBytesForLevel(int level) {
		// Note: the result for level zero is not really used since we set
		// the level-0 compaction threshold based on number of files.
		double result = 10 * 1048576.0; // Result for both level-0 and level-1
		while (level > 1) {
			result *= 10;
			level--;
		}
		return result;
	}

	private void Finalize(Version v) {
		// Precomputed best level for next compaction
		int best_level = -1;
		double best_score = -1;

		for (int level = 0; level < config.kNumLevels - 1; level++) {
			double score;
			if (level == 0) {
				// We treat level-0 specially by bounding the number of files
				// instead of number of bytes for two reasons:
				//
				// (1) With larger write-buffer sizes, it is nice not to do too
				// many level-0 compactions.
				//
				// (2) The files in level-0 are merged on every read and
				// therefore we wish to avoid too many files when the individual
				// file size is small (perhaps because of a small write-buffer
				// setting, or very high compression ratios, or lots of
				// overwrites/deletions).
				score = (double) v.files_.get(level).size()
						/ (double) (config.kL0_CompactionTrigger);
			} else {
				// Compute the ratio of current size to size limit.
				long level_bytes = TotalFileSize(v.files_.get(level));
				score = (double) (level_bytes)
						/ (double) MaxBytesForLevel(level);
			}

			if (score > best_score) {
				best_level = level;
				best_score = score;
			}
		}

		v.compaction_level_ = best_level;
		v.compaction_score_ = best_score;
	}

	/**
	 * Stores the minimal range that covers all entries in inputs in smallest,
	 * largest.
	 * 
	 * REQUIRES: inputs is not empty, smallest/largest not null
	 * 
	 * @param inputs
	 *            get range from this <code>List</code>
	 * @param smallest
	 * @return
	 * @param largest
	 * @return
	 */
	private void GetRange(List<FileMetaData> inputs, InternalKey smallest,
			InternalKey largest) {
		assert (!inputs.isEmpty());
		smallest.Clear();
		largest.Clear();
		for (int i = 0; i < inputs.size(); i++) {
			FileMetaData f = inputs.get(i);
			if (i == 0) {
				smallest.InternalKey_(f.smallest);
				largest.InternalKey_(f.largest);
			} else {
				if (icmp_.Compare(f.smallest, smallest) < 0) {
					smallest.InternalKey_(f.smallest);
				}
				if (icmp_.Compare(f.largest, largest) > 0) {
					largest.InternalKey_(f.largest);
				}
			}
		}
	}

	/**
	 * Stores the minimal range that covers all entries in inputs1 and inputs2
	 * in smallest, largest. REQUIRES: inputs is not empty Combine 2 inputs,
	 * then call GetRange
	 * 
	 * @param inputs1
	 * @param inputs2
	 * @param smallest
	 * @param largest
	 */
	private void GetRange2(List<FileMetaData> inputs1,
			List<FileMetaData> inputs2, InternalKey smallest,
			InternalKey largest) {
		// wlu, 2012-7-8, bugfix: Again! cann0t assign inputs1 to all and then
		// addall inputs2.
		// reference is bad here!!!
		List<FileMetaData> all = new ArrayList<FileMetaData>();
		all.addAll(inputs1);
		all.addAll(inputs2);
		GetRange(all, smallest, largest);
	}

	/**
	 * Get files for both level and level + 1 for compaction <li>get overlaps
	 * from "level + 1"</li> <li>get whole range and reversely pre-get a
	 * expended @ level</li> <li>See if we can grow the number of inputs in
	 * pre-get "level" without changing the number of "level+1" files we pick
	 * up, if we can, we will expend level; else, don't expend.</li>
	 * 
	 * @param c
	 */
	void SetupOtherInputs(Compaction c) {
		int level = c.level();
		InternalKey smallest = new InternalKey();
		InternalKey largest = new InternalKey();
		GetRange(c.inputs_.get(0), smallest, largest);
		// get overlaps from upper level
		current_.GetOverlappingInputs(level + 1, smallest, largest,
				c.inputs_.get(1));

		// Get entire range covered by compaction
		InternalKey all_start = new InternalKey();
		InternalKey all_limit = new InternalKey();
		// range may be larger than [smallest, largest]
		GetRange2(c.inputs_.get(0), c.inputs_.get(1), all_start, all_limit);

		// See if we can grow the number of inputs in "level" without
		// changing the number of "level+1" files we pick up.
		if (!c.inputs_.get(1).isEmpty()) {
			List<FileMetaData> expanded0 = new ArrayList<FileMetaData>();
			// get files according to expended range
			current_.GetOverlappingInputs(level, all_start, all_limit,
					expanded0);
			long inputs0_size = Version.TotalFileSize(c.inputs_.get(0));
			long inputs1_size = Version.TotalFileSize(c.inputs_.get(1));
			long expanded0_size = Version.TotalFileSize(expanded0);

			if (expanded0.size() > c.inputs_.get(0).size()
					&& inputs1_size + expanded0_size < Version.kExpandedCompactionByteSizeLimit) {
				InternalKey new_start = new InternalKey();
				InternalKey new_limit = new InternalKey();
				GetRange(expanded0, new_start, new_limit);
				List<FileMetaData> expanded1 = new ArrayList<FileMetaData>();
				current_.GetOverlappingInputs(level + 1, new_start, new_limit,
						expanded1);
				// So, we CAN grow # @ level without changing # @ level + 1
				if (expanded1.size() == c.inputs_.get(1).size()) {
					LOG.info("Expanding@" + level + " "
							+ c.inputs_.get(0).size() + " + "
							+ c.inputs_.get(1).size() + " (" + inputs0_size
							+ " + " + inputs1_size + "bytes) to "
							+ expanded0.size() + " + " + expanded1.size()
							+ " ( " + expanded0_size + "+ " + inputs1_size
							+ " bytes)\n");
					smallest = new_start;
					largest = new_limit;
					c.inputs_.set(0, expanded0);
					c.inputs_.set(1, expanded1);
					GetRange2(c.inputs_.get(0), c.inputs_.get(1), all_start,
							all_limit);
				}
			}
		}

		// Compute the set of grandparent files that overlap this compaction
		// (parent == level+1; grandparent == level+2)
		if (level + 2 < config.kNumLevels) {
			current_.GetOverlappingInputs(level + 2, all_start, all_limit,
					c.grandparents_);
		}

		LOG.debug("Compacting " + level + " '" + smallest.DebugString()
				+ "' .. '" + largest.DebugString() + "'");

		// Update the place where we will do the next compaction for this level.
		// We update this immediately instead of waiting for the VersionEdit
		// to be applied so that if the compaction fails, we will try a
		// different key range next time.
		compact_pointer_[level] = largest.Encode().toString();
		c.edit_.SetCompactPointer(level, largest);
	}

	// Save current contents to *log
	// TODO
	Status WriteSnapshot(Writer log) {
		// TODO: Break up into multiple records to reduce memory usage on
		// recovery?

		// Save metadata
		VersionEdit edit = new VersionEdit();
		edit.SetComparatorName(new Slice(icmp_.user_comparator().Name()));

		// Save compaction pointers
		for (int level = 0; level < config.kNumLevels; level++) {
			if (!compact_pointer_[level].isEmpty()) {
				InternalKey key = new InternalKey();
				key.DecodeFrom(new Slice(compact_pointer_[level]));
				edit.SetCompactPointer(level, key);
			}
		}

		// Save files
		for (int level = 0; level < config.kNumLevels; level++) {
			List<FileMetaData> files = current_.files_.get(level);
			for (int i = 0; i < files.size(); i++) {
				FileMetaData f = files.get(i);
				edit.AddFile(level, f.number, f.file_size, f.smallest,
						f.largest);
			}
		}

		byte[] record = edit.EncodeTo();
		return log.AddRecord(new Slice(record));
	}

	void AppendVersion(Version v) {
		// Make "v" current
		assert (v.refs_ == 0);
		assert (v != current_);
		if (current_ != null) {
			current_.Unref();
		}
		current_ = v;
		v.Ref();

		// Append to linked list
		v.prev_ = dummy_versions_.prev_;
		v.next_ = dummy_versions_;
		v.prev_.next_ = v;
		v.next_.prev_ = v;
	}

	Env env_;
	String dbname_;
	Options options_;
	TableCache table_cache_;
	public InternalKeyComparator icmp_;
	long next_file_number_;
	long manifest_file_number_;
	SequenceNumber last_sequence_;
	long log_number_;
	long prev_log_number_; // 0 or backing store for memtable being compacted

	// Opened lazily
	_WritableFile descriptor_file_;
	Writer descriptor_log_;
	Version dummy_versions_; // Head of circular doubly-linked list of versions.
	Version current_; // == dummy_versions_.prev_

	// Per-level key at which the next compaction at that level should start.
	// Either an empty string, or a valid InternalKey.
	String compact_pointer_[] = new String[config.kNumLevels];

	// No copying allowed

}
