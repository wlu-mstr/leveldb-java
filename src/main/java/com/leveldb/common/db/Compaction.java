package com.leveldb.common.db;

import java.util.ArrayList;
import java.util.List;

import com.leveldb.common.Comparator;
import com.leveldb.common.Slice;
import com.leveldb.common.config;
import com.leveldb.common.comparator.InternalKeyComparator;
import com.leveldb.common.version.Version;
import com.leveldb.common.version.VersionEdit;

// TODO
public class Compaction {

	// Return the level that is being compacted. Inputs from "level"
	// and "level+1" will be merged to produce a set of "level+1" files.
	public int level() {
		return level_;
	}

	// Return the object that holds the edits to the descriptor done
	// by this compaction.
	VersionEdit edit() {
		return edit_;
	}

	// "which" must be either 0 or 1
	int num_input_files(int which) {
		return inputs_.get(which).size();
	}

	// Return the ith input file at "level()+which" ("which" must be 0 or 1).
	FileMetaData input(int which, int i) {
		return inputs_.get(which).get(i);
	}

	// Maximum size of files to build during this compaction.
	long MaxOutputFileSize() {
		return max_output_file_size_;
	}

	/**
	 * Is this a trivial compaction that can be implemented by just moving a
	 * single input file to the next level (no merging or splitting) <br>
	 * </br> Avoid a move if there is lots of overlapping grandparent data.
	 * Otherwise, the move could create a parent file that will require a very
	 * expensive merge later on.
	 */
	boolean IsTrivialMove() {
		return (num_input_files(0) == 1 && num_input_files(1) == 0 && Version
				.TotalFileSize(grandparents_) <= Version.kMaxGrandParentOverlapBytes);
	}

	// Add all inputs to this compaction as delete operations to edit.
	void AddInputDeletions(VersionEdit edit) {
		for (int which = 0; which < 2; which++) {
			for (int i = 0; i < inputs_.get(which).size(); i++) {
				edit.DeleteFile(level_ + which,
						inputs_.get(which).get(i).number);
			}
		}
	}

	// Returns true if the information we have available guarantees that
	// the compaction is producing data in "level+1" for which no data exists
	// in levels greater than "level+1".
	boolean IsBaseLevelForKey(Slice user_key) {
		// Maybe use binary search to find right entry instead of linear search?
		Comparator user_cmp = input_version_.vset_.icmp_.user_comparator();
		for (int lvl = level_ + 2; lvl < config.kNumLevels; lvl++) {
			List<FileMetaData> files = input_version_.files_.get(lvl);
			for (; level_ptrs_[lvl] < files.size();) {
				FileMetaData f = files.get(level_ptrs_[lvl]);
				if (user_cmp.Compare(user_key, f.largest.user_key()) <= 0) {
					// We've advanced far enough
					if (user_cmp.Compare(user_key, f.smallest.user_key()) >= 0) {
						// Key falls in this file's range, so definitely not
						// base level
						return false;
					}
					break;
				}
				level_ptrs_[lvl]++;
			}
		}
		return true;
	}

	// Returns true iff we should stop building the current output
	// before processing "internal_key".
	boolean ShouldStopBefore(Slice internal_key) {
		// Scan to find earliest grandparent file that contains key.
		InternalKeyComparator icmp = input_version_.vset_.icmp_;
		while (grandparent_index_ < grandparents_.size()
				&& icmp.Compare(internal_key,
						grandparents_.get(grandparent_index_).largest.Encode()) > 0) {
			if (seen_key_) {
				overlapped_bytes_ += grandparents_.get(grandparent_index_).file_size;
			}
			grandparent_index_++;
		}
		seen_key_ = true;

		if (overlapped_bytes_ > Version.kMaxGrandParentOverlapBytes) {
			// Too much overlap for current output; start new output
			overlapped_bytes_ = 0;
			return true;
		} else {
			return false;
		}
	}

	// Release the input version for the compaction, once the compaction
	// is successful.
	void ReleaseInputs() {
		if (input_version_ != null) {
			input_version_.Unref();
			input_version_ = null;
		}
	}

	// construction
	public Compaction(int level) {
		level_ = level;
		max_output_file_size_ = MaxFileSizeForLevel(level);
		input_version_ = null;
		grandparent_index_ = 0;
		seen_key_ = false;
		overlapped_bytes_ = 0;
		for (int i = 0; i < config.kNumLevels; i++) {
			level_ptrs_[i] = 0;
			inputs_.add(new ArrayList<FileMetaData>());
			grandparents_.add(new FileMetaData());
		}
	}

	private long MaxFileSizeForLevel(int level) {
		// We could vary per level to reduce number of files?
		return Version.kTargetFileSize;
	}

	int level_;
	long max_output_file_size_;
	public Version input_version_;
	public VersionEdit edit_ = new VersionEdit();

	// Each compaction reads inputs from "level_" and "level_+1"
	// The two sets of inputs
	public List<List<FileMetaData>> inputs_ = new ArrayList<List<FileMetaData>>();
	// State used to check for number of of overlapping grandparent files
	// (parent == level_ + 1, grandparent == level_ + 2)
	public List<FileMetaData> grandparents_ = new ArrayList<FileMetaData>();
	public int grandparent_index_; // Index in grandparent_starts_
	boolean seen_key_; // Some output key has been seen
	long overlapped_bytes_; // Bytes of overlap between current output
							// and grandparent files

	// State for implementing IsBaseLevelForKey

	// level_ptrs_ holds indices into input_version_.levels_: our state
	// is that we are positioned at one of the file ranges for each
	// higher level than the ones involved in this compaction (i.e. for
	// all L >= level_ + 2).
	public int level_ptrs_[] = new int[config.kNumLevels];
}