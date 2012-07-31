package com.leveldb.common.table;

import java.util.ArrayList;
import java.util.List;

import com.leveldb.common.ByteVector;
import com.leveldb.common.Slice;
import com.leveldb.common.options.Options;
import com.leveldb.util.coding;
import com.leveldb.util.util;

/**
 * 2012-4-11
 * @author wlu
 * Build a Block 
 */
public class BlockBuilder {
	public BlockBuilder(Options options) {
		options_ = options;
		restarts_ = new ArrayList<Integer>();
		counter_ = 0;
		finished_ = false;
		assert (options.block_restart_interval >= 1);
		restarts_.add(0); // First restart point is at offset 0
		// wlu, 2-12-5-10
		buffer_ = new ByteVector();
		last_key_ = new ByteVector(128);
	}

	// Reset the contents as if the BlockBuilder was just constructed.
	public void Reset() {
		buffer_.clear();
		restarts_.clear();
		restarts_.add(0); // First restart point is at offset 0
		counter_ = 0;
		finished_ = false;
		last_key_.clear();
	}

	// REQUIRES: Finish() has not been callled since the last call to Reset().
	// REQUIRES: key is larger than any previously added key
	// <shared><non_shared><value_size><non_shared_data><value_data>
	public void Add(Slice key, Slice value) {
		Slice last_key_piece = new Slice(last_key_.getData());
		assert (!finished_);
		assert (counter_ <= options_.block_restart_interval);
		assert (buffer_.empty() // No values yet?
		|| options_.comparator.Compare(key, last_key_piece) > 0);
		int shared = 0;
		if (counter_ < options_.block_restart_interval) {
			// See how much sharing to do with previous string
			int min_length = Math.min(last_key_piece.size(), key.size());
			while ((shared < min_length)
					&& (last_key_piece.get(shared) == key.get(shared))) {
				shared++;
			}
		} else {
			// Restart compression
			restarts_.add(buffer_.getSize());
			counter_ = 0;
		}
		int non_shared = key.size() - shared;

		// Add "<shared><non_shared><value_size>" to buffer_
		buffer_.append(coding.EncodeVarint32(shared));
		buffer_.append(coding.EncodeVarint32(non_shared));
		buffer_.append(coding.EncodeVarint32(value.size()));

		// Add string delta (non-shared part) to buffer_ followed by value
		buffer_.append(key.data(), shared, non_shared);
		buffer_.append(value.data());

		// Update state
		last_key_.resize(shared);
		last_key_.append(key.data(), shared, non_shared);
		assert (last_key_.compareTo(key.data()) == 0);
		counter_++;
	}

	// Finish building the block and return a slice that refers to the
	// block contents. The returned slice will remain valid for the
	// lifetime of this builder or until Reset() is called.
	/// wlu, 2012-5-28, change Varint32 coding to Fixed32coding
	public Slice Finish() {
		// Append restart array
		for (int i = 0; i < restarts_.size(); i++) {
			buffer_.append(util.toBytes(restarts_.get(i)));
		}

		buffer_.append(util.toBytes(restarts_.size()));
		finished_ = true;
		return new Slice(buffer_.getData());
	}

	// Returns an estimate of the current (uncompressed) size of the block
	// we are building.
	public int CurrentSizeEstimate() {
		return (buffer_.getSize() + /* Raw data buffer */
		restarts_.size() * util.SIZEOF_INT + /* Restart array */
		util.SIZEOF_INT); /* Restart array length */
	}

	// Return true iff no entries have been added since the last Reset()
	public boolean empty() {
		return buffer_.empty();
	}

	Options options_;
	ByteVector buffer_; // Destination buffer
	List<Integer> restarts_; // Restart points
	int counter_; // Number of entries emitted since restart
	boolean finished_; // Has Finish() been called?
	ByteVector last_key_;
}
