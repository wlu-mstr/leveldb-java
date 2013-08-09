package com.leveldb.common.table;

import com.leveldb.common.Slice;
import com.leveldb.common.Status;
import com.leveldb.common.file._WritableFile;
import com.leveldb.common.options.CompressionType;
import com.leveldb.common.options.Options;
import com.leveldb.util.crc32java;
import com.leveldb.util.util;

import de.jarnbjo.jsnappy.SnappyCompressor;

public class TableBuilder {
	// Create a builder that will store the contents of the table it is
	// building in file. Does not close the file. It is up to the
	// caller to close the file after calling Finish().
	public TableBuilder(Options options, _WritableFile file) {
		rep_ = new Rep(options, file);
	}

	// REQUIRES: Either Finish() or Abandon() has been called.
	// ~TableBuilder();

	// Change the options used by this builder. Note: only some of the
	// option fields can be changed after construction. If a field is
	// not allowed to change dynamically and its value in the structure
	// passed to the constructor is different from its value in the
	// structure passed to this method, this method will return an error
	// without changing any fields.
	public Status ChangeOptions(Options options) {
		// Note: if more fields are added to Options, update
		// this function to catch changes that should not be allowed to
		// change in the middle of building a Table.
		if (options.comparator != rep_.options.comparator) {
			return Status.InvalidArgument(new Slice(
					"changing comparator while building table"), null);
		}

		// Note that any live BlockBuilders point to rep_->options and therefore
		// will automatically pick up the updated options.
		rep_.options = options;
		rep_.index_block_options = options;
		rep_.index_block_options.block_restart_interval = 1;
		return Status.OK();

	}

	// Add key,value to the table being constructed.
	// REQUIRES: key is after any previously added key according to comparator.
	// REQUIRES: Finish(), Abandon() have not been called
	public void Add(Slice key, Slice value) {
		assert (!rep_.closed);
		if (!ok())
			return;
		if (rep_.num_entries > 0) {
			assert (rep_.options.comparator.Compare(key, new Slice(
					rep_.last_key)) > 0);
		}

		if (rep_.pending_index_entry) {
			assert (rep_.data_block.empty());
			rep_.last_key = rep_.options.comparator.FindShortestSeparator(rep_.last_key, key);
			byte[] handle_encoding = rep_.pending_handle.EncodeTo();
			rep_.index_block.Add(new Slice(rep_.last_key), new Slice(
					handle_encoding));
			rep_.pending_index_entry = false;
		}

		// r.last_key.assign(key.data(), key.size());
		rep_.last_key = key.data();
		rep_.num_entries++;
		rep_.data_block.Add(key, value);

		int estimated_block_size = rep_.data_block.CurrentSizeEstimate();
		if (estimated_block_size >= rep_.options.block_size) {
			Flush();
		}

	}

	// Advanced operation: flush any buffered key/value pairs to file.
	// Can be used to ensure that two adjacent entries never live in
	// the same data block. Most clients should not need to use this method.
	// REQUIRES: Finish(), Abandon() have not been called
	public void Flush() {
		assert (!rep_.closed);
		if (!ok())
			return;
		if (rep_.data_block.empty())
			return;
		assert (!rep_.pending_index_entry);
		WriteBlock(rep_.data_block, rep_.pending_handle);
		if (ok()) {
			rep_.pending_index_entry = true;
			rep_.status = rep_.file.Flush();
		}

	}

	// Return non-ok iff some error has been detected.
	public Status status() {
		return rep_.status;

	}

	// Finish building the table. Stops using the file passed to the
	// constructor after this function returns.
	// REQUIRES: Finish(), Abandon() have not been called
	public Status Finish() {
		Flush();
		assert (!rep_.closed);
		rep_.closed = true;
		BlockHandle metaindex_block_handle = new BlockHandle(); // dummy
																// currenly
		BlockHandle index_block_handle = new BlockHandle(); // TODO
		if (ok()) {
			BlockBuilder meta_index_block = new BlockBuilder(rep_.options);
			// TODO(postrelease): Add stats and other meta blocks
			WriteBlock(meta_index_block, metaindex_block_handle);
		}
		if (ok()) {
			if (rep_.pending_index_entry) {
				rep_.last_key = rep_.options.comparator.FindShortSuccessor(rep_.last_key);
				byte[] handle_encoding = rep_.pending_handle.EncodeTo();
				rep_.index_block.Add(new Slice(rep_.last_key), new Slice(
						handle_encoding));
				rep_.pending_index_entry = false;
			}
			WriteBlock(rep_.index_block, index_block_handle);
		}
		if (ok()) {
			Footer footer = new Footer();
			footer.set_metaindex_handle(metaindex_block_handle);
			footer.set_index_handle(index_block_handle);
			byte[] footer_encoding = new byte[0];
			footer_encoding = footer.EncodeTo(footer_encoding);
			rep_.status = rep_.file.Append(new Slice(footer_encoding));
			if (rep_.status.ok()) {
				rep_.offset += footer_encoding.length;
			}
		}
		return rep_.status;

	}

	// Indicate that the contents of this builder should be abandoned. Stops
	// using the file passed to the constructor after this function returns.
	// If the caller is not going to call Finish(), it must call Abandon()
	// before destroying this builder.
	// REQUIRES: Finish(), Abandon() have not been called
	public void Abandon() {
		assert (!rep_.closed);
		rep_.closed = true;

	}

	// Number of calls to Add() so far.
	public 	long NumEntries() {
		return rep_.num_entries;

	}

	// Size of the file generated so far. If invoked after a successful
	// Finish() call, returns the size of the final generated file.
	public long FileSize() {
		return rep_.offset;

	}

	private boolean ok() {
		return status().ok();
	}

	void WriteBlock(BlockBuilder block, BlockHandle handle) {
		// File format contains a sequence of blocks where each block has:
		// block_data: uint8[n]
		// type: uint8---
		// --------------|-- trailer
		// crc: uint32---
		assert (ok());
		Rep r = rep_;
		Slice raw = block.Finish();

		Slice block_contents = new Slice();
		CompressionType type = r.options.compression;
		// TODO(postrelease): Support more compression options: zlib?
		switch (type.value) {
		case CompressionType.kNoCompression:
			block_contents = raw;
			break;

		/*case CompressionType.kSnappyCompression: {
			r.compressed_output = SnappyCompressor.compress(raw.data(), 0,
					raw.size()).toByteArray();
			if (r.compressed_output != null
					&& r.compressed_output.length < raw.size() - (raw.size())
							/ 8) {
				block_contents = new Slice(r.compressed_output);
			} else {
				// Snappy not supported, or compressed less than 12.5%, so just
				// store uncompressed form
				block_contents = raw;
				type.value = CompressionType.kNoCompression;
			}
			break;
		}*/
		}
		handle.set_offset(r.offset);
		handle.set_size(block_contents.size());
		r.status = r.file.Append(block_contents);

		// deal with trailer
		if (r.status.ok()) {
			// trailer: {compress type(1); crc result(4)}
			byte trailer[] = new byte[Footer.kBlockTrailerSize];
			// compress
			trailer[0] = type.value;
			// crc
			crc32java crc32 = new crc32java();
			int crc = crc32.Value(block_contents.data(), block_contents.size());
			crc = crc32.Extend(crc, trailer, 1); // Extend crc to cover block
													// type
			//
			util.putInt(trailer, 1, crc32java.Mask(crc));
			r.status = r.file.Append(new Slice(trailer,
					Footer.kBlockTrailerSize));
			if (r.status.ok()) {
				r.offset += block_contents.size() + Footer.kBlockTrailerSize;
			}
		}

		r.compressed_output = null;
		block.Reset();
	}

	class Rep {
		Options options;
		Options index_block_options;
		_WritableFile file;
		long offset;
		Status status;
		BlockBuilder data_block;
		BlockBuilder index_block;
		byte[] last_key;
		long num_entries;
		boolean closed; // Either Finish() or Abandon() has been called.

		// We do not emit the index entry for a block until we have seen the
		// first key for the next data block. This allows us to use shorter
		// keys in the index block. For example, consider a block boundary
		// between the keys "the quick brown fox" and "the who". We can use
		// "the r" as the key for the index block entry since it is >= all
		// entries in the first block and < all entries in subsequent
		// blocks.
		//
		// Invariant: r->pending_index_entry is true only if data_block is
		// empty.
		boolean pending_index_entry;
		BlockHandle pending_handle = new BlockHandle(); // Handle to add to index block

		byte[] compressed_output;

		public Rep(Options opt, _WritableFile f) {
			options = opt;
			index_block_options = opt;
			file = f;
			offset = 0;
			data_block = new BlockBuilder(options);
			index_block = new BlockBuilder(index_block_options);
			num_entries = 0;
			closed = false;
			pending_index_entry = false;
			index_block_options.block_restart_interval = 1;
			// wlu 2012-5-10
			status = Status.OK();
		}
	}

	private Rep rep_;

}