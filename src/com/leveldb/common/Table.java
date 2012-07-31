package com.leveldb.common;

import com.leveldb.common.file._RandomAccessFile;
import com.leveldb.common.options.Options;
import com.leveldb.common.options.ReadOptions;
import com.leveldb.common.table.Block;
import com.leveldb.common.table.BlockHandle;
import com.leveldb.common.table.Footer;
import com.leveldb.common.table.TwoLevelIterator;
import com.leveldb.util.util;

//2012-4-10 implement Table
public class Table {

	class TableBlockReader implements TwoLevelIterator.BlockFunction {

		class ReleaseBlockFunction implements Function {
			@Override
			public void exec(Object... args) {
				try {
					Cache cache = (Cache) (args[0]);
					Cache.Handle handle = (Cache.Handle) (args[1]);
					cache.Release(handle);
				} catch (ArrayIndexOutOfBoundsException e) {
					e.printStackTrace();
				}
			}

		}

		/**
		 * Here a implement of exec will return an iterator of a #Block#; The
		 * #Block# is get according to input paramenter #index_value#, which
		 * contains a BlockHandler#{offset, size}#. If cache is applied: key in
		 * the cache is {table's cache_id, block's offset}, value is the
		 * #Block#. return iterator of Block at the specific #{offset, size}# in
		 * the table file
		 */
		@Override
		public Iterator exec(Object arg, ReadOptions options, Slice index_value) {
			Table table = (Table) (arg);
			Cache block_cache = table.rep_.options.block_cache;
			Block block = new Block(null, 0, false); // is to be reassigned or
														// value be set
			Cache.Handle cache_handle = null;

			BlockHandle handle = new BlockHandle();
			// Slice input = index_value;
			ByteCollection input = new ByteCollection(index_value.data(), 0);
			int s = handle.DecodeFrom(input); // set handle's offset and size
			// We intentionally allow extra stuff in index_value so that we
			// can add more features in the future.

			if (s != 0) {
				boolean may_cache;
				// with cache
				if (block_cache != null) {
					// create a key by cache_id|offset, corresponding value is a
					// Block
					byte cache_key_buffer[] = new byte[16];
					util.putLong(cache_key_buffer, 0, table.rep_.cache_id);
					util.putLong(cache_key_buffer, 8, handle.offset());
					// !!! key in the cache is {table's cache_id, block's
					// offset}
					Slice key = new Slice(cache_key_buffer);
					// look up the key in the cache
					cache_handle = block_cache.Lookup(key);
					// in the cache, just return
					if (cache_handle != null) {
						block = (Block) (block_cache.Value(cache_handle));
					}
					// not in the cache, read the block and insert to the cache
					else {
						try {
							// !!! read (set) the block from the file according
							// to the #handle#,
							// which is a BlockHandle {offset, size}.
							may_cache = Block.ReadBlock(table.rep_.file,
									options, handle, block);
							if (may_cache && options.fill_cache) {
								cache_handle = block_cache.Insert(key, block,
										block.size(), null); // DeleteCachedBlock
							}
						} catch (Exception e) {
							e.printStackTrace();
						}

					}
				}
				// without cache
				else {
					try {
						may_cache = Block.ReadBlock(table.rep_.file, options,
								handle, block);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}

			Iterator iter;
			if (block != null) {
				// !!! return a iterator of Block
				iter = block.NewIterator(table.rep_.options.comparator);
				if (cache_handle == null) {
					iter.RegisterCleanup(null, block, null); // no need
				}
				// release the handle from the cache
				else {
					iter.RegisterCleanup(new ReleaseBlockFunction(),
							block_cache, cache_handle);
				}
			} else {
				iter = Iterator.NewErrorIterator(Status.IOError(new Slice(
						"block null"), null));
			}
			return iter;
		}
	}

	// information package for the table
	public static class Rep {
		Options options;
		Status status;
		_RandomAccessFile file;
		long cache_id;

		BlockHandle metaindex_handle; // Handle to metaindex_block: saved from
										// footer
		Block index_block;
	}

	/**
	 * Attempt to open the table that is stored in bytes [0..file_size) of
	 * "file", and read the metadata entries necessary to allow retrieving data
	 * from the table.
	 * 
	 * If successful, returns ok and sets "*table" to the newly opened table.
	 * The client should delete "*table" when no longer needed. If there was an
	 * error while initializing the table, sets "*table" to NULL and returns a
	 * non-ok status. Does not take ownership of "*source", but the client must
	 * ensure that "source" remains live for the duration of the returned
	 * table's lifetime.
	 * 
	 * file must remain live while this Table is in use.
	 **/
	public static Table Open(Options options, _RandomAccessFile file, long size) {
		Table table = null;
		if (size < Footer.kEncodedLength) {
			// throw new Exception("file is too short to be an sstable");
			return null;
		}

		byte[] footer_space = new byte[Footer.kEncodedLength];
		Slice footer_input = new Slice();
		footer_space = file.Read(size - Footer.kEncodedLength,
				Footer.kEncodedLength, footer_input);
		ByteCollection footer_input_bc = new ByteCollection(footer_space, 0);

		Footer footer = new Footer();
		Status s = footer.DecodeFrom(footer_input_bc);
		if (!s.ok())
			return null;

		// Read the index block
		Block index_block = new Block(null, 0, false);
		if (s.ok()) {
			@SuppressWarnings("unused")
			boolean may_cache = false; // Ignored result
			try {
				may_cache = Block.ReadBlock(file, new ReadOptions(),
						footer.index_handle(), index_block);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		if (s.ok()) {
			// We've successfully read the footer and the index block: we're
			// ready to serve requests.
			Rep rep = new Rep();
			rep.options = options;
			rep.file = file;
			rep.metaindex_handle = footer.metaindex_handle();
			rep.index_block = index_block;
			rep.cache_id = (options.block_cache != null ? options.block_cache
					.NewId() : 0);
			table = new Table(rep);
		} else {
			if (index_block != null)
				index_block = null;
		}
		return table;
	}

	/**
	 * Returns a new iterator over the table contents. The result of
	 * NewIterator() is initially invalid (caller must call one of the Seek
	 * methods on the iterator before using it).
	 * 
	 * outer iterator is index_block.iterator BlockFunction here will read
	 **/
	public Iterator NewIterator(ReadOptions iReadOpt) {
		return TwoLevelIterator.NewTwoLevelIterator(
				rep_.index_block.NewIterator(rep_.options.comparator), /*outer iterator with value BlockHandle*/
				new TableBlockReader(), this, iReadOpt);
	}

	/**
	 * Given a key, return an approximate byte offset in the file where the data
	 * for that key begins (or would begin if the key were present in the file).
	 * The returned value is in terms of file bytes, and so includes effects
	 * like compression of the underlying data. E.g., the approximate offset of
	 * the last key in the table will be close to the file length.
	 **/
	public long ApproximateOffsetOf(Slice key) {
		// index_iter is a coarse iterator, BlockHandler {offset, size}
		Iterator index_iter = rep_.index_block
				.NewIterator(rep_.options.comparator);
		index_iter.Seek(key);
		long result;
		if (index_iter.Valid()) {
			BlockHandle handle = new BlockHandle();
			ByteCollection input = new ByteCollection(
					index_iter.value().data(), 0);
			int s = handle.DecodeFrom(input);
			if (s != 0) {
				result = handle.offset();
			} else {
				// Strange: we can't decode the block handle in the index block.
				// We'll just return the offset of the metaindex block, which is
				// close to the whole file size for this case.
				result = rep_.metaindex_handle.offset();
			}
		} else {
			// key is past the last key in the file. Approximate the offset
			// by returning the offset of the metaindex block (which is
			// right near the end of the file).
			result = rep_.metaindex_handle.offset();
		}
		index_iter = null;
		return result;
	}

	private Rep rep_;

	public Table(Rep rep) {
		rep_ = rep;
	}

	// No copying allowed
}
