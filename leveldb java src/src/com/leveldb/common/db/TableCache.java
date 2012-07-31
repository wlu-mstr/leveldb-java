package com.leveldb.common.db;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.leveldb.common.Cache;
import com.leveldb.common.Env;
import com.leveldb.common.Function;
import com.leveldb.common.Iterator;
import com.leveldb.common.Slice;
import com.leveldb.common.Table;
import com.leveldb.common.file._RandomAccessFile;
import com.leveldb.common.file.filename;
import com.leveldb.common.options.Options;
import com.leveldb.common.options.ReadOptions;
import com.leveldb.util.util;

/**
 * 2012-4-9
 * 
 * @author wlu
 * 
 */
public class TableCache {

	Log LOG = LogFactory.getLog(TableCache.class);
	// file & table
	class TableAndFile {
		_RandomAccessFile file;
		Table table;
	}

	// delete both the table and file
	class DeleteTableAndFile implements Function {
		TableAndFile tandf;

		public DeleteTableAndFile(TableAndFile taf) {
			tandf = taf;
		}

		@Override
		public void exec(Object... args) {
			LOG.info("File closed in Cache delete Function: " + tandf.file.FileName() );
			tandf.file.Close();
			tandf.file = null;
			tandf.table = null;
		}
	}

	// release handle in the cache
	class UnrefEntryCacheAndHandle implements Function {
		Cache c;
		Cache.Handle h;

		public UnrefEntryCacheAndHandle(Cache ic, Cache.Handle ih) {
			c = ic;
			h = ih;
		}

		@Override
		public void exec(Object... args) {
			c.Release(h);
		}

	}

	TableCache(String dbname, Options options, int entries) {
		env_ = options.env;
		dbname_ = dbname;
		options_ = options;
		cache_ = Cache.NewLRUCache(entries);
	}
	
	public void Destroy(){
		cache_.Destroy();
	}

	/*
	 * Return an iterator for the specified file number (the corresponding file
	 * length must be exactly "file_size" bytes). If "tableptr" is non-NULL,
	 * also sets "tableptr" to point to the Table object underlying the
	 * returned iterator, or NULL if no Table object underlies the returned
	 * iterator. The returned "tableptr" object is owned by the cache and
	 * should not be deleted, and is valid for as long as the returned iterator
	 * is live.
	 * 
	 * inner Cache
	 * Key: filenumber
	 * Value: file & table 
	 * 
	 * @return iterator over the table
	 */
	public Iterator NewIterator(ReadOptions options, long file_number,
			long file_size, Table[] tableptr) {
		if (tableptr != null) {
			tableptr[0] = null;
		}

		byte[] buf = util.toBytes(file_number);
		Slice key = new Slice(buf);
		Cache.Handle handle = cache_.Lookup(key);
		if (handle == null) {
			String fname = filename.TableFileName(dbname_, file_number);
			_RandomAccessFile file = null;
			Table table = null;
			file = env_.NewRandomAccessFile(fname);

			table = Table.Open(options_, file, file_size);

			if (table == null) {
				file.Close();
				file = null;
				// We do not cache error results so that if the error is
				// transient,
				// or somebody repairs the file, we recover automatically.
				return Iterator.NewErrorIterator(null);
			}

			TableAndFile tf = new TableAndFile();
			tf.file = file;
			tf.table = table;
			Function DeleteEntry = new DeleteTableAndFile(tf);
			handle = cache_.Insert(key, tf, 1, DeleteEntry);
		}

		Table table = ((TableAndFile) (cache_.Value(handle))).table;
		Iterator result = table.NewIterator(options);

		Function UnrefEntry = new UnrefEntryCacheAndHandle(cache_, handle);
		result.RegisterCleanup(UnrefEntry, cache_, handle);
		if (tableptr != null) {
			tableptr[0] = table;
		}

		return result;
	}

	// Evict any entry for the specified file number
	void Evict(long file_number) {
		byte[] buf = util.toBytes(file_number);
		cache_.Erase(new Slice(buf));
	}

	Env env_;
	String dbname_;
	Options options_;
	Cache cache_;
}
