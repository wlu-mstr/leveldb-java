package com.leveldb.common.db;

import com.leveldb.common.Env;
import com.leveldb.common.Iterator;
import com.leveldb.common.Slice;
import com.leveldb.common.Status;
import com.leveldb.common.file._WritableFile;
import com.leveldb.common.file.filename;
import com.leveldb.common.options.Options;
import com.leveldb.common.options.ReadOptions;
import com.leveldb.common.table.TableBuilder;

/**
 * 2012-4-12: build a table (SST file) from memtable
 * @author Administrator
 *
 */
public class Builder {
	public static Status BuildTable(String dbname, Env env, Options options,
			TableCache table_cache, Iterator iter, FileMetaData meta) {
		Status s = new Status();
		meta.setFile_size(0);
		iter.SeekToFirst();

		String fname = filename.TableFileName(dbname, meta.getNumber());
		if (iter.Valid()) {
			_WritableFile file = env.NewWritableFile(fname);

			TableBuilder builder = new TableBuilder(options, file);
			meta.getSmallest().DecodeFrom(iter.key());
			for (; iter.Valid(); iter.Next()) {
				Slice key = iter.key();
				meta.getLargest().DecodeFrom(key);
				builder.Add(key, iter.value());
			}

			// Finish and check for builder errors
			if (s.ok()) {
				s = builder.Finish();
				if (s.ok()) {
					meta.setFile_size(builder.FileSize());
					assert (meta.getFile_size() > 0);
				}
			} else {
				builder.Abandon();
			}
			builder = null;

			// Finish and check for file errors
			if (s.ok()) {
				s = file.Sync();
			}
			if (s.ok()) {
				s = file.Close();
			}
			file = null;

			if (s.ok()) {
				// Verify that the table is usable
				Iterator it = table_cache.NewIterator(new ReadOptions(),
						meta.getNumber(), meta.getFile_size(), null);
				s = it.status();
				it = null;
			}
		}

		// Check for input iterator errors
		if (!iter.status().ok()) {
			s = iter.status();
		}

		if (s.ok() && meta.getFile_size() > 0) {
			// Keep it
		} else {
			env.DeleteFile(fname);
		}
		return s;
	}

}
