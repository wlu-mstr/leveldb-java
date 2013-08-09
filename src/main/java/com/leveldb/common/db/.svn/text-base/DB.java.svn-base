package com.leveldb.common.db;

import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.List;

import com.leveldb.common.Env;
import com.leveldb.common.Iterator;
import com.leveldb.common.Slice;
import com.leveldb.common.Status;
import com.leveldb.common.WriteBatch;
import com.leveldb.common.file.FileType;
import com.leveldb.common.file._WritableFile;
import com.leveldb.common.file.filename;
import com.leveldb.common.log.Writer;
import com.leveldb.common.options.Options;
import com.leveldb.common.options.ReadOptions;
import com.leveldb.common.options.WriteOptions;
import com.leveldb.common.version.VersionEdit;

//A DB is a persistent ordered map from keys to values.
//A DB is safe for concurrent access from multiple threads without
//any external synchronization.

public abstract class DB {
	// Open the database with the specified "name".
	// Stores a pointer to a heap-allocated database in dbptr and returns
	// OK on success.
	// Stores NULL in dbptr and returns a non-OK status on error.
	// Caller should delete dbptr when it is no longer needed.

	// Set the database entry for "key" to "value". Returns OK on success,
	// and a non-OK status on error.
	// Note: consider setting options.sync = true.
	public Status Put(WriteOptions opt, Slice key, Slice value) {
		WriteBatch batch = new WriteBatch();
		batch.Put(key, value);
		return Write(opt, batch); // call concrete Write
	}

	// Remove the database entry (if any) for "key". Returns OK on
	// success, and a non-OK status on error. It is not an error if "key"
	// did not exist in the database.
	// Note: consider setting options.sync = true.
	public Status Delete(WriteOptions opt, Slice key) {
		WriteBatch batch = new WriteBatch();
		batch.Delete(key);
		return Write(opt, batch);
	}

	// Apply the specified updates to the database.
	// Returns OK on success, non-OK on failure.
	// Note: consider setting options.sync = true.
	public abstract Status Write(WriteOptions options, WriteBatch updates);

	// If the database contains an entry for "key" store the
	// corresponding value in value and return OK.
	//
	// If there is no entry for "key" leave value unchanged and return
	// a status for which Status::IsNotFound() returns true.
	//
	// May return some other Status on an error.
	public abstract Slice Get(ReadOptions options, Slice key, Status s);

	// Return a heap-allocated iterator over the contents of the database.
	// The result of NewIterator() is initially invalid (caller must
	// call one of the Seek methods on the iterator before using it).
	//
	// Caller should delete the iterator when it is no longer needed.
	// The returned iterator should be deleted before this db is deleted.
	public abstract Iterator NewIterator(ReadOptions options);

	// Return a handle to the current DB state. Iterators created with
	// this handle will all observe a stable snapshot of the current DB
	// state. The caller must call ReleaseSnapshot(result) when the
	// snapshot is no longer needed.
	public abstract Snapshot GetSnapshot();

	// Release a previously acquired snapshot. The caller must not
	// use "snapshot" after this call.
	public abstract void ReleaseSnapshot(Snapshot snapshot);

	// DB implementations can export properties about their state
	// via this method. If "property" is a valid property understood by this
	// DB implementation, fills "value" with its current value and returns
	// true. Otherwise returns false.
	//
	//
	// Valid property names include:
	//
	// "leveldb.num-files-at-level<N>" - return the number of files at level
	// <N>,
	// where <N> is an ASCII representation of a level number (e.g. "0").
	// "leveldb.stats" - returns a multi-line string that describes statistics
	// about the internal operation of the DB.
	public abstract boolean GetProperty(Slice property, StringBuffer value);

	// For each i in [0,n-1], store in "sizes[i]", the approximate
	// file system space used by keys in "[range[i].start .. range[i].limit)".
	//
	// Note that the returned sizes measure file system space usage, so
	// if the user data compresses by a factor of ten, the returned
	// sizes will be one-tenth the size of the corresponding user data size.
	//
	// The results may not include the sizes of recently written data.
	public abstract long[] GetApproximateSizes(Range range[], int n);

	public long GetApproximateSizes(Range range) {
		return GetApproximateSizes(new Range[] { range }, 1)[0];
	}
	
	public abstract void CompactRange(Slice begin, Slice end);

	// Possible extensions:
	// (1) Add a method to compact a range of keys

	public static DB Open(Options options, String dbname) {
		DB dbptr = null;
		DBImpl impl = new DBImpl(options, dbname);
		impl.getmutex().lock();
		VersionEdit edit = new VersionEdit();
		Status s = impl.Recover(edit); // Handles create_if_missing,
										// error_if_exists
		if (s.ok()) {
			long new_log_number = impl.versions_.NewFileNumber();
			_WritableFile lfile = options.env.NewWritableFile(filename
					.LogFileName(dbname, new_log_number));
			{
				edit.SetLogNumber(new_log_number);
				impl.logfile_ = lfile;
				impl.logfile_number_ = new_log_number;
				impl.log_ = new Writer(lfile);
				s = impl.versions_.LogAndApply(edit, impl.mutex_);
			}
			if (s.ok()) {
				impl.DeleteObsoleteFiles();
				impl.MaybeScheduleCompaction();
			}
		}
		impl.mutex_.unlock();
		if (s.ok()) {
			dbptr = impl;
		} else {
			// wlu, 2012-7-10, bugFix: something goes wrong, release resources
			impl.Close();
			impl = null;
		}
		return dbptr;
	}
	
	public static DB Open(Options options, String dbname, Status s_) {
		DB dbptr = null;
		DBImpl impl = new DBImpl(options, dbname);
		impl.getmutex().lock();
		VersionEdit edit = new VersionEdit();
		Status s = impl.Recover(edit); // Handles create_if_missing,
										// error_if_exists
		if (s.ok()) {
			long new_log_number = impl.versions_.NewFileNumber();
			_WritableFile lfile = options.env.NewWritableFile(filename
					.LogFileName(dbname, new_log_number));
			{
				edit.SetLogNumber(new_log_number);
				impl.logfile_ = lfile;
				impl.logfile_number_ = new_log_number;
				impl.log_ = new Writer(lfile);
				s = impl.versions_.LogAndApply(edit, impl.mutex_);
			}
			if (s.ok()) {
				impl.DeleteObsoleteFiles();
				impl.MaybeScheduleCompaction();
			}
		}
		impl.mutex_.unlock();
		if (s.ok()) {
			dbptr = impl;
		} else {
			// wlu, 2012-7-10, bugFix: something goes wrong, release resources
			impl.Close();
			impl = null;
			
		}
		
		s_.Status_(s);
		return dbptr;
	}

	public abstract void Close();

	public static Status DestroyDB(String dbname, Options options) {
		Env env = options.env;
		List<String> filenames = new ArrayList<String>();
		// Ignore error in case directory does not exist
		try {
			filenames = env.GetChildren(dbname);
		} catch (Exception e) {
			filenames = new ArrayList<String>();
		}
		if (filenames.isEmpty()) {
			return Status.OK();
		}

		FileLock lock = null;
		String lockname = filename.LockFileName(dbname);
		lock = env.LockFile(lockname);

		Status s = Status.OK();

		if (lock != null) {
			long number;
			FileType type = new FileType();
			for (int i = 0; i < filenames.size(); i++) {
				try {
					number = filename.ParseFileName(filenames.get(i), type);
				} catch (Exception e) {
					// delete the file whatever it is
					number = 0;
				}
				if (number >= 0 && type.value != FileType.kDBLockFile) { // Lock
																			// file
																			// will
																			// be
																			// deleted
																			// at
																			// end
					s = env.DeleteFile(dbname + "/" + filenames.get(i));
				}
			}
			env.UnlockFile(lock); // Ignore error since state is already gone
			env.DeleteFile(dbname + "/LOCK");
			env.DeleteDir(dbname); // Ignore error in case dir contains other
									// files
		}
		return s;
	}

}
