package com.leveldb.common.db;

import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.leveldb.common.AtomicPointer;
import com.leveldb.common.Cache;
import com.leveldb.common.Comparator;
import com.leveldb.common.Env;
import com.leveldb.common.Function;
import com.leveldb.common.Iterator;
import com.leveldb.common.Logger;
import com.leveldb.common.Slice;
import com.leveldb.common.Status;
import com.leveldb.common.WriteBatch;
import com.leveldb.common.WriteBatchInternal;
import com.leveldb.common.config;
import com.leveldb.common.comparator.InternalKeyComparator;
import com.leveldb.common.file.FileType;
import com.leveldb.common.file._SequentialFile;
import com.leveldb.common.file._WritableFile;
import com.leveldb.common.file.filename;
//import com.leveldb.common.log.Writer;
import com.leveldb.common.log.Reader;
import com.leveldb.common.options.Options;
import com.leveldb.common.options.ReadOptions;
import com.leveldb.common.options.WriteOptions;
import com.leveldb.common.table.MergingIterator;
import com.leveldb.common.table.TableBuilder;
import com.leveldb.common.version.Version;
import com.leveldb.common.version.VersionEdit;
import com.leveldb.common.version.VersionSet;
import com.leveldb.util.SequenceNumber;
import com.leveldb.util.ValueType;
import com.leveldb.util.logging;

// test
public class DBImpl extends DB {
	Log LOG = LogFactory.getLog(DBImpl.class);

	static class CompactionState {
		Compaction compaction;
		// Sequence numbers < smallest_snapshot are not significant since we
		// will never have to service a snapshot below smallest_snapshot.
		// Therefore if we have seen a sequence number S <= smallest_snapshot,
		// we can drop all entries for the same key with sequence numbers < S.
		public SequenceNumber smallest_snapshot;

		// Files produced by compaction
		static class Output {
			long number;
			long file_size;
			InternalKey smallest = new InternalKey();
			InternalKey largest = new InternalKey();
		}

		// wlu, 2012-7-7, bugfix: init the List
		List<Output> outputs = new ArrayList<DBImpl.CompactionState.Output>();

		// State kept for output being generated
		public _WritableFile outfile;
		public TableBuilder builder;

		public long total_bytes;

		public Output current_output() {
			return outputs.get(outputs.size() - 1);
		}

		public CompactionState(Compaction c) {
			compaction = c;
			outfile = null;
			builder = null;
			total_bytes = 0;
		}
	}

	// Information kept for every waiting writer
	class Writer {
		Status status;
		WriteBatch batch;
		boolean sync;
		boolean done;
		Condition cv;

		Writer(ReentrantLock mu) {
			cv = mu.newCondition();
		}
	}

	// members
	Env env_;
	InternalKeyComparator internal_comparator_;
	Options options_; // options_.comparator == &internal_comparator_
	boolean owns_info_log_;
	boolean owns_cache_;
	String dbname_;

	// table_cache_ provides its own synchronization
	TableCache table_cache_;

	// Lock over the persistent DB state. Non-NULL iff successfully acquired.
	FileLock db_lock_;

	VersionSet versions_;
	_WritableFile logfile_;
	com.leveldb.common.log.Writer log_;
	Condition bg_cv_; // Signalled when background work finishes
	MemTable mem_;
	MemTable imm_; // Memtable being compacted
	// So bg thread can detect non-NULL imm_
	// TODO maybe change to AtomicBoolean
	AtomicPointer<MemTable> has_imm_ = new AtomicPointer<MemTable>(null);

	long logfile_number_;

	SnapshotList snapshots_ = new SnapshotList();

	// Set of table files to protect from deletion because they are
	// part of ongoing compactions.
	Set<Long> pending_outputs_ = new HashSet<Long>();

	// Has a background compaction been scheduled or is running?
	boolean bg_compaction_scheduled_;

	// Information for a manual compaction
	class ManualCompaction {
		int level;
		boolean done;
		InternalKey begin; // NULL means beginning of key range
		InternalKey end; // NULL means end of key range
		InternalKey tmp_storage; // Used to keep track of compaction progress
	}

	ManualCompaction manual_compaction_;

	// Queue of writers.
	Queue<Writer> writers_ = new LinkedList<Writer>();
	WriteBatch tmp_batch_;

	public final ReentrantLock mutex_ = new ReentrantLock();

	AtomicBoolean shutting_down_ = new AtomicBoolean(false);

	// Have we encountered a background error in paranoid mode?
	Status bg_error_ = new Status();

	// Per level compaction stats. stats_[level] stores the stats for
	// compactions that produced data for the specified "level".
	class CompactionStats {
		long micros;
		long bytes_read;
		long bytes_written;

		CompactionStats() {
			micros = 0;
			bytes_read = 0;
			bytes_written = 0;
		}

		void Add(CompactionStats c) {
			this.micros += c.micros;
			this.bytes_read += c.bytes_read;
			this.bytes_written += c.bytes_written;
		}
	}

	CompactionStats stats_[] = new CompactionStats[config.kNumLevels];

	// ///////////////////
	public ReentrantLock getmutex() {
		return mutex_;
	}

	private int ClipToRange(int ptr, int minvalue, int maxvalue) {
		if (ptr > maxvalue)
			ptr = maxvalue;
		if (ptr < minvalue)
			ptr = minvalue;
		return ptr;
	}

	private Options SanitizeOptions(String dbname, InternalKeyComparator icmp,
			Options src) {
		Options result = new Options();
		result.Options_(src);
		result.comparator = icmp;
		ClipToRange(result.max_open_files, 20, 50000);
		ClipToRange(result.write_buffer_size, 64 << 10, 1 << 30);
		ClipToRange(result.block_size, 1 << 10, 4 << 20);
		if (result.info_log == null) {
			// Open a log file in the same directory as the db
			src.env.CreateDir(dbname); // In case it does not exist
			src.env.RenameFile(filename.InfoLogFileName(dbname),
					filename.OldInfoLogFileName(dbname));
			result.info_log = src.env.NewLogger(filename
					.InfoLogFileName(dbname));
			// if (!s.ok()) {
			// // No place suitable for logging
			// result.info_log = NULL;
			// }
		}
		if (result.block_cache == null) {
			result.block_cache = Cache.NewLRUCache(8 << 20);
		}
		return result;
	}

	public DBImpl(Options options, String dbname) {
		env_ = options.env;
		internal_comparator_ = new InternalKeyComparator(options.comparator);
		options_ = SanitizeOptions(dbname, internal_comparator_, options);
		owns_info_log_ = options_.info_log != options.info_log;
		owns_cache_ = options_.block_cache != options.block_cache;
		dbname_ = dbname;
		db_lock_ = null;
		// shutting_down_ = null;
		bg_cv_ = mutex_.newCondition();
		mem_ = new MemTable(internal_comparator_);
		imm_ = null;
		logfile_ = null;
		logfile_number_ = 0;
		log_ = null;
		tmp_batch_ = new WriteBatch();
		bg_compaction_scheduled_ = false;
		manual_compaction_ = null;
		mem_.Ref();
		has_imm_.Release_Store(null);

		// Reserve ten files or so for other uses and give the rest to
		// TableCache.
		int table_cache_size = options.max_open_files - 10;
		table_cache_ = new TableCache(dbname_, options_, table_cache_size);

		versions_ = new VersionSet(dbname_, options_, table_cache_,
				internal_comparator_);

		for (int i = 0; i < config.kNumLevels; i++) {
			stats_[i] = new CompactionStats();
		}
	}

	// public void ReleaseDbImpl

	@Override
	public Status Put(WriteOptions options, Slice key, Slice value) {
		return super.Put(options, key, value);
	}

	@Override
	public Status Delete(WriteOptions options, Slice key) {
		return super.Delete(options, key);
	}

	@Override
	public Status Write(WriteOptions options, WriteBatch my_batch) {
		Status status = Status.OK();
		Writer w = new Writer(mutex_);
		w.batch = my_batch;
		w.sync = options.sync;
		w.done = false;

		// MutexLock l(&mutex_);
		mutex_.lock();
		try {
			writers_.add(w);
			while (!w.done && w != writers_.peek()) {
				w.cv.awaitUninterruptibly(); // wait here
			}
			if (w.done) {
				return w.status;
			}

			// May temporarily unlock and wait.
			status = MakeRoomForWrite(my_batch == null);
			SequenceNumber last_sequence = versions_.LastSequence();
			Writer last_writer = w;
			if (status.ok() && my_batch != null) { // NULL batch is for
													// compactions
				WriteBatch updates = BuildBatchGroup(last_writer);
				WriteBatchInternal
						.SetSequence(updates, last_sequence.value + 1);
				last_sequence.value += WriteBatchInternal.Count(updates);

				// Add to log and apply to memtable. We can release the lock
				// during this phase since &w is currently responsible for
				// logging
				// and protects against concurrent loggers and concurrent writes
				// into mem_.
				{
					mutex_.unlock();
					status = log_.AddRecord(WriteBatchInternal
							.Contents(updates));
					if (status.ok() && options.sync) {
						status = logfile_.Sync();
					}
					if (status.ok()) {
						status = WriteBatchInternal.InsertInto(updates, mem_);
					}
					mutex_.lock();
				}
				if (updates == tmp_batch_)
					tmp_batch_.Clear();

				versions_.SetLastSequence(last_sequence);
			}

			while (true) {
				Writer ready = writers_.peek();
				writers_.poll();
				if (ready != w) {
					ready.status = status;
					ready.done = true;
					ready.cv.signal(); // just signal not signalAll ... why?
				}
				if (ready == last_writer)
					break;
			}

			// Notify new head of write queue
			if (!writers_.isEmpty()) {
				writers_.peek().cv.signal(); // signal the next round...
			}

		} finally {
			mutex_.unlock();
		}
		return status;

	}

	// REQUIRES: mutex_ is held
	// REQUIRES: this thread is currently at the front of the writer queue
	private Status MakeRoomForWrite(boolean force) {
		mutex_.isHeldByCurrentThread();
		assert (!writers_.isEmpty());
		boolean allow_delay = !force;
		Status s = Status.OK();
		while (true) {
			if (!bg_error_.ok()) {
				// Yield previous error
				s = bg_error_;
				break;
			} else if (allow_delay
					&& versions_.NumLevelFiles(0) >= config.kL0_SlowdownWritesTrigger) {
				// We are getting close to hitting a hard limit on the number of
				// L0 files. Rather than delaying a single write by several
				// seconds when we hit the hard limit, start delaying each
				// individual write by 1ms to reduce latency variance. Also,
				// this delay hands over some CPU to the compaction thread in
				// case it is sharing the same core as the writer.
				mutex_.unlock();
				env_.SleepForMicroseconds(1000);
				allow_delay = false; // Do not delay a single write more than
										// once
				mutex_.lock();
			} else if (!force
					&& (mem_.ApproximateMemoryUsage() <= options_.write_buffer_size)) {
				// There is room in current memtable
				break;
			} else if (imm_ != null) {
				// We have filled up the current memtable, but the previous
				// one is still being compacted, so we wait.
				bg_cv_.awaitUninterruptibly();
			} else if (versions_.NumLevelFiles(0) >= config.kL0_StopWritesTrigger) {
				// There are too many level-0 files.
				LOG.info("There are too many level-0 files. waiting...\n");
				bg_cv_.awaitUninterruptibly();
			} else {
				// Attempt to switch to a new memtable and trigger compaction of
				// old
				assert (versions_.PrevLogNumber() == 0);
				long new_log_number = versions_.NewFileNumber();
				_WritableFile lfile = env_.NewWritableFile(filename
						.LogFileName(dbname_, new_log_number));
				if (!s.ok()) {
					break;
				}

				// close before reset
				logfile_.Close();
				logfile_ = lfile;
				logfile_number_ = new_log_number;
				log_ = new com.leveldb.common.log.Writer(lfile);
				imm_ = mem_;
				has_imm_.Release_Store(imm_);
				mem_ = new MemTable(internal_comparator_);
				mem_.Ref();
				force = false; // Do not force another compaction if have room
				LOG.info("Attempt to switch to a new memtable and trigger compaction of old");
				MaybeScheduleCompaction();
			}
		}
		return s;
	}

	// REQUIRES: Writer list must be non-empty
	// REQUIRES: First writer must have a non-NULL batch
	private WriteBatch BuildBatchGroup(Writer last_writer) {
		assert (!writers_.isEmpty());
		Writer first = writers_.peek();
		WriteBatch result = first.batch;
		assert (result != null);

		int size = WriteBatchInternal.ByteSize(first.batch);

		// Allow the group to grow up to a maximum size, but if the
		// original write is small, limit the growth so we do not slow
		// down the small write too much.
		int max_size = 1 << 20;
		if (size <= (128 << 10)) {
			max_size = size + (128 << 10);
		}

		last_writer = first;
		java.util.Iterator<Writer> iter = writers_.iterator();// begin();
		iter.next();
		// ++iter; // Advance past "first"
		while (iter.hasNext()) {
			Writer w = iter.next();
			if (w.sync && !first.sync) {
				// Do not include a sync write into a batch handled by a
				// non-sync write.
				break;
			}

			if (w.batch != null) {
				size += WriteBatchInternal.ByteSize(w.batch);
				if (size > max_size) {
					// Do not make batch too big
					break;
				}

				// Append to *reuslt
				if (result == first.batch) {
					// Switch to temporary batch instead of disturbing caller's
					// batch
					result = tmp_batch_;
					assert (WriteBatchInternal.Count(result) == 0);
					WriteBatchInternal.Append(result, first.batch);
				}
				WriteBatchInternal.Append(result, w.batch);
			}
			last_writer = w;
		}
		return result;
	}

	@Override
	public Slice Get(ReadOptions options, Slice key, Status st) {
		Slice result = new Slice();
		Status s = null;
		mutex_.lock();
		SequenceNumber snapshot;
		if (options.snapshot != null) {
			snapshot = ((SnapshotImpl) (options.snapshot)).number_;

		} else {
			snapshot = versions_.LastSequence();
		}

		MemTable mem = mem_;
		MemTable imm = imm_;
		Version current = versions_.current();
		mem.Ref();
		if (imm != null) {
			imm.Ref();
		}
		current.Ref();

		boolean have_stat_update = false;
		Version.GetStats stats = new Version.GetStats();

		// Unlock while reading from files and memtables
		{
			mutex_.unlock();
			// First look in the memtable, then in the immutable memtable
			// (if
			// any).
			LookupKey lkey = new LookupKey(key, snapshot);
			if (mem.Get(lkey, result, s) != null) {
				// different as original codes

			} else if (imm != null && imm.Get(lkey, result, s) != null) {
				// Done
			} else {
				result.setData_(current.Get(options, lkey, stats,
						new Status[] { new Status() }));
				have_stat_update = true;
			}
			mutex_.lock();
		}

		if (have_stat_update && current.UpdateStats(stats)) {
			MaybeScheduleCompaction();
		}
		mem.Unref();
		if (imm != null)
			imm.Unref();
		current.Unref();

		mutex_.unlock();
		if (result.size() == 0) {
			st.Status_(Status.NotFound(
					new Slice("Value of key '" + key.toString()
							+ "' is not found."), null));
		}
		return result;

	}

	Comparator user_comparator() {
		return internal_comparator_.user_comparator();
	}

	/*
	 * create a new file (with new sequence number) for compact.outfile; create
	 * a new TableBuilder for the compact.builder
	 */
	Status OpenCompactionOutputFile(CompactionState compact) {
		assert (compact != null);
		assert (compact.builder == null);
		long file_number;
		{
			mutex_.lock();
			file_number = versions_.NewFileNumber();
			pending_outputs_.add(file_number);
			CompactionState.Output out = new CompactionState.Output();
			out.number = file_number;
			out.smallest.Clear();
			out.largest.Clear();
			compact.outputs.add(out);
			mutex_.unlock();
		}

		// Make the output file
		String fname = filename.TableFileName(dbname_, file_number);
		compact.outfile = env_.NewWritableFile(fname);
		compact.builder = new TableBuilder(options_, compact.outfile);
		return Status.OK();
	}

	public void Close() {
		// Wait for background work to finish
		mutex_.lock();
		// shutting_down_.Release_Store(this); // Any non-NULL value is ok
		while (bg_compaction_scheduled_) {
			try {
				bg_cv_.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		mutex_.unlock();

		if (db_lock_ != null) {
			env_.UnlockFile(db_lock_);
		}

		versions_.Close();// close the files
		versions_ = null;
		if (mem_ != null)
			mem_.Unref();
		if (imm_ != null)
			imm_.Unref();
		tmp_batch_ = null;
		log_ = null;
		if (logfile_ != null) {
			logfile_.Close();
			logfile_ = null;
		}

		// wlu, 2012-6-2, need to destroy yhe cache
		table_cache_.Destroy();
		table_cache_ = null;

		if (owns_info_log_) {
			options_.info_log.Close();
			options_.info_log = null;
		}
		// wlu,
		if (options_.info_log != null) {
			options_.info_log.Close();
		}

		if (owns_cache_) {
			options_.block_cache = null;
		}

		// terminate the scheduled threads
		env_.EndSchedule();

	}

	Status FinishCompactionOutputFile(CompactionState compact, Iterator input) {
		assert (compact != null);
		assert (compact.outfile != null);
		assert (compact.builder != null);

		long output_number = compact.current_output().number;
		assert (output_number != 0);

		// Check for iterator errors
		Status s = input.status();
		long current_entries = compact.builder.NumEntries();
		if (s.ok()) {
			s = compact.builder.Finish();
		} else {
			compact.builder.Abandon();
		}
		long current_bytes = compact.builder.FileSize();
		compact.current_output().file_size = current_bytes;
		compact.total_bytes += current_bytes;
		compact.builder = null;

		// Finish and check for file errors
		if (s.ok()) {
			s = compact.outfile.Sync();
		}
		if (s.ok()) {
			s = compact.outfile.Close();
		}
		compact.outfile = null;

		if (s.ok() && current_entries > 0) {
			// Verify that the table is usable
			Iterator iter = table_cache_.NewIterator(new ReadOptions(),
					output_number, current_bytes, null);
			s = iter.status();
			iter = null;
			// wlu, 2012-7-10, bugFix: s !=null
			if (s != null && s.ok()) {
				LOG.info("Generated table #" + output_number + ": "
						+ current_entries + " keys, " + current_bytes
						+ " bytes");
			}
			// wlu, 2012-7-10, bugFix: s ==null ...
			if (s == null) {
				s = new Status();
			}
		}
		return s;
	}

	Status InstallCompactionResults(CompactionState compact) {
		mutex_.isHeldByCurrentThread();
		LOG.info("Compacted " + compact.compaction.num_input_files(0) + "@"
				+ compact.compaction.level() + " + "
				+ compact.compaction.num_input_files(1) + "@"
				+ (compact.compaction.level() + 1) + " files =>"
				+ (compact.total_bytes) + " bytes");

		// Add compaction outputs
		compact.compaction.AddInputDeletions(compact.compaction.edit());
		int level = compact.compaction.level();
		for (int i = 0; i < compact.outputs.size(); i++) {
			CompactionState.Output out = compact.outputs.get(i);
			compact.compaction.edit().AddFile(level + 1, out.number,
					out.file_size, out.smallest, out.largest);
		}
		return versions_.LogAndApply(compact.compaction.edit(), mutex_);
	}

	void CleanupCompaction(CompactionState compact) {
		mutex_.isHeldByCurrentThread();
		if (compact.builder != null) {
			// May happen if we get a shutdown call in the middle of compaction
			compact.builder.Abandon();
			compact.builder = null;
		} else {
			assert (compact.outfile == null);
		}
		// wlu, 2012-7-7, bugfix: check whether is null
		if (compact.outfile != null) {
			compact.outfile.Close();
		}
		compact.outfile = null;
		for (int i = 0; i < compact.outputs.size(); i++) {
			CompactionState.Output out = compact.outputs.get(i);
			pending_outputs_.remove(out.number);
		}
		compact = null;
	}

	/**
	 * Whatever.., this function write key-value in the compact to file (table)
	 * by compact's builder. Details including data drop ...
	 * 
	 * @param compact
	 * @return
	 */
	Status DoCompactionWork(CompactionState compact) {
		long start_micros = env_.NowMicros();
		long imm_micros = 0; // Micros spent doing imm_ compactions

		LOG.info("Compacting " + compact.compaction.num_input_files(0) + "@"
				+ compact.compaction.level() + " + "
				+ compact.compaction.num_input_files(1) + "@"
				+ (compact.compaction.level() + 1) + " files");

		assert (versions_.NumLevelFiles(compact.compaction.level()) > 0);
		assert (compact.builder == null);
		assert (compact.outfile == null);
		if (snapshots_.empty()) {
			compact.smallest_snapshot = versions_.LastSequence();
		} else {
			compact.smallest_snapshot = snapshots_.oldest().number_;
		}

		// Release mutex while we're actually doing the compaction work
		mutex_.unlock();

		Iterator input = versions_.MakeInputIterator(compact.compaction);
		input.SeekToFirst();
		Status status = Status.OK();
		ParsedInternalKey ikey;
		String current_user_key = "";
		boolean has_current_user_key = false;
		long last_sequence_for_key = SequenceNumber.kMaxSequenceNumber;
		for (; input.Valid() && !shutting_down_.get();) {
			// Prioritize immutable compaction work
			if (has_imm_.NoBarrier_Load() != null) {
				long imm_start = env_.NowMicros();
				mutex_.lock();
				if (imm_ != null) {
					CompactMemTable();
					bg_cv_.signalAll(); // Wakeup MakeRoomForWrite() if
										// necessary
				}
				mutex_.unlock();
				imm_micros += (env_.NowMicros() - imm_start);
			}

			Slice key = input.key();
			if (compact.compaction.ShouldStopBefore(key)
					&& compact.builder != null) {
				status = FinishCompactionOutputFile(compact, input);
				if (!status.ok()) {
					break;
				}
			}

			// Handle key/value, add to state, etc.
			boolean drop = false;
			ikey = InternalKey.ParseInternalKey_(key);
			if (ikey == null) {
				// Do not hide error keys
				current_user_key = "";
				has_current_user_key = false;
				last_sequence_for_key = SequenceNumber.kMaxSequenceNumber;
			} else {
				if (!has_current_user_key
						|| user_comparator().Compare(ikey.user_key,
								new Slice(current_user_key)) != 0) {
					// First occurrence of this user key
					// current_user_key.assign(ikey.user_key.data(),
					// ikey.user_key.size());
					current_user_key = ikey.user_key.toString();
					has_current_user_key = true;
					last_sequence_for_key = SequenceNumber.kMaxSequenceNumber;
				}

				if (last_sequence_for_key <= compact.smallest_snapshot.value) {
					// Hidden by an newer entry for same user key
					drop = true; // (A)
				} else if (ikey.type.value == ValueType.kTypeDeletion
						&& ikey.sequence.value <= compact.smallest_snapshot.value
						&& compact.compaction.IsBaseLevelForKey(ikey.user_key)) {
					// For this user key:
					// (1) there is no data in higher levels
					// (2) data in lower levels will have larger sequence
					// numbers
					// (3) data in layers that are being compacted here and have
					// smaller sequence numbers will be dropped in the next
					// few iterations of this loop (by rule (A) above).
					// Therefore this deletion marker is obsolete and can be
					// dropped.
					drop = true;
				}

				last_sequence_for_key = ikey.sequence.value;
			}
			// #if 0
			LOG.debug("  Compact: " + ikey.user_key.toString() + ", seq "
					+ ikey.sequence + ", type: " + ikey.type.value + " "
					+ ValueType.kTypeValue + ", drop: " + drop + ", is_base: "
					+ compact.compaction.IsBaseLevelForKey(ikey.user_key)
					+ ", " + last_sequence_for_key + " smallest_snapshot:"
					+ compact.smallest_snapshot);

			// #endif

			if (!drop) {
				// Open output file if necessary
				if (compact.builder == null) {
					status = OpenCompactionOutputFile(compact);
					if (!status.ok()) {
						break;
					}
				}
				if (compact.builder.NumEntries() == 0) {
					compact.current_output().smallest.DecodeFrom(key);
				}
				compact.current_output().largest.DecodeFrom(key);
				compact.builder.Add(key, input.value());

				// Close output file if it is big enough
				if (compact.builder.FileSize() >= compact.compaction
						.MaxOutputFileSize()) {
					status = FinishCompactionOutputFile(compact, input);
					if (!status.ok()) {
						break;
					}
				}
			}

			input.Next();
		}

		if (status.ok() && shutting_down_.get()) {
			status = Status.IOError(new Slice("Deleting DB during compaction"),
					null);
		}
		if (status.ok() && compact.builder != null) {
			status = FinishCompactionOutputFile(compact, input);
		}
		if (status.ok()) {
			status = input.status();
		}
		input = null;

		CompactionStats stats = new CompactionStats();
		stats.micros = env_.NowMicros() - start_micros - imm_micros;
		for (int which = 0; which < 2; which++) {
			for (int i = 0; i < compact.compaction.num_input_files(which); i++) {
				stats.bytes_read += compact.compaction.input(which, i).file_size;
			}
		}
		for (int i = 0; i < compact.outputs.size(); i++) {
			stats.bytes_written += compact.outputs.get(i).file_size;
		}

		mutex_.lock();
		stats_[compact.compaction.level() + 1].Add(stats);

		if (status.ok()) {
			status = InstallCompactionResults(compact);
		}
		VersionSet.LevelSummaryStorage tmp = new VersionSet.LevelSummaryStorage();
		LOG.info("compacted to: " + versions_.LevelSummary(tmp));
		return status;
	}

	/**
	 * write memtable data to leve0 or higher; call: Builder.BuildTable; add a
	 * CompactStatus to the selected level
	 */
	private Status WriteLevel0Table(MemTable mem, VersionEdit edit, Version base) {
		assert (mutex_.isHeldByCurrentThread());// AssertHeld();
		long start_micros = env_.NowMicros();
		FileMetaData meta = new FileMetaData();
		meta.setNumber(versions_.NewFileNumber());
		pending_outputs_.add(meta.getNumber());
		Iterator iter = mem.NewIterator();
		LOG.info("Level-0 table #" + meta.number
				+ ": started (in WriteLevel0Table(...))");

		Status s;
		{
			mutex_.unlock();
			s = Builder.BuildTable(dbname_, env_, options_, table_cache_, iter,
					meta);
			mutex_.lock();
		}

		LOG.info("Level-0 table #" + meta.number + ": " + meta.file_size
				+ " bytes " + s.toString());
		iter = null;
		pending_outputs_.remove(meta.getNumber());

		// Note that if file_size is zero, the file has been deleted and
		// should not be added to the manifest.
		int level = 0;
		if (s.ok() && meta.getFile_size() > 0) {
			Slice min_user_key = meta.getSmallest().user_key();
			Slice max_user_key = meta.getLargest().user_key();
			if (base != null) {
				level = base.PickLevelForMemTableOutput(min_user_key,
						max_user_key);
			}
			edit.AddFile(level, meta.getNumber(), meta.getFile_size(),
					meta.getSmallest(), meta.getLargest());
		}

		CompactionStats stats = new CompactionStats();
		stats.micros = env_.NowMicros() - start_micros;
		stats.bytes_written = meta.getFile_size();
		stats_[level].Add(stats);
		return s;
	}

	Status CompactMemTable() {
		assert (mutex_.isHeldByCurrentThread());
		assert (imm_ != null);

		// Save the contents of the memtable as a new Table
		VersionEdit edit = new VersionEdit();
		Version base = versions_.current();
		base.Ref();
		Status s = WriteLevel0Table(imm_, edit, base);
		base.Unref();

		if (s.ok() && shutting_down_.get()) {
			s = Status.IOError(new Slice(
					"Deleting DB during memtable compaction"), null);
		}

		// Replace immutable memtable with the generated Table
		if (s.ok()) {
			edit.SetPrevLogNumber(0);
			edit.SetLogNumber(logfile_number_); // Earlier logs no longer needed
			s = versions_.LogAndApply(edit, mutex_); // TODO
		}

		if (s.ok()) {
			// Commit to the new state
			imm_.Unref();
			imm_ = null;
			has_imm_.Release_Store(null); // set to null
			DeleteObsoleteFiles();
		}

		return s;
	}

	/**
	 * Compation running at backgroud thread: <li>Compact memtable first</li>
	 * <li>Create Compaction</li>
	 * <ol>
	 * <li>if is manual, get Compaction based on the specific range at the
	 * specific level</li>
	 * <li>if not manual, Pick Compaction</li>
	 * </ol>
	 * <li>Compact the Compaction</li>
	 * <ol>
	 * <li>c==null, do nothing</li>
	 * <li>not manual AND is trival move, move by "del and add"</li>
	 * <li>call {DoCompactionWork}</li>
	 * </ol>
	 */
	private void BackgroundCompaction() {
		if (!mutex_.isHeldByCurrentThread()) {
			return;
		}

		if (imm_ != null) {
			CompactMemTable();
			return;
		}

		Compaction c;
		boolean is_manual = (manual_compaction_ != null);
		InternalKey manual_end = null;
		if (is_manual) {
			// return sth to be compacted @ level
			c = versions_.CompactRange(manual_compaction_.level,
					manual_compaction_.begin, manual_compaction_.end);
			manual_compaction_.done = (c == null);
			if (c != null) {
				manual_end = c.input(0, c.num_input_files(0) - 1).getLargest();
			}
			LOG.info("Manual compaction at level-"
					+ manual_compaction_.level
					+ " from "
					+ (manual_compaction_.begin != null ? manual_compaction_.begin
							.DebugString() : "(begin)")
					+ " ..."
					+ (manual_compaction_.end != null ? manual_compaction_.end
							.DebugString() : "(end)")
					+ "; will stop at "
					+ (manual_compaction_.done ? "(end)" : manual_end
							.DebugString()) + "\n");
		} else {
			c = versions_.PickCompaction();
		}

		Status status = Status.OK();
		if (c == null) {
			// Nothing to do
		} else if (!is_manual && c.IsTrivialMove()) {
			// Move file to next level
			assert (c.num_input_files(0) == 1);
			FileMetaData f = c.input(0, 0);
			c.edit().DeleteFile(c.level(), f.getNumber());
			c.edit().AddFile(c.level() + 1, f.getNumber(), f.getFile_size(),
					f.getSmallest(), f.getLargest());
			status = versions_.LogAndApply(c.edit(), mutex_);
			VersionSet.LevelSummaryStorage tmp = new VersionSet.LevelSummaryStorage();
			LOG.info("Moved " + (f.getNumber()) + "to level-" + (c.level() + 1)
					+ " " + (f.getFile_size()) + " bytes " + status.toString()
					+ ": " + versions_.LevelSummary(tmp) + "\n");
		} else {
			CompactionState compact = new CompactionState(c);
			status = DoCompactionWork(compact);
			CleanupCompaction(compact);
			c.ReleaseInputs();
			DeleteObsoleteFiles();
		}
		// delete c;

		if (status.ok()) {
			// Done
		} else if (shutting_down_.get()) {
			// Ignore compaction errors found during shutting down
		} else {
			Logger.Log(options_.info_log, "Compaction error: ",
					status.toString());
			if (options_.paranoid_checks && bg_error_.ok()) {
				bg_error_ = status;
			}
		}

		if (is_manual) {
			ManualCompaction m = manual_compaction_;
			if (!status.ok()) {
				m.done = true;
			}
			if (!m.done) {
				// We only compacted part of the requested range. Update *m
				// to the range that is left to be compacted.
				m.tmp_storage = manual_end;
				m.begin = m.tmp_storage;
			}
			manual_compaction_ = null;
		}
	}

	@Override
	public Iterator NewIterator(ReadOptions options) {
		SequenceNumber latest_snapshot = new SequenceNumber(0);
		Iterator internal_iter = NewInternalIterator(options, latest_snapshot);
		return DBIter
				.NewDBIterator(
						dbname_,
						env_,
						user_comparator(),
						internal_iter,
						(options.snapshot != null ? ((SnapshotImpl) (options.snapshot)).number_
								: latest_snapshot));
	}

	@Override
	public Snapshot GetSnapshot() {
		mutex_.lock();
		try {
			return snapshots_.New(versions_.LastSequence());
		} finally {
			mutex_.unlock();
		}
	}

	@Override
	public void ReleaseSnapshot(Snapshot snapshot) {
		mutex_.lock();
		snapshots_.Delete((SnapshotImpl) (snapshot));

	}

	@Override
	public boolean GetProperty(Slice property, StringBuffer value) {

		mutex_.tryLock();
		Slice in = property;
		Slice prefix = new Slice("leveldb.");
		if (!in.starts_with(prefix)) {
			return false;
		}
		in.remove_prefix(prefix.size());

		if (in.starts_with(new Slice("num-files-at-level"))) {
			in.remove_prefix("num-files-at-level".length());
			long level = logging.ConsumeDecimalNumber(in.toString());

			// in.empty();
			if (level >= config.kNumLevels) {
				return false;
			} else {
				String buf = versions_.NumLevelFiles((int) level) + "";
				value.append(buf);
				return true;
			}
		} else if (in.compareTo(new Slice("stats")) == 0) {
			String buf = "                               Compactions\n"
					+ "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
					+ "--------------------------------------------------\n";
			value.append(buf);
			for (int level = 0; level < config.kNumLevels; level++) {
				int files = versions_.NumLevelFiles(level);
				if (stats_[level].micros > 0 || files > 0) {

					buf = level + " " + files + " "
							+ versions_.NumLevelBytes(level) / 1048576.0 + " "
							+ stats_[level].micros / 1e6 + " "
							+ stats_[level].bytes_read / 1048576.0 + " "
							+ stats_[level].bytes_written / 1048576.0 + "\n";
					value.append(buf);
				}
			}
			return true;
		} else if (in.compareTo(new Slice("sstables")) == 0) {
			value.append(versions_.current().DebugString());
			return true;
		}

		mutex_.unlock();
		return false;

	}

	@Override
	public long[] GetApproximateSizes(Range range[], int n) {
		Version v = null;
		try {
			mutex_.lock();
			versions_.current().Ref();
			v = versions_.current();

		} finally {
			mutex_.unlock();
		}

		long sizes[] = new long[n];
		for (int i = 0; i < n; i++) {
			// Convert user_key into a corresponding internal key.
			InternalKey k1 = new InternalKey(range[i].start,
					SequenceNumber.MaxSequenceNumber,
					ValueType.ValueTypeForSeek);
			InternalKey k2 = new InternalKey(range[i].limit,
					SequenceNumber.MaxSequenceNumber,
					ValueType.ValueTypeForSeek);
			long start = versions_.ApproximateOffsetOf(v, k1);
			long limit = versions_.ApproximateOffsetOf(v, k2);
			sizes[i] = (limit >= start ? limit - start : 0);
		}

		try {
			mutex_.lock();
			v.Unref();
		} finally {
			mutex_.unlock();
		}

		return sizes;

	}

	// wlu, 2012-5-11
	public void CompactRange(Slice begin, Slice end) {
		int max_level_with_files = 1;
		mutex_.lock();
		try {
			Version base = versions_.current();
			for (int level = 1; level < config.kNumLevels; level++) {
				if (base.OverlapInLevel(level, begin, end)) {
					max_level_with_files = level;
				}
			}
		} finally {
			mutex_.unlock();
		}
		TEST_CompactMemTable();
		for (int level = 0; level < max_level_with_files; level++) {
			TEST_CompactRange(level, begin, end);
		}
	}

	// Extra methods (for testing) that are not in the public DB interface

	// Compact any files in the named level that overlap [*begin,*end]
	public void TEST_CompactRange(int level, Slice begin, Slice end) {
		assert (level >= 0);
		assert (level + 1 < config.kNumLevels);

		InternalKey begin_storage, end_storage;

		ManualCompaction manual = new ManualCompaction();
		manual.level = level;
		manual.done = false;
		if (begin == null) {
			manual.begin = null;
		} else {
			begin_storage = new InternalKey(begin,
					SequenceNumber.MaxSequenceNumber,
					ValueType.ValueTypeForSeek);
			manual.begin = begin_storage;
		}
		if (end == null) {
			manual.end = null;
		} else {
			end_storage = new InternalKey(end, new SequenceNumber(0),
					new ValueType((byte) 0));
			manual.end = end_storage;
		}

		mutex_.lock();
		try {
			while (!manual.done) {
				while (manual_compaction_ != null) {
					bg_cv_.await();
				}
				manual_compaction_ = manual;
				MaybeScheduleCompaction();
				while (manual_compaction_ == manual) {
					bg_cv_.await();
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			mutex_.unlock();
		}
	}

	// Force current memtable contents to be compacted.
	public Status TEST_CompactMemTable() {
		// NULL batch means just wait for earlier writes to be done
		Status s = Write(new WriteOptions(), null);
		if (s.ok()) {
			// Wait until the compaction completes
			mutex_.lock();
			try {
				while (imm_ != null && bg_error_.ok()) {
					bg_cv_.await();
				}
				if (imm_ != null) {
					s = bg_error_;
				}

			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				mutex_.unlock();
			}
		}
		return s;
	}

	static class IterState {
		ReentrantLock mu;
		Version version;
		MemTable mem;
		MemTable imm;
	}

	static class CleanupIteratorState implements Function {
		@Override
		public void exec(Object... args) {
			IterState state = (IterState) (args[0]);
			state.mu.lock();
			state.mem.Unref();
			if (state.imm != null)
				state.imm.Unref();
			state.version.Unref();
			state.mu.unlock();

		}
	}

	Iterator NewInternalIterator(ReadOptions options,
			SequenceNumber latest_snapshot) {
		IterState cleanup = new IterState();
		mutex_.lock();
		latest_snapshot.value = versions_.LastSequence().value;

		// Collect together all needed child iterators
		List<Iterator> list = new ArrayList<Iterator>();
		list.add(mem_.NewIterator());
		mem_.Ref();
		if (imm_ != null) {
			list.add(imm_.NewIterator());
			imm_.Ref();
		}

		versions_.current().AddIterators(options, list);
		Iterator[] iterArray = new Iterator[list.size()];
		int i = 0;
		for (Iterator it : list) {
			iterArray[i] = it;
			i++;
		}
		// if (false) {
		// Iterator i1 = mem_.NewIterator();
		// i1.SeekToFirst();
		// while (i1.Valid()) {
		// System.out.println("In Mem\t" + i1.key() + " -> " + i1.value());
		// i1.Next();
		//
		// }
		//
		// if (imm_ != null) {
		// Iterator i2 = imm_.NewIterator();
		// i2.SeekToFirst();
		// while (i2.Valid()) {
		// System.out.println("In imm\t" + i2.key() + " -> "
		// + i2.value());
		// i2.Next();
		// }
		// }
		//
		// // Iterator i3 = list.get(2);
		// // i3.SeekToFirst();
		// // while(i3.Valid()){
		// // System.out.println("In file\t" + i3.key() + " -> " + i3.value());
		// // i3.Next();
		// // }
		// }
		Iterator internal_iter = MergingIterator.NewMergingIterator(
				internal_comparator_, iterArray, list.size());
		versions_.current().Ref();

		cleanup.mu = mutex_;
		cleanup.mem = mem_;
		cleanup.imm = imm_;
		cleanup.version = versions_.current();
		internal_iter
				.RegisterCleanup(new CleanupIteratorState(), cleanup, null);

		mutex_.unlock();
		return internal_iter;
	}

	// Return an internal iterator over the current state of the database.
	// The keys of this iterator are internal keys (see format.h).
	// The returned iterator should be deleted when no longer needed.
	public Iterator TEST_NewInternalIterator() {
		SequenceNumber ignored = new SequenceNumber(0);
		return NewInternalIterator(new ReadOptions(), ignored);
	}

	// Return the maximum overlapping data (in bytes) at next level for any
	// file at a level >= 1.
	public long TEST_MaxNextLevelOverlappingBytes() {
		mutex_.lock();
		try {
			return versions_.MaxNextLevelOverlappingBytes();
		} finally {
			mutex_.unlock();
		}
	}

	private Status NewDB() {
		VersionEdit new_db = new VersionEdit();
		new_db.SetComparatorName(new Slice(user_comparator().Name()));
		new_db.SetLogNumber(0);
		new_db.SetNextFile(2);
		new_db.SetLastSequence(new SequenceNumber(0));

		_WritableFile file = null;
		String manifest = null;

		try {
			manifest = filename.DescriptorFileName(dbname_, 1);
			file = env_.NewWritableFile(manifest);
		} catch (Exception e) {
			return Status.IOError(new Slice(e.getMessage()), null);
		}

		Status s = Status.OK();
		{
			com.leveldb.common.log.Writer log = new com.leveldb.common.log.Writer(
					file);
			byte[] record = new_db.EncodeTo();
			s = log.AddRecord(new Slice(record));
			if (s.ok()) {
				s = file.Close();
			}
		}
		file.Close();
		file = null;
		if (s.ok()) {
			// Make "CURRENT" file that points to the new manifest file.
			s = filename.SetCurrentFile(env_, dbname_, 1);
		} else {
			env_.DeleteFile(manifest);
		}
		return s;
	}

	// Recover the descriptor from persistent storage. May do a significant
	// amount of work to recover recently logged updates. Any changes to
	// be made to the descriptor are added to *edit.
	public Status Recover(VersionEdit edit) {
		if (!mutex_.isHeldByCurrentThread()) {
			return Status.NotSupported(new Slice("mutex should be hold"), null);
		}

		// Ignore error from CreateDir since the creation of the DB is
		// committed only when the descriptor is created, and this directory
		// may already exist from a previous failed creation attempt.
		env_.CreateDir(dbname_);
		assert (db_lock_ == null);
		db_lock_ = env_.LockFile(filename.LockFileName(dbname_));
		if (db_lock_ == null) {
			return Status.IOError(new Slice("LockFile create error"), null);
		}

		Status s = null;
		if (!env_.FileExists(filename.CurrentFileName(dbname_))) {
			if (options_.create_if_missing) {
				s = NewDB();
				if (!s.ok()) {
					return s;
				}
			} else {
				return Status.InvalidArgument(new Slice(dbname_), new Slice(
						"does not exist (create_if_missing is false)"));
			}
		} else {
			if (options_.error_if_exists) {
				return Status.InvalidArgument(new Slice(dbname_), new Slice(
						"exists (error_if_exists is true)"));
			}
		}

		s = versions_.Recover();
		if (s.ok()) {
			SequenceNumber max_sequence = new SequenceNumber(0);

			// Recover from all newer log files than the ones named in the
			// descriptor (new log files may have been added by the previous
			// incarnation without registering them in the descriptor).
			//
			// Note that PrevLogNumber() is no longer used, but we pay
			// attention to it in case we are recovering a database
			// produced by an older version of leveldb.
			long min_log = versions_.LogNumber();
			long prev_log = versions_.PrevLogNumber();
			List<String> filenames = env_.GetChildren(dbname_);

			long number;
			FileType type = new FileType();
			List<Long> logs = new ArrayList<Long>();
			for (int i = 0; i < filenames.size(); i++) {
				try {
					number = filename.ParseFileName(filenames.get(i), type);
				} catch (Exception e) {
					number = -1;
					e.printStackTrace();
				}
				if (number >= 0 && type.value == FileType.kLogFile
						&& ((number >= min_log) || (number == prev_log))) {
					logs.add(number);
				}
			}

			// Recover in the order in which the logs were generated
			Collections.sort(logs);
			for (int i = 0; i < logs.size(); i++) {
				s = RecoverLogFile(logs.get(i), edit, max_sequence);

				// The previous incarnation may not have written any MANIFEST
				// records after allocating this log number. So we manually
				// update the file number allocation counter in VersionSet.
				versions_.MarkFileNumberUsed(logs.get(i));
			}

			if (s.ok()) {
				if (versions_.LastSequence().value < max_sequence.value) {
					versions_.SetLastSequence(max_sequence);
				}
			}
		}

		return s;
	}

	// for RecoverLogFile
	private void MaybeIgnoreError(Status s) {
		if (s.ok() || options_.paranoid_checks) {
			// No change needed
		} else {
			LOG.info("Ignoring error " + s.toString());
			s.Status_(Status.OK());
		}
	}

	public void DeleteObsoleteFiles() {
		// Make a set of all of the live files
		Set<Long> live = pending_outputs_;
		versions_.AddLiveFiles(live);

		List<String> filenames = env_.GetChildren(dbname_); // Ignoring errors
															// on purpose
		long number;
		FileType type = new FileType();
		for (int i = 0; i < filenames.size(); i++) {
			try {
				number = filename.ParseFileName(filenames.get(i), type);
			} catch (Exception e) {
				number = -1;
				e.printStackTrace();
			}
			if (number != -1) {
				boolean keep = true;
				switch (type.value) {
				case FileType.kLogFile:
					keep = ((number >= versions_.LogNumber()) || (number == versions_
							.PrevLogNumber()));
					break;
				case FileType.kDescriptorFile:
					// Keep my manifest file, and any newer incarnations'
					// (in case there is a race that allows other incarnations)
					keep = (number >= versions_.ManifestFileNumber());
					break;
				case FileType.kTableFile:
					keep = live.contains(number);
					// LOG.info("#" + number +
					// " is not contained in 'live' List");
					break;
				case FileType.kTempFile:
					// Any temp files that are currently being written to must
					// be recorded in pending_outputs_, which is inserted into
					// "live"
					keep = live.contains(number);
					break;
				case FileType.kCurrentFile:
				case FileType.kDBLockFile:
				case FileType.kInfoLogFile:
					keep = true;
					break;
				}

				if (!keep) {
					if (type.value == FileType.kTableFile) {
						table_cache_.Evict(number);
					}
					LOG.info("Delete type=" + type.value + " # " + number
							+ "\n delete file: " + dbname_ + "/"
							+ filenames.get(i));
					env_.DeleteFile(dbname_ + "/" + filenames.get(i));
				}
			}
		}
	}

	private Status RecoverLogFile(long log_number, VersionEdit edit,
			SequenceNumber max_sequence) {
		class LogReporter extends Reader.Reporter {
			Env env;
			Logger info_log;
			String fname;
			Status status; // NULL if options_.paranoid_checks==false

			@Override
			public void Corruption(int bytes, Status status) {
				LOG.info((this.status == null ? "(ignoring error) " : "")
						+ fname + ": dropping " + bytes + " bytes; " + status);
				if (this.status != null && this.status.ok())
					this.status = status;

			}

		}
		// Env* env;
		// Logger* info_log;
		// const char* fname;
		// Status* status; // NULL if options_.paranoid_checks==false
		// virtual void Corruption(size_t bytes, const Status& s) {
		// Log(info_log, "%s%s: dropping %d bytes; %s",
		// (this->status == NULL ? "(ignoring error) " : ""),
		// fname, static_cast<int>(bytes), s.ToString().c_str());
		// if (this->status != NULL && this->status->ok()) *this->status = s;
		// }
		// };

		if (!mutex_.isHeldByCurrentThread()) {
			return null;
		}

		// Open the log file
		String fname = filename.LogFileName(dbname_, log_number);
		_SequentialFile file = env_.NewSequentialFile(fname);
		Status status = new Status();
		// MaybeIgnoreError(status);
		// return status;

		// Create the log reader.
		LogReporter reporter = new LogReporter();
		reporter.env = env_;
		reporter.info_log = options_.info_log;
		reporter.fname = fname;
		reporter.status = (options_.paranoid_checks ? status : null);
		// We intentially make log::Reader do checksumming even if
		// paranoid_checks==false so that corruptions cause entire commits
		// to be skipped instead of propagating bad information (like overly
		// large sequence numbers).
		Reader reader = new Reader(file, reporter, true/* checksum */, 0/* initial_offset */);
		LOG.info("Recovering log #" + log_number);

		// Read all the records and add to a memtable
		byte[] scratch = new byte[0];
		Slice record = new Slice();
		WriteBatch batch = new WriteBatch();
		MemTable mem = null;
		while (reader.ReadRecord(record, scratch) && status.ok()) {
			if (record.size() < 12) {
				reporter.Corruption(record.size(), Status.Corruption(new Slice(
						"log record too small"), new Slice()));
				continue;
			}
			WriteBatchInternal.SetContents(batch, record);

			if (mem == null) {
				mem = new MemTable(internal_comparator_);
				mem.Ref();
			}
			status = WriteBatchInternal.InsertInto(batch, mem);
			MaybeIgnoreError(status);
			if (!status.ok()) {
				break;
			}
			long last_seq = WriteBatchInternal.Sequence(batch).value
					+ WriteBatchInternal.Count(batch) - 1;
			if (last_seq > max_sequence.value) {
				max_sequence.value = last_seq;
			}

			if (mem.ApproximateMemoryUsage() > options_.write_buffer_size) {
				status = WriteLevel0Table(mem, edit, null);
				if (!status.ok()) {
					// Reflect errors immediately so that conditions like full
					// file-systems cause the DB::Open() to fail.
					break;
				}
				mem.Unref();
				mem = null;
			}
		}

		if (status.ok() && mem != null) {
			status = WriteLevel0Table(mem, edit, null);
			// Reflect errors immediately so that conditions like full
			// file-systems cause the DB::Open() to fail.
		}

		if (mem != null)
			mem.Unref();
		file.Close();
		file = null;
		return status;
	}

	public void MaybeScheduleCompaction() {
		assert (mutex_.isHeldByCurrentThread());
		if (bg_compaction_scheduled_) {
			// Already scheduled
			LOG.info("Already scheduled");
		} else if (shutting_down_.get()) {
			// DB is being deleted; no more background compactions
			LOG.info("DB is being deleted");
		} else if (imm_ == null && manual_compaction_ == null
				&& !versions_.NeedsCompaction()) {
			// No work to be done
			LOG.info("No work to be done");
		} else {
			bg_compaction_scheduled_ = true;
			// env_.Schedule(&DBImpl::BGWork, this);
			LOG.info("Schedule new background thread");
			env_.Schedule(new BGWork());
		}
	}

	void BackgroundCall() {
		mutex_.lock();
		// try {
		assert (bg_compaction_scheduled_);
		if (!shutting_down_.get()) {
			BackgroundCompaction();
		}
		bg_compaction_scheduled_ = false;

		// Previous compaction may have produced too many files in a level,
		// so reschedule another compaction if needed.
		MaybeScheduleCompaction();
		bg_cv_.signalAll();
		// } finally {
		mutex_.unlock();
		// }
	}

	class BGWork implements Function {
		@Override
		public void exec(Object... args) {
			BackgroundCall();
		}

	}

}
