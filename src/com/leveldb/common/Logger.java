package com.leveldb.common;

public abstract class Logger {
	// Write an entry to the log file with the specified format.
	public abstract void Logv(String format, String... ap);

	public static void Log(Logger info_log, String... format) {
		if (info_log != null) {
			info_log.Logv(null, format);
		}
	}
	
	public void Close(){}
}
