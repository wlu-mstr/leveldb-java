package com.leveldb.common.file;

public class FileType {
	public static final int kLogFile = 0;
	 public static final int kDBLockFile = 1;
	 public static final int kTableFile = 2;
	 public static final int kDescriptorFile = 3;
	 public static final int kCurrentFile = 4;
	 public static final int kTempFile = 5;
	 public static final int kInfoLogFile = 6;
	 // Either the current one, or an old one
	 
	 public int value;
	 
	 public FileType(int i){
		 value = i;
	 }
	 
	 public FileType(){
		 value = -1;
	 }
}
