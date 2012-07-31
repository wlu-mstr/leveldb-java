package com.leveldb.common.db;


public class FileMetaData{
	public int refs;
	public int allowed_seeks; // Seeks allowed until compaction
	public long number;
	public long file_size; // File size in bytes
	// [need to init the two members]
	public InternalKey smallest = new InternalKey(); // Smallest internal key served by table
	public InternalKey largest  = new InternalKey(); // Largest internal key served by table

	public FileMetaData() {
		refs = 0;
		allowed_seeks = 1 << 30;
		file_size = 0;
	}

	public int getRefs() {
		return refs;
	}

	public void setRefs(int refs) {
		this.refs = refs;
	}

	public int getAllowed_seeks() {
		return allowed_seeks;
	}

	public void setAllowed_seeks(int allowed_seeks) {
		this.allowed_seeks = allowed_seeks;
	}

	public long getNumber() {
		return number;
	}

	public void setNumber(long number) {
		this.number = number;
	}

	public long getFile_size() {
		return file_size;
	}

	public void setFile_size(long file_size) {
		this.file_size = file_size;
	}

	public InternalKey getSmallest() {
		return smallest;
	}

	public void setSmallest(InternalKey smallest) {
		this.smallest = smallest;
	}

	public InternalKey getLargest() {
		return largest;
	}

	public void setLargest(InternalKey largest) {
		this.largest = largest;
	}

	

}
