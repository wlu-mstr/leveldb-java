package com.leveldb.common;

import com.leveldb.util.util;

public class Slice implements Comparable<Slice> {
	// ----------------------
	// ^offset
	// ^end
	// | size |
	private byte[] data_;
	private int offset_;
	private int size_;

	public Slice() {
		data_ = new byte[0];
		size_ = 0;
		offset_ = 0;
	}

	public Slice(byte[] iData, int iSize) {
		data_ = iData;
		size_ = iSize;
		offset_ = 0;
	}

	// tricky
	public Slice(byte[] idata, int ofs, int sz) {
		data_ = idata;
		size_ = sz;
		offset_ = ofs;
	}

	public Slice(byte[] iData) {
		data_ = iData;
		size_ = iData.length;
		offset_ = 0;
	}

	// special one
	public Slice(String iStr) {
		data_ = util.toBytes(iStr);
		size_ = data_.length;
		offset_ = 0;
	}

	public Slice(String iStr, int length) {
		data_ = util.head(util.toBytes(iStr), length);
		size_ = length;
		offset_ = 0;
	}

	public byte[] data() {
		// if (size_ == data_.length && offset_ == 0) { // no necessary to do
		// deep copy
		// return data_;
		// } else
		// {
		if(size_ <= 0){
			size_ = size_;
		}
		byte[] ret = new byte[size_];
		System.arraycopy(data_, offset_, ret, 0, size_);
		return ret;
		// }
	}

	public int size() {
		return size_;
	}

	public boolean empty() {
		return size_ == 0;
	}

	public byte get(int n) {
		assert (n < size());
		return data_[n + offset_];
	}

	// Change this slice to refer to an empty array
	public void clear() {
		data_ = new byte[0];
		size_ = 0;
		offset_ = 0;
	}

	// Drop the first "n" bytes from this slice.
	// Polished 12-5-4, by wlu
	public void remove_prefix(int n) {
		assert (n <= size());
		// System.arraycopy(data_, n, data_, 0, size_ - n);
		// if too many thrush here, remove them
		if (offset_ + n >= 65535) {
			System.arraycopy(data_, offset_, data_, 0, size_);
			offset_ = 0;
		}
		offset_ += n;
		size_ -= n;
	}

	// Return a string that contains the copy of the referenced data.
	public String toString() {
		return util.toString(getData_());// String(data_);// + ", " + size_;
	}

	// Three-way comparison. Returns value:
	// < 0 iff "*this" < "b",
	// == 0 iff "*this" == "b",
	// > 0 iff "*this" > "b"
	@Override
	public int compareTo(Slice b) {
		return util.compareTo(data(), b.data());
	}

	// Return true iff "x" is a prefix of "*this"
	public boolean starts_with(Slice x) {
		return ((size_ >= x.size_) && util.compareTo(data_, offset_, x.size(),
				x.data(), 0, x.size()) == 0);
	}

	public static boolean equal2(Slice x, Slice y) {
		return x.size() == y.size() && x.compareTo(y) == 0;
	}

	public static boolean notEqual2(Slice x, Slice y) {
		return !equal2(x, y);
	}

	// added

	public byte[] getData_() {
		return data();
	}

	public void setData_(byte[] idata_) {
		if(idata_ == null){
			this.data_ = new byte[0];
			this.offset_ = 0;
			this.size_ = 0;
			return;
		}
		this.data_ = idata_;
		this.offset_ = 0;
		this.size_ = idata_.length;
	}

	// tricky
	public void setData_(byte[] idata_, int ofs, int size) {
		this.data_ = idata_;
		this.offset_ = ofs;
		this.size_ = size;
	}

	// tricky
	// -------------------------
	// ^ offset
	// ^end
	// | len |
	public void setData_(byte[] idata_, int ofs) {
		this.data_ = idata_;
		this.offset_ = ofs;
		this.size_ = idata_.length - ofs;
	}

	// maybe dummy
	public void setSize_(int size_) {
		this.size_ = size_;
	}

	public void setOffset(int off) {
		this.offset_ = off;
	}

	public int getOffset() {
		return this.offset_;
	}
}
