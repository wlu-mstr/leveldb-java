package com.leveldb.common;

import com.leveldb.util.util;

// suitable for many append
public class ByteVector {
	private final int defaultsize = 1024; // 1k
	private final int maxdelta = 100 * 1024; // 100k
	private byte[] data;
	private int size;
	private int capacity;
	private int delta; // expend data array by delta

	public ByteVector() {
		data = new byte[defaultsize]; // default init size
		capacity = defaultsize;
		delta = capacity;
		size = 0;
	}

	public ByteVector(int initsize) {
		if (initsize < 0) {
			initsize = defaultsize;
		}
		data = new byte[initsize];
		capacity = initsize;
		delta = capacity;
		size = 0;
	}

	public void append(byte[] inewdata) {
		append(inewdata, 0, inewdata.length);
	}

	public void append(byte[] inewdata, int offset, int length) {
		if (size + length <= capacity) { // just insert
			System.arraycopy(inewdata, offset, data, size, length);
			size += length;
		} else {
			// expend the array
			capacity = capacity + delta;
			// enlarge delta if necessary
			delta = 2 * delta;
			if (delta > maxdelta) {
				delta = maxdelta;
			}
			byte[] data_temp = new byte[capacity];
			// copy original data
			System.arraycopy(data, 0, data_temp, 0, size);
			data = data_temp;
			append(inewdata, offset, length);

		}
	}

	public void clear() {
		size = 0;
	}
	
	public boolean empty(){
		return size == 0;
	}
	
	/*
	 * may truncate or expend the array
	 */
	public void resize(int inewsize){
		// append zero, side effect: expend the array
		if(inewsize > size){
			append(new byte[inewsize - size]); 
		}
		size = inewsize;
	}
	
	public byte get(int idx){
		return data[idx];
	}
	
	public void set(int idx, byte b){
		data[idx] = b;
	}
	
	/**
	 * reset data
	 * @param b	the new data
	 */
	public void set(byte[] b){
		this.size = 0;
		this.append(b);
	}
	
	public void increase(int idx, int delta){
		data[idx] += delta;
	}

	public byte[] getData() {
		byte[] retdata = new byte[size];
		System.arraycopy(data, 0, retdata, 0, size);
		return retdata;
	}
	
	// return reference to the raw bytes, with zeros at tail.
	// need to take care when using, give start--end
	public byte[] getRawRef(){
		return data;
	}
	
	public void resetData(byte[] b){
		data = util.deepCopy(b);
	}

	public int getSize() {
		return size;
	}
	
	public void setSize(int s){
		size = s;
	}

	public int getCapacity() {
		return capacity;
	}
	
	public int compareTo(byte[] other, int offset, int length){
		return util.compareTo(data, 0, size, other, offset, length);
	}
	
	public int compareTo(byte[] other){
		return compareTo(other, 0, other.length);
	}
	
	public String toString(){
		return util.toString(getData());
	}

	public static void main(String args[]) {
		ByteVector bv = new ByteVector();

		for (int i = 0; i < 10; i++) {
			bv.append("testString".getBytes());
			System.out.println(bv.getSize() + "\t" + bv.getCapacity() + "\t"
					+ util.toString(bv.getData()));
		}
		//bv.clear();
		
		System.out.println("=================================================");
		for (int i = 0; i < 10; i++) {
			bv.append("testString".getBytes());
			System.out.println(bv.getSize() + "\t" + bv.getCapacity() + "\t"
					+ util.toString(bv.getData()));
		}
		
		System.out.println(bv.compareTo("uest".getBytes()));
	}

}
