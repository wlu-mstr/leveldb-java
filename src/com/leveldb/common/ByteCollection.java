package com.leveldb.common;

public class ByteCollection {
	public byte[] bytes;
	public int curr_pos;
	public boolean ok;
	public ByteCollection(byte[] ibyts, int ipos){
		bytes = ibyts;
		curr_pos = ipos;
		ok = true;
	}
	
	// copy src data into bytes, from curr_pos
	public void BytesSet(byte[] src){
		System.arraycopy(src, 0, bytes, curr_pos, src.length);
	}
	
	public byte get(){
		return bytes[curr_pos];
	}
	
	public boolean OK(){
		return ok;
	}
	
	public boolean STOP(){
		return bytes.length <= curr_pos;
	}

}
