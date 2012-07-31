package com.leveldb.util;

public class ValueType {
	
	public static final byte kTypeDeletion = 0x0;
	public static final byte  kTypeValue = 0x1;
	
	// kValueTypeForSeek defines the ValueType that should be passed when
	// constructing a ParsedInternalKey object for seeking to a particular
	// sequence number (since we sort sequence numbers in decreasing order
	// and the value type is embedded as the low 8 bits in the sequence
	// number in internal keys, we need to use the highest-numbered
	// ValueType, not the lowest).
	public static byte kValueTypeForSeek = kTypeValue;

	public byte value;
	public ValueType(byte v){
		value = v;
	}
	
	public String toString(){
		return "ValueType: " + value;
	}
	
	public static ValueType TypeValue = new ValueType(kTypeValue);
	public static ValueType TypeDeletion = new ValueType(kTypeDeletion);
	public static ValueType ValueTypeForSeek = new ValueType(kValueTypeForSeek);
}
