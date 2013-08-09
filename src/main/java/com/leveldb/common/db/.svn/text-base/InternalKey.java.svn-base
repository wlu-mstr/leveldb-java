package com.leveldb.common.db;

import com.leveldb.common.Slice;
import com.leveldb.common.comparator.BytewiseComparatorImpl;
import com.leveldb.common.comparator.InternalKeyComparator;
import com.leveldb.util.SequenceNumber;
import com.leveldb.util.ValueType;
import com.leveldb.util.util;

public class InternalKey {
	/**
	 * Format user_key | longnum{squencenumber<<8|type}
	 */

	public byte[] rep_;
	
	public void InternalKey_(InternalKey other){
		rep_ = other.rep_;
	}
	// changed from 1l<<56 - 1, because java doesnot support unsigned long
	final static long kMaxSequenceNumber = ((1l << 55) - 1);

	public static long PackSequenceAndType(SequenceNumber seq, ValueType t) {
		assert (seq.value <= kMaxSequenceNumber);
		assert (t.value <= ValueType.kTypeValue);
		return (seq.value << 8) | t.value; // lshift 8 bits and concat one byte
	}

	public static byte[] AppendInternalKey(byte[] result, ParsedInternalKey key) {
		if (result == null) {
			result = new byte[0];
		}
		byte key_data[] = key.getUser_key().data();
		byte[] tb = new byte[result.length + key_data.length + 8];

		int offset = 0;
		System.arraycopy(result, 0, tb, offset, result.length);

		offset += result.length;
		System.arraycopy(key_data, 0, tb, offset, key_data.length);

		offset += key_data.length;
		byte[] c = util.toBytes(PackSequenceAndType(key.getSequence(),
				key.getType()));
		System.arraycopy(c, 0, tb, offset, 8);

		return tb;
	}

	

	public InternalKey() {
		rep_ = new byte[0];
	} // Leave rep_ as empty to indicate it is invalid

	public InternalKey(Slice user_key, SequenceNumber s, ValueType t) {
		rep_ = AppendInternalKey(rep_, new ParsedInternalKey(user_key, s, t));
	}

	public void DecodeFrom(Slice s) {
		rep_ = s.data();
	}

	public Slice Encode() {
		assert (rep_ != null);
		return new Slice(rep_);
	}

	// get first bytes excpt the last 8 bytes
	public static Slice ExtractUserKey(Slice internal_key) {
		return new Slice(
				util.head(internal_key.data(), internal_key.size() - 8));
	}

	// get the last 8 bytes
	public static Slice ExtractNum(Slice internal_key) {
		return new Slice(util.end(internal_key.data(), 8));
	}

	public Slice user_key() {
		return new Slice(util.head(rep_, rep_.length - 8));
	}

	public void SetFrom(ParsedInternalKey p) {
		rep_ = new byte[0];
		rep_ = AppendInternalKey(rep_, p);
	}

	public void Clear() {
		rep_ = new byte[0];
	}

	public String DebugString() {
		if (rep_.length < 8) {
			return "Length < 8, (bad)";
		}
		Slice userkey = user_key();
		int len = rep_.length;
		byte[] num = new byte[8];
		System.arraycopy(rep_, len - 8, num, 0, 8);
		long num_ = util.toLong(num);
		ParsedInternalKey pk = new ParsedInternalKey(userkey,
				new SequenceNumber(num_ >> 8), new ValueType(
						(byte) (num_ & 0xff)));

		return pk.DebugString();// TODO
	}

	public String toString() {
		return DebugString();
	}

	// wlu, 2012-4-9
	public static ParsedInternalKey ParseInternalKey_(Slice internal_key) {
		ParsedInternalKey result = new ParsedInternalKey();
		int n = internal_key.size();
		if (n < 8)
			return null;
		long num = util.toLong(internal_key.data(), n - 8);
		byte c = (byte) (num & 0xff);
		result.sequence = new SequenceNumber(num >> 8);
		result.type = new ValueType(c);
		result.user_key = new Slice(internal_key.data(), 0, n - 8);
		if (c <= (ValueType.kTypeValue)) {
			return result;
		}
		return null;
	}

	// ////////////////////////////////////////////////////////////////////////////
	// / add test cases from dbformat.cpp
	static class FormatTest {

		static void ASSERT_TRUE(boolean b) {
			if (!b) {
				System.out.println(b);
			}
		}

		static void ASSERT_TRUE(boolean b, String error) {
			if (!b) {
				System.out.println(error);
			}
		}
		
		static void ASSERT_EQ(byte[] b1, byte[] b2, String error) {
			if (util.compareTo(b1, b2) != 0) {
				System.out.println(error);
			}
		}
		
		static void ASSERT_EQ(byte[] b1, byte[] b2) {
			if (util.compareTo(b1, b2) != 0) {
				System.out.println(util.toString(b1) + ", " + util.toString(b2));
			}
		}
		
		

		static byte[] IKey(String user_key, long seq, ValueType vt) {
			byte[] encoded = AppendInternalKey(null, new ParsedInternalKey(
					new Slice(user_key), new SequenceNumber(seq), vt));
			return encoded;
		}
		
		static byte[] IKey(byte[] user_key, long seq, ValueType vt) {
			byte[] encoded = AppendInternalKey(null, new ParsedInternalKey(
					new Slice(user_key), new SequenceNumber(seq), vt));
			return encoded;
		}
		

		static byte[] Shorten(byte[] s, byte[] l) {
			byte[] result = (new InternalKeyComparator(
					BytewiseComparatorImpl.getInstance()))
					.FindShortestSeparator(s, new Slice(l));
			return  (result);
		}

		byte[] ShortSuccessor(byte[] s) {
			byte[] result = (new InternalKeyComparator(
					BytewiseComparatorImpl.getInstance()))
					.FindShortSuccessor(s);

			return  (result);
		}

		static void TestKey(String key, long seq, ValueType vt) {
			byte[] encoded = IKey(key, seq, vt);

			Slice in = new Slice(encoded);
			ParsedInternalKey decoded = new ParsedInternalKey();
			// new ParsedInternalKey(new Slice(""), new SequenceNumber(0),
			// ValueType.TypeValue);
			decoded = ParseInternalKey_(in);
			ASSERT_TRUE(decoded != null);
			ASSERT_TRUE(key.compareTo(decoded.user_key.toString()) == 0, "key: " + key + ", [D] " + decoded.user_key.toString());
			ASSERT_TRUE(seq == decoded.sequence.value, "Seq: " + seq + ", [D] " + decoded.sequence.value);
			ASSERT_TRUE(vt.value == decoded.type.value, "value: " +  vt.value + " [D] " + decoded.type.value);

			decoded = ParseInternalKey_(new Slice("bar"));
			ASSERT_TRUE(decoded == null);
		}

		void InternalKey_EncodeDecode() {
			String keys[] = { "", "k", "hello", "longggggggggggggggggggggg" };
			long seq[] = { 1, 2, 3, (1 << 8) - 1, 1 << 8, (1 << 8) + 1,
					(1 << 16) - 1, 1 << 16, (1 << 16) + 1, (1 << 32) - 1,
					1 << 32, (1 << 32) + 1 };
			for (int k = 0; k < keys.length; k++) {
				for (int s = 0; s < seq.length; s++) {
					if(k == 1 && s == 3){
						k = 1;
					}
					TestKey(keys[k], seq[s], ValueType.TypeValue);
					TestKey("hello", 1, ValueType.TypeDeletion);
				}
			}
		}
		
		
		
		void InternalKeyShortSeparator() {
			  // When user keys are same
			  ASSERT_EQ(IKey("foo", 100, ValueType.TypeValue),
			            Shorten(IKey("foo", 100, ValueType.TypeValue),
			                    IKey("foo", 99, ValueType.TypeValue)));
			  ASSERT_EQ(IKey("foo", 100, ValueType.TypeValue),
			            Shorten(IKey("foo", 100, ValueType.TypeValue),
			                    IKey("foo", 101, ValueType.TypeValue)));
			  ASSERT_EQ(IKey("foo", 100, ValueType.TypeValue),
			            Shorten(IKey("foo", 100, ValueType.TypeValue),
			                    IKey("foo", 100, ValueType.TypeValue)));
			  ASSERT_EQ(IKey("foo", 100, ValueType.TypeValue),
			            Shorten(IKey("foo", 100, ValueType.TypeValue),
			                    IKey("foo", 100, ValueType.TypeDeletion)));

			  // When user keys are misordered
			  ASSERT_EQ(IKey("foo", 100, ValueType.TypeValue),
			            Shorten(IKey("foo", 100, ValueType.TypeValue),
			                    IKey("bar", 99, ValueType.TypeValue)));

			  // When user keys are different, but correctly ordered
			  ASSERT_EQ(IKey("g", kMaxSequenceNumber, ValueType.ValueTypeForSeek),
			            Shorten(IKey("foo", 100, ValueType.TypeValue),
			                    IKey("hello", 200, ValueType.TypeValue)));

			  // When start user key is prefix of limit user key
			  ASSERT_EQ(IKey("foo", 100, ValueType.TypeValue),
			            Shorten(IKey("foo", 100, ValueType.TypeValue),
			                    IKey("foobar", 200, ValueType.TypeValue)));

			  // When limit user key is prefix of start user key
			  ASSERT_EQ(IKey("foobar", 100, ValueType.TypeValue),
			            Shorten(IKey("foobar", 100, ValueType.TypeValue),
			                    IKey("foo", 200, ValueType.TypeValue)));
			}
		
		void InternalKeyShortestSuccessor() {
			  ASSERT_EQ(IKey("g", kMaxSequenceNumber, ValueType.ValueTypeForSeek),
			            ShortSuccessor(IKey("foo", 100, ValueType.TypeValue)));
			  byte[] b = {(byte) 0xff, (byte) 0xff};
			  String s = util.toString(b);
			  ASSERT_EQ(IKey(b, 100, ValueType.TypeValue),
			            ShortSuccessor(IKey(b, 100, ValueType.TypeValue)));
			}
	}
	
	public static void main(String args[]){
		FormatTest ft = new FormatTest();
		//ft.InternalKey_EncodeDecode();
		//ft.InternalKeyShortSeparator();
		ft.InternalKeyShortestSuccessor();
		
	}

}
