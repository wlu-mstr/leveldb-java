package com.leveldb.common.comparator;

import com.leveldb.common.Comparator;
import com.leveldb.common.Slice;
import com.leveldb.common.db.InternalKey;
import com.leveldb.util.SequenceNumber;
import com.leveldb.util.ValueType;
import com.leveldb.util.util;

public class InternalKeyComparator extends Comparator {

	private Comparator user_comparator_;

	public InternalKeyComparator(Comparator c) {
		user_comparator_ = c;
	}

	// getter
	public Comparator user_comparator() {
		return user_comparator_;
	}

	@Override
	public int Compare(Slice akey, Slice bkey) {
		// Order by:
		// increasing user key (according to user-supplied comparator)
		// decreasing sequence number
		// decreasing type (though sequence# should be enough to disambiguate)
		int r = user_comparator_.Compare(InternalKey.ExtractUserKey(akey),
				InternalKey.ExtractUserKey(bkey));
		if (r == 0) {
			long anum = util.toLong(InternalKey.ExtractNum(akey).data());
			long bnum = util.toLong(InternalKey.ExtractNum(bkey).data());
			if (anum > bnum) {
				r = -1;
			} else if (anum < bnum) {
				r = +1;
			}
		}
		return r;
	}

	public int Compare(InternalKey akey, InternalKey bkey) {
		return Compare(akey.Encode(), bkey.Encode());
	}

	@Override
	public String Name() {
		return InternalKeyComparator.class.getName();
	}

	@Override
	public byte[] FindShortestSeparator(byte[] start, Slice limit) {
		// Attempt to shorten the user portion of the key
		Slice user_start = InternalKey.ExtractUserKey(new Slice(start));
		Slice user_limit = InternalKey.ExtractUserKey(limit);
		// make a copy here, 'cause BytewiseComparatorImpl changes input array
		byte[] tmp = util.deepCopy(user_start.data());
		byte[] result = user_comparator_.FindShortestSeparator(tmp, user_limit);
		if (result.length < user_start.size()
				&& user_comparator_.Compare(user_start, new Slice(result)) < 0) {
			// User key has become shorter physically, but larger logically.
			// Tack on the earliest possible number to the shortened user key.
			result = util.add(result, util.toBytes(InternalKey
					.PackSequenceAndType(SequenceNumber.MaxSequenceNumber,
							ValueType.ValueTypeForSeek)));
			assert (this.Compare(new Slice(start), new Slice(result)) < 0);
			assert (this.Compare(new Slice(result), limit) < 0);
			return result;
		}
		return start;
	}

	@Override
	public byte[] FindShortSuccessor(byte[] key) {
		Slice user_key = InternalKey.ExtractUserKey(new Slice(key));
		// make a copy here, 'cause BytewiseComparatorImpl changes input array
		byte[] tmp = util.deepCopy(user_key.data());
		tmp = user_comparator_.FindShortSuccessor(tmp);
		if (tmp.length < user_key.size()
				&& user_comparator_.Compare(user_key, new Slice(tmp)) < 0) {
			// User key has become shorter physically, but larger logically.
			// Tack on the earliest possible number to the shortened user key.
			tmp = util.add(tmp, util.toBytes(InternalKey.PackSequenceAndType(
					SequenceNumber.MaxSequenceNumber,
					ValueType.ValueTypeForSeek)));
			assert (this.Compare(new Slice(key), new Slice(tmp)) < 0);
			// key->swap(tmp);
			return tmp;
		}
		return key;
	}
}
