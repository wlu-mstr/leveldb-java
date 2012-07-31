package com.leveldb.util;

/**
 * some util functions
 * 
 * @author wlu
 * 
 */
public class logging {

	public static String AppendNumberTo(long num) {
		return "" + num;
	}

	// void AppendEscapedStringTo(std::string* str, const Slice& value) {
	// for (size_t i = 0; i < value.size(); i++) {
	// char c = value[i];
	// if (c >= ' ' && c <= '~') {
	// str->push_back(c);
	// } else {
	// char buf[10];
	// snprintf(buf, sizeof(buf), "\\x%02x",
	// static_cast<unsigned int>(c) & 0xff);
	// str->append(buf);
	// }
	// }
	// }
	//
	// std::string NumberToString(uint64_t num) {
	// std::string r;
	// AppendNumberTo(&r, num);
	// return r;
	// }
	//
	// std::string EscapeString(const Slice& value) {
	// std::string r;
	// AppendEscapedStringTo(&r, value);
	// return r;
	// }
	//
	// bool ConsumeChar(Slice* in, char c) {
	// if (!in->empty() && (*in)[0] == c) {
	// in->remove_prefix(1);
	// return true;
	// } else {
	// return false;
	// }
	// }
	//
	public static long ConsumeDecimalNumber(String rest) {
		try {
			return Long.parseLong(rest);
		} catch (NumberFormatException e) {
			throw e;
		}
	}

}
