package com.leveldb.common;

import com.leveldb.util.util;

public class Status {
	// Create a success status.
	public Status() {
		state_ = null;
	}

	// Copy the specified status.

	// Return a success status.
	public static Status OK() {
		return new Status();
	}

	// Return error status of an appropriate type.
	public static Status NotFound(Slice msg, Slice msg2) {
		return new Status(Code.kNotFound, msg, msg2);
	}

	public static Status Corruption(Slice msg, Slice msg2) {
		return new Status(Code.kCorruption, msg, msg2);
	}

	public static Status NotSupported(Slice msg, Slice msg2) {
		return new Status(Code.kNotSupported, msg, msg2);
	}

	public static Status InvalidArgument(Slice msg, Slice msg2) {
		return new Status(Code.kInvalidArgument, msg, msg2);
	}

	public static Status IOError(Slice msg, Slice msg2) {
		return new Status(Code.kIOError, msg, msg2);
	}

	// Returns true iff the status indicates success.
	public boolean ok() {
		return (state_ == null);
	}

	// Returns true iff the status indicates a NotFound error.
	public boolean IsNotFound() {
		return code() == Code.kNotFound;
	}

	// Return a string representation of this status suitable for printing.
	// Returns the string "OK" for success.
	public String toString() {
		if (state_ == null) {
			return "OK";
		} else {
			String type = "";
			switch (code()) {
			case Code.kOk:
				type = "OK";
				break;
			case Code.kNotFound:
				type = "NotFound: ";
				break;
			case Code.kCorruption:
				type = "Corruption: ";
				break;
			case Code.kNotSupported:
				type = "Not implemented: ";
				break;
			case Code.kInvalidArgument:
				type = "Invalid argument: ";
				break;
			case Code.kIOError:
				type = "IO error: ";
				break;
			default:
				type = "Unknow code: " + code();
				break;
			}

			byte[] mess_ = new byte[state_.length - 5];
			System.arraycopy(state_, 5, mess_, 0, mess_.length);
			String mess = util.toString(mess_);

			return type + mess;
		}
	};

	// OK status has a NULL state_. Otherwise, state_ is a new[] array
	// of the following form:
	// state_[0..3] == length of message
	// state_[4] == code
	// state_[5..] == message
	private byte[] state_;

	class Code {
		static final int kOk = 0;
		static final int kNotFound = 1;
		static final int kCorruption= 2;
		static final int kNotSupported= 3;
		static final int kInvalidArgument= 4;
		static final int kIOError = 5;
		int value = 0;
		public Code(int c){
			value=c;
		}
	}

	public int code() {
		return ((state_ == null) ? Code.kOk : (state_[4]));
	}

	public Status(int code, Slice msg, Slice msg2) {
		assert (code != Code.kOk);
		if (msg2 == null || msg2.empty()) {
			msg2 = new Slice("");
		}
		String mess = util.toString(msg.data()) + ": "
				+ util.toString(msg2.data());
		byte[] mess_ = mess.getBytes();
		byte res[] = new byte[mess_.length + 5]; // 0...3 is actually not dummy
		System.arraycopy(util.toBytes(mess_.length), 0, res, 0, 4);
		// important
		int od = code;//.ordinal();
		res[4] = (byte) ((od) & 0xff);
		// message
		System.arraycopy(mess_, 0, res, 5, mess_.length);
		state_ = res;
	};

	public static byte[] CopyState(byte[] s) {
		byte[] ret = new byte[s.length];
		System.arraycopy(s, 0, ret, 0, s.length);
		return ret;
	}

	public Status(Status s) {
		state_ = (s.state_ == null) ? null : CopyState(s.state_);
	}
	
	public void Status_(Status s) {
		state_ = (s.state_ == null) ? null : CopyState(s.state_);
	}
}
