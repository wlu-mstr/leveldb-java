package com.leveldb.common.file;

import com.leveldb.common.Env;
import com.leveldb.common.Slice;
import com.leveldb.common.Status;
import com.leveldb.util.logging;

// TODO: seems that there are so many functions to be add here;
// I wanna fill each of them one by one
public class filename {
	public static String MakeFileName(String name, long number, String suffix) {
		return name + String.format("/%06d.", number) + suffix;
	}

	public static String LogFileName(String name, long number) {
		assert (number > 0);
		return MakeFileName(name, number, "log");
	}

	public static String LockFileName(String dbname) {
		return dbname + "/LOCK";
	}

	public static String TempFileName(String dbname, long number) {
		assert (number > 0);
		return MakeFileName(dbname, number, "dbtmp");
	}

	public static String CurrentFileName(String dbname) {
		return dbname + "/CURRENT";
	}

	// Owned filenames have the form:
	// dbname/CURRENT
	// dbname/LOCK
	// dbname/LOG
	// dbname/LOG.old
	// dbname/MANIFEST-[0-9]+
	// dbname/[0-9]+.(log|sst)
	public static long ParseFileName(String fname, FileType type)
			throws Exception {
		String rest = fname;
		long number = -1;
		if (rest.compareTo("CURRENT") == 0) {
			number = 0;
			type.value = FileType.kCurrentFile;
		} else if (rest.compareTo("LOCK") == 0) {
			number = 0;
			type.value = FileType.kDBLockFile;
		} else if (rest.compareTo("LOG") == 0 || rest.compareTo("LOG.old") == 0) {
			number = 0;
			type.value = FileType.kInfoLogFile;
		} else if (rest.startsWith("MANIFEST-")) {
			rest = rest.substring(("MANIFEST-").length());
			long n = 0;
			try {
				n = logging.ConsumeDecimalNumber(rest);
			} catch (Exception e) {
				throw e;
			}

			type.value = FileType.kDescriptorFile;
			number = n;
		} else {
			// Avoid strtoull() to keep filename format independent of the
			// current locale
			long num;
			int dotpos = rest.indexOf(".");
			if (dotpos == -1) {
				throw new Exception(
						"prefix error, hint: should be [0-9]+.(log|sst|...)");
			}
			try {
				num = logging.ConsumeDecimalNumber(rest.substring(0, dotpos));
			} catch (Exception e) {
				throw e;
			}
			String suffix = rest.substring(dotpos);
			if (suffix.compareTo(".log") == 0) {
				type.value = FileType.kLogFile;
			} else if (suffix.compareTo(".sst") == 0) {
				type.value = FileType.kTableFile;
			} else if (suffix.compareTo(".dbtmp") == 0) {
				type.value = FileType.kTempFile;
			} else {
				throw new Exception(
						"suffix error, hint: should be [0-9]+.(log|sst|...)");
			}
			number = num;
		}
		return number;
	}

	public static String TableFileName(String name, long number) {
		assert (number > 0);
		return MakeFileName(name, number, "sst");
	}

	/*
	 * write CURRENT to tmp file and rename tmp file
	 */
	public static Status SetCurrentFile(Env env, String dbname,
			long descriptor_number) {
		// Remove leading "dbname/" and add newline to manifest file name
		String manifest = DescriptorFileName(dbname, descriptor_number);
		String contents = manifest;
		assert (manifest.startsWith(dbname + "/"));
		contents = contents.substring(dbname.length() + 1);
		String tmp = filename.TempFileName(dbname, descriptor_number);
		Status s = Env.WriteStringToFileSync(env, new Slice(contents + "\n"),
				tmp);
		if (s.ok()) {
			s = env.RenameFile(tmp, filename.CurrentFileName(dbname));
		}
		if (!s.ok()) {
			env.DeleteFile(tmp);
		}
		return s;
	}

	// db//MANIFEST-000123
	public static String DescriptorFileName(String dbname,
			long manifest_file_number_) {
		assert (manifest_file_number_ > 0);
		return dbname + "/MANIFEST-"
				+ String.format("%06d", manifest_file_number_);
	}

	// //////////////////////////////////////////////////////////////////////
	// / test cases goes here
	static class FileNameTest {
		public void Parse() {
			FileType type = new FileType();
			long number = -1;

			// Successful parses
			class A {
				String fname;
				long number;
				int type;

				public A(String f, long n, int t) {
					fname = f;
					number = n;
					type = t;
				}
			}
			A cases[] = {
					new A("100.log", 100, FileType.kLogFile),
					new A("0.log", 0, FileType.kLogFile),
					new A("0.sst", 0, FileType.kTableFile),
					new A("CURRENT", 0, FileType.kCurrentFile),
					new A("LOCK", 0, FileType.kDBLockFile),
					new A("MANIFEST-2", 2, FileType.kDescriptorFile),
					new A("MANIFEST-7", 7, FileType.kDescriptorFile),
					new A("LOG", 0, FileType.kInfoLogFile),
					new A("LOG.old", 0, FileType.kInfoLogFile),
					new A("446744073709551615.log", 446744073709551615l,
							FileType.kLogFile) };
			for (int i = 0; i < cases.length; i++) {
				String f = cases[i].fname;
				try {
					number = ParseFileName(f, type);
				} catch (Exception e) {
					e.printStackTrace();
				}
				// ASSERT_TRUE(ParseFileName(f, &number, &type)) << f;
				ASSERT_TRUE(cases[i].type == type.value, f);
				ASSERT_TRUE(cases[i].number == number, f);
			}

			// Errors
			String errors[] = { "", "foo", "foo-dx-100.log", ".log", "",
					"manifest", "CURREN", "CURRENTX", "MANIFES", "MANIFEST",
					"MANIFEST-", "XMANIFEST-3", "MANIFEST-3x", "LOC", "LOCKx",
					"LO", "LOGx", "18446744073709551616.log",
					"184467440737095516150.log", "100", "100.", "100.lop" };
			ASSERT_TRUE(true, "-------------------");
			for (int i = 0; i < errors.length; i++) {
				String f = errors[i];
				try {
					ASSERT_TRUE(ParseFileName(f, type) != -1, f);
				} catch (Exception e) {
					ASSERT_TRUE(true, "error   " + f);
				}
			}
		}

		void Construction() {
			FileType type = new FileType();
			String fname;

			try {

				fname = CurrentFileName("foo");
				ASSERT_TRUE("foo/".compareTo(fname.substring(0, 4)) == 0, fname);
				ASSERT_TRUE(ParseFileName(fname.substring(4), type) == 0,
						fname.substring(4));

				ASSERT_TRUE(FileType.kCurrentFile == type.value, type.value
						+ "");

				fname = LockFileName("foo");
				ASSERT_TRUE("foo/".compareTo(fname.substring(0, 4)) == 0, fname);
				ASSERT_TRUE(ParseFileName(fname.substring(4), type) == 0,
						fname.substring(4));
				ASSERT_TRUE(FileType.kDBLockFile == type.value, type.value + "");

				fname = LogFileName("foo", 192);
				ASSERT_TRUE("foo/".compareTo(fname.substring(0, 4)) == 0, fname);
				ASSERT_TRUE(ParseFileName(fname.substring(4), type) == 192,
						fname.substring(4));
				ASSERT_TRUE(FileType.kLogFile == type.value, type.value + "");

				fname = TableFileName("bar", 200);
				ASSERT_TRUE("bar/".compareTo(fname.substring(0, 4)) == 0, fname);
				ASSERT_TRUE(ParseFileName(fname.substring(4), type) == 200,
						fname.substring(4));
				ASSERT_TRUE(FileType.kTableFile == type.value, type.value + "");

				fname = DescriptorFileName("bar", 100);
				ASSERT_TRUE("bar/".compareTo(fname.substring(0, 4)) == 0, fname);
				ASSERT_TRUE(ParseFileName(fname.substring(4), type) == 100,
						fname.substring(4));
				ASSERT_TRUE(FileType.kDescriptorFile == type.value, type.value
						+ "");

				fname = TempFileName("tmp", 999);
				ASSERT_TRUE("tmp/".compareTo(fname.substring(0, 4)) == 0, fname);
				ASSERT_TRUE(ParseFileName(fname.substring(4), type) == 999,
						fname.substring(4));
				ASSERT_TRUE(FileType.kTempFile == type.value, type.value + "");

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		private void ASSERT_TRUE(boolean b, String f) {
			if (b) {
				System.out.println(f);
			}

		}
	}

	public static void main(String args[]) {
		FileNameTest fnt = new FileNameTest();
		try {
			// fnt.Parse();
			fnt.Construction();
		} catch (Exception e) {
			// e.printStackTrace();
		}
	}

	public static String InfoLogFileName(final String dbname) {
		return dbname + "/LOG";
	}

	// Return the name of the old info log file for "dbname".
	public static String OldInfoLogFileName(final String dbname) {
		return dbname + "/LOG.old";
	}

}
