package com.leveldb.common.db;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import com.leveldb.common.Slice;
import com.leveldb.common.comparator.BytewiseComparatorImpl;
import com.leveldb.common.comparator.InternalKeyComparator;
import com.leveldb.common.db.FileMetaData;
import com.leveldb.common.db.InternalKey;
import com.leveldb.common.version.Version;
import com.leveldb.util.SequenceNumber;
import com.leveldb.util.ValueType;
import com.leveldb.util.util;

public class VersionSetTest extends TestCase {
	// ////////////////////////////////////////////////////////////////////////////////////////////
	// /// test of VersionSet

	List<FileMetaData> files_;
	boolean disjoint_sorted_files_;

	public VersionSetTest() {
		disjoint_sorted_files_ = true;
		files_ = new ArrayList<FileMetaData>();
	}

	void Add(byte[] smallest, byte[] largest, SequenceNumber smallest_seq,
			SequenceNumber largest_seq) {
		FileMetaData f = new FileMetaData();
		f.number = files_.size() + 1;
		f.smallest = new InternalKey(new Slice(smallest), smallest_seq,
				ValueType.TypeValue);
		f.largest = new InternalKey(new Slice(largest), largest_seq,
				ValueType.TypeValue);
		files_.add(f);
	}

	void Add(byte[] smallest, byte[] largest) {
		Add(smallest, largest, new SequenceNumber(100), new SequenceNumber(100));
	}

	int Find(byte[] key) {
		InternalKey target = new InternalKey(new Slice(key),
				new SequenceNumber(100), ValueType.TypeValue);
		InternalKeyComparator cmp = new InternalKeyComparator(
				BytewiseComparatorImpl.getInstance());
		return Version.FindFile(cmp, files_, target.Encode());
	}

	int Find(String k) {
		return Find(util.toBytes(k));
	}

	boolean Overlaps(Slice smallest, Slice largest) {
		InternalKeyComparator cmp = new InternalKeyComparator(
				BytewiseComparatorImpl.getInstance());
		Slice s = (smallest != null ? smallest : new Slice());
		Slice l = (largest != null ? largest : new Slice());
		return Version.SomeFileOverlapsRange(cmp, disjoint_sorted_files_,
				files_, (smallest != null ? s : null), (largest != null ? l
						: null));
	}

	boolean Overlaps(String s1, String s2) {
		return Overlaps(s1 == null ? null : new Slice(s1), s2 == null ? null
				: new Slice(s2));
	}

	boolean Overlaps2(Slice smallest, Slice largest) {
		return Overlaps(smallest, largest);
	}

	void ASSERT_TRUE(boolean b) {
		assertEquals(b, true);
	}

	void ASSERT_EQ(int a, int b) {
		assertEquals(a, b);
	}

	public void testEmpty() {
		ASSERT_EQ(0, Find(util.toBytes("foo")));
		ASSERT_TRUE(!Overlaps(new Slice("a"), new Slice("z")));
		ASSERT_TRUE(!Overlaps(null, new Slice("z")));
		ASSERT_TRUE(!Overlaps(new Slice("a"), null));
		ASSERT_TRUE(!Overlaps2(null, null));
	}

	public void testSingle() {
		Add(util.toBytes("p"), util.toBytes("q"));
		ASSERT_EQ(0, Find("a"));
		ASSERT_EQ(0, Find("p"));
		ASSERT_EQ(0, Find("p1"));
		ASSERT_EQ(0, Find("q"));
		ASSERT_EQ(1, Find("q1"));
		ASSERT_EQ(1, Find("z"));

		ASSERT_TRUE(!Overlaps("a", "b"));
		ASSERT_TRUE(!Overlaps("z1", "z2"));
		ASSERT_TRUE(Overlaps("a", "p"));
		ASSERT_TRUE(Overlaps("a", "q"));
		ASSERT_TRUE(Overlaps("a", "z"));
		ASSERT_TRUE(Overlaps("p", "p1"));
		ASSERT_TRUE(Overlaps("p", "q"));
		ASSERT_TRUE(Overlaps("p", "z"));
		ASSERT_TRUE(Overlaps("p1", "p2"));
		ASSERT_TRUE(Overlaps("p1", "z"));
		ASSERT_TRUE(Overlaps("q", "q"));//
		ASSERT_TRUE(Overlaps("q", "q1"));//

		ASSERT_TRUE(!Overlaps(null, new Slice("j")));
		ASSERT_TRUE(!Overlaps(new Slice("r"), null));
		ASSERT_TRUE(Overlaps(null, new Slice("p")));
		ASSERT_TRUE(Overlaps(null, new Slice("p1")));
		ASSERT_TRUE(Overlaps(new Slice("q"), null));//
		ASSERT_TRUE(Overlaps2(null, null));
	}

	public void testMuitly() {
		Add(util.toBytes("150"), util.toBytes("200"));// 0
		Add(util.toBytes("200"), util.toBytes("250"));// 1
		Add(util.toBytes("300"), util.toBytes("350"));// 2
		Add(util.toBytes("400"), util.toBytes("450"));// 3
		ASSERT_EQ(0, Find("100"));
		ASSERT_EQ(0, Find("150"));
		ASSERT_EQ(0, Find("151"));
		ASSERT_EQ(0, Find("199"));
		ASSERT_EQ(0, Find("200"));
		ASSERT_EQ(1, Find("201"));
		ASSERT_EQ(1, Find("249"));
		ASSERT_EQ(1, Find("250"));
		ASSERT_EQ(2, Find("251"));
		ASSERT_EQ(2, Find("299"));
		ASSERT_EQ(2, Find("300"));
		ASSERT_EQ(2, Find("349"));
		ASSERT_EQ(2, Find("350"));
		ASSERT_EQ(3, Find("351"));
		ASSERT_EQ(3, Find("400"));
		ASSERT_EQ(3, Find("450"));
		ASSERT_EQ(4, Find("451"));

		ASSERT_TRUE(!Overlaps("100", "149"));
		ASSERT_TRUE(!Overlaps("251", "299"));
		ASSERT_TRUE(!Overlaps("451", "500"));
		ASSERT_TRUE(!Overlaps("351", "399"));

		ASSERT_TRUE(Overlaps("100", "150"));
		ASSERT_TRUE(Overlaps("100", "200"));
		ASSERT_TRUE(Overlaps("100", "300"));
		ASSERT_TRUE(Overlaps("100", "400"));
		ASSERT_TRUE(Overlaps("100", "500"));
		ASSERT_TRUE(Overlaps("375", "400"));
		ASSERT_TRUE(Overlaps("450", "450"));
		ASSERT_TRUE(Overlaps("450", "500"));
	}

	public void testMultipleNullBoundaries() {
		Add(util.toBytes("150"), util.toBytes("200"));
		Add(util.toBytes("200"), util.toBytes("250"));
		Add(util.toBytes("300"), util.toBytes("350"));
		Add(util.toBytes("400"), util.toBytes("450"));
		ASSERT_TRUE(!Overlaps(null, "149"));
		ASSERT_TRUE(!Overlaps("451", null));
		ASSERT_TRUE(Overlaps2(null, null));
		ASSERT_TRUE(Overlaps(null, "150"));
		ASSERT_TRUE(Overlaps(null, "199"));
		ASSERT_TRUE(Overlaps(null, "200"));
		ASSERT_TRUE(Overlaps(null, "201"));
		ASSERT_TRUE(Overlaps(null, "400"));
		ASSERT_TRUE(Overlaps(null, "800"));
		ASSERT_TRUE(Overlaps("100", null));
		ASSERT_TRUE(Overlaps("200", null));
		ASSERT_TRUE(Overlaps("449", null));
		ASSERT_TRUE(Overlaps("450", null));
	}

	public void testOverlapSequenceChecks() {
		Add(util.toBytes("200"), util.toBytes("200"), new SequenceNumber(5000),
				new SequenceNumber(3000));
		ASSERT_TRUE(!Overlaps("199", "199"));
		ASSERT_TRUE(!Overlaps("201", "300"));
		ASSERT_TRUE(Overlaps("200", "200"));
		ASSERT_TRUE(Overlaps("190", "200"));
		ASSERT_TRUE(Overlaps("200", "210"));
	}

	public void testOverlappingFiles() {
		Add(util.toBytes("150"), util.toBytes("600"));
		Add(util.toBytes("400"), util.toBytes("500"));
		disjoint_sorted_files_ = false;
		ASSERT_TRUE(!Overlaps("100", "149"));
		ASSERT_TRUE(!Overlaps("601", "700"));
		ASSERT_TRUE(Overlaps("100", "150"));
		ASSERT_TRUE(Overlaps("100", "200"));
		ASSERT_TRUE(Overlaps("100", "300"));
		ASSERT_TRUE(Overlaps("100", "400"));
		ASSERT_TRUE(Overlaps("100", "500"));
		ASSERT_TRUE(Overlaps("375", "400"));
		ASSERT_TRUE(Overlaps("450", "450"));
		ASSERT_TRUE(Overlaps("450", "500"));
		ASSERT_TRUE(Overlaps("450", "700"));
		ASSERT_TRUE(Overlaps("600", "700"));
	}

	public static void main(String args[]) {
		TestSuite t = new TestSuite(VersionSetTest.class.getName());
		t.addTestSuite(VersionSetTest.class);
		TestRunner.run(t);
	}

}
