package tests;

import com.leveldb.util.util;

import de.jarnbjo.jsnappy.SnappyCompressor;

public class _compressionTest {
	public static void main(String args[]){
		byte[] b = new byte[]{0,0,2,-112};
		System.out.println(util.toInt(b));
	}

}
