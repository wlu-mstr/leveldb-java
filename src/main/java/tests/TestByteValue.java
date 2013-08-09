package tests;

import java.util.Random;

import com.leveldb.util.util;


public class TestByteValue {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		String s = util.RandomString(new Random(), 10);
		byte[] k = util.RandomKey(new Random(), 4);
		System.out.println(util.toString(k) + " : " + s);
	}

}
