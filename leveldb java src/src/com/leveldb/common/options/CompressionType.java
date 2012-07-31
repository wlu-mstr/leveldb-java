package com.leveldb.common.options;

public class CompressionType {
	
		// NOTE: do not change the values of existing entries, as these are
		// part of the persistent format on disk.
		public static final byte kNoCompression = 0x0; 
		public static final byte kSnappyCompression = 0x1;
		
		public byte value;
		public CompressionType(byte ib){
			value = ib;
		}
		
		public static final CompressionType NoCompression = new CompressionType(kNoCompression);
	
}
