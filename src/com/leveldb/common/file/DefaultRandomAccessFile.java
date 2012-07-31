package com.leveldb.common.file;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import com.leveldb.common.Slice;

/**
 * a mmf, read all data to buffer
 * 
 * @author Administrator
 * 
 */
public class DefaultRandomAccessFile extends _RandomAccessFile {
	private String fileName;
	MappedByteBuffer raf;
	RandomAccessFile raf1;

	@SuppressWarnings("unused")
	public DefaultRandomAccessFile(String iFileName) throws IOException {
		fileName = iFileName;
		raf1 = new RandomAccessFile(fileName, "rw");
//		if (fileName.contains(".sst")) {
//			System.out.println("Random Access Open: " + fileName);
//		}
//		FileChannel fc = raf1.getChannel();
//		long len = raf1.length();
//		raf = fc.map(MapMode.READ_WRITE, 0, len);
	}

	@Override
	public byte[] Read(long offset, int n, Slice result) {
		byte br[] = new byte[n];
		// raf.get(br, (int)offset, n);
		try {
			raf1.seek(offset);
			raf1.read(br);
		} catch (IOException e) {
			e = new IOException(e.toString() + "\n When seek @ offset: " + offset);
			e.printStackTrace();
		}
		result.setData_(br);
		return br;
	}

	@Override
	public void Close() {
		try {
			
			raf1.close();
//			if (fileName.contains("05.sst")) {
//				System.out.println("Random Access Close: " + fileName);
//			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public String FileName() {
		return fileName;
	}

}
