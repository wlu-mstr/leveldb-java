package com.leveldb.common.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicBoolean;

import com.leveldb.common.Slice;
import com.leveldb.common.Status;

public class DefaultWritableFile extends _WritableFile {

	private FileChannel fileChannel;
	private final AtomicBoolean closed = new AtomicBoolean();

	private String filename_;
	private int page_size_;
	private int map_size_; // How much extra memory to map at a time

	private int file_offset_; // Offset of base_ in file
	private int block_offset;

	RandomAccessFile raf1;
	//private FileChannel fc;
	//private MappedByteBuffer raf;

	private int Roundup(int x, int y) {
		return ((x + y - 1) / y) * y;
	}

	public DefaultWritableFile(String iFileName, int iPageSize, int iMapSize) {
		filename_ = iFileName;
		page_size_ = iPageSize;
		map_size_ = Roundup(iMapSize, page_size_);
		file_offset_ = 0;
		block_offset = map_size_;

		try {
			if(filename_.contains("05.sst")){
				@SuppressWarnings("unused")
				int d = 0;
			}
			raf1 = new RandomAccessFile(filename_, "rw");
			
//			System.err.println("Opened " + filename_);
			//fc = raf1.getChannel();
			//raf = fc.map(MapMode.READ_WRITE, 0, map_size_);

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private int expand(int ms) {
		if (ms < 1 << 20) {
			return ms * 2;
		}
		return ms;
	}

	@Override
	public Status Append(Slice data) {
		byte src[] = data.data();
		int size = data.size();

		int R = size;
		int src_offset = 0;

		try {
//			while (R > 0) {
//				if (R > map_size_) {
//					raf.put(src, src_offset, map_size_);
//					R -= map_size_;
//					src_offset += map_size_;
//					file_offset_ += map_size_;
//					map_size_ = expand(map_size_);
//					raf = fc.map(MapMode.READ_WRITE, file_offset_, map_size_);
//				} else {
//					raf.put(src, src_offset, R);
//					//raf.compact();
//					file_offset_ += R; // like size
//					block_offset = R; // just R
//					R = 0;
//				}
//			}
			
			raf1.write(src);

		} catch (IOException e) {
			e.printStackTrace();
			return Status.IOError(new Slice(e.getMessage()), null);
		}

		return Status.OK();
	}

	@Override
	public Status Close() {
		try {
			raf1.close();
//			System.err.println("Closed " + filename_);
		} catch (IOException e) {
			e.printStackTrace();
			return Status.IOError(new Slice(e.toString()), null);
		}
		return Status.OK();
	}

	@Override
	public Status Flush() {
		return Status.OK();
	}

	@Override
	public Status Sync() {
		//raf.force();
		return Status.OK();
	}
	
	public static void main(String args[]) throws IOException{
		DefaultWritableFile dwf= new DefaultWritableFile("C://wf", 1024, 2000);
		byte[] ba = new byte[1024*2 + 30];
		for(int i = 0; i < ba.length; i++){
			ba[i] = 0x48;
		}
		                     
		Slice ts = new Slice(ba);
		dwf.Append(ts);
		dwf.Sync();
		dwf.Close();
		
		dwf= new DefaultWritableFile("C://wf", 1024, 2000);
		dwf.Append(ts);
		dwf.Sync();
		dwf.Close();
		
		DefaultRandomAccessFile drf = new DefaultRandomAccessFile("C://wf");
		Slice rs = new Slice();
		byte[] ra = drf.Read(0, ba.length, rs);
		System.out.print(ra.length);
	}

}
