package com.leveldb.common.file;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.leveldb.common.Slice;
import com.leveldb.common.Status;

/*
 */
public class DefaultSequentialFile extends _SequentialFile {

	private String filename_;
	BufferedInputStream in = null;

	public DefaultSequentialFile(String iFileName) {
		filename_ = iFileName;
		try {
			in = new BufferedInputStream(new FileInputStream(filename_));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public byte[] Read(int n, Slice result) {
		byte[] scratch = new byte[n];
		int size = -1;
		try {
			size = in.read(scratch);
		} catch (IOException e) {
			e.printStackTrace();
			try {
				in.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}

		}
		//
		if(size == -1){ 
			return null;
		}
		result.setData_(scratch);
		result.setSize_(size);
		
		return scratch;

	}

	@Override
	public Status Skip(long n) {
		try {
			in.skip(n);
		} catch (IOException e) {
			e.printStackTrace();
			return Status.IOError(new Slice(e.toString()), null);
		}
		return Status.OK();
	}
	
	public void Close(){
		try {
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
