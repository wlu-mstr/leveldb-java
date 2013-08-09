package com.leveldb.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.leveldb.common.Logger;

public class DefaultLogger extends Logger {
	private PrintWriter pWriter;

	public DefaultLogger(File log) {
		try {
			if (!log.exists()) {
				log.createNewFile();
			}
			pWriter = new PrintWriter(new FileWriter(log, true));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void Logv(String format, String... ap) {
		// append to log file
		Date dt = new Date();
		long longtime = dt.getTime();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

		pWriter.append("[" + sdf.format(longtime) + "\t" + format + "]\t");
		for (String msg : ap) {
			pWriter.append(msg + "; ");
		}
		pWriter.append("\n");

	}
	
	public void Close(){
		pWriter.close();
	}

}
