package com.wordnik.util;

import java.io.File;

public class FileCompressionThread extends Thread {
	String filename = null;

	public FileCompressionThread(String filename) {
		this.filename = filename;
	}

	public void run() {
		try {
			GZipUtil.createArchive(filename + ".gz", filename);
			new File(filename).delete();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
