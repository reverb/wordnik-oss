package com.wordnik.util;

import java.io.IOException;
import java.io.OutputStream;

public class BinaryRotatingFileWriter extends AbstractFileWriter {
	OutputStream os = null;

	public BinaryRotatingFileWriter(String filePrefix, String destinationDirectory, String extension, long maxFileSizeInBytes, boolean compressOnRotate){
		super(filePrefix, destinationDirectory, extension, maxFileSizeInBytes, compressOnRotate);
	}

	public void write(byte[] bytesToWrite) throws IOException {
		getOutputStream().write(bytesToWrite);
		int sz = bytesToWrite.length;
		currentFileSize += sz;
		totalBytesWritten += sz;
		if(currentFileSize > maxFileSizeInBytes){
			rotateFile();
		}
	}

	OutputStream getOutputStream() throws IOException {
		if(!hasWriter()){
			os = getOutputStream(filePrefix);
		}
		return os;
	}

	@Override
	public void closeWriter() throws IOException {
		if(hasWriter()){
			os.close();
		}
	}

	@Override
	boolean hasWriter() {
		return os == null?false:true;
	}

	@Override
	void resetWriter() {
		if(hasWriter()){
			os = null;
		}		
	}

	public void write(String string) {
		
	}
}