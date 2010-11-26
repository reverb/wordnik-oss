package com.wordnik.util;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

public class RotatingFileWriter extends AbstractFileWriter {
	Writer writer;

	public RotatingFileWriter(String filePrefix, String destinationDirectory, String extension, long maxFileSizeInBytes, boolean compressOnRotate){
		super(filePrefix, destinationDirectory, extension, maxFileSizeInBytes, compressOnRotate);
	}

	public void write(String stringToWrite) throws IOException {
		getWriter().write(stringToWrite + "\n");
		int sz = stringToWrite.length();
		currentFileSize += sz;
		totalBytesWritten += sz;
		if(currentFileSize > maxFileSizeInBytes){
			rotateFile();
		}
	}
	
	public void close() throws IOException {
		rotateFile();
	}

	Writer getWriter() throws IOException {
		if(!hasWriter()){
			openFile(filePrefix);
		}
		return writer;
	}
	
	boolean hasWriter(){
		if(writer == null){
			return false;
		}
		return true;
	}
	
	public void closeWriter() throws IOException {
		if(hasWriter()){
			writer.close();
		}
	}

	Writer openFile(String prefix) throws IOException {
		writer = new OutputStreamWriter(getOutputStream(prefix), "UTF-8");
		currentFileSize = 0;
		return writer;
	}
	
	void resetWriter(){
		if(hasWriter()){
			writer = null;
		}
	}
}
