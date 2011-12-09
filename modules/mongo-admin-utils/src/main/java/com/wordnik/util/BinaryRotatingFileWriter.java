// Copyright (C) 2010  Wordnik, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your 
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty 
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser 
// General Public License for more details.  You should have received a copy 
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.

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
}