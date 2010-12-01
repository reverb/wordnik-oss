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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.StringTokenizer;

public abstract class AbstractFileWriter {
	protected String filePrefix;
	protected String destinationDirectory;
	protected String extension;
	protected boolean compressOnRotate = false;

	protected long totalBytesWritten;
	protected long maxFileSizeInBytes;
	protected long currentFileSize;

	public AbstractFileWriter(String filePrefix, String destinationDirectory, String extension, long maxFileSizeInBytes, boolean compressOnRotate) {
		this.filePrefix = filePrefix;
		this.destinationDirectory = destinationDirectory;
		this.extension = extension;
		this.maxFileSizeInBytes = maxFileSizeInBytes;
		this.compressOnRotate = compressOnRotate;

		if(destinationDirectory != null){
			File file = new File(destinationDirectory);
			if(!file.exists()){
				if(!file.mkdir()){
					throw new RuntimeException("output directory doesn't exist and cannot be created");
				}
			}
		}
	}
	abstract boolean hasWriter();
	public abstract void closeWriter() throws IOException;
	

	String getFilename(){
		return filePrefix + "." + extension;
	}
	
	String getFilename(String increment){
		return filePrefix + "." + increment + "." + extension;
	}

	public long getTotalBytesWritten() {
		return totalBytesWritten;
	}
	
	String getPath(String filename) {
		if(destinationDirectory != null){
			return destinationDirectory + File.separator + filename;
		}
		return filename;
	}

	String getNextIncrement() throws IOException {
		File[] files = null;
		if(destinationDirectory == null){
			files = new File(getPath(new File (".").getCanonicalPath())).listFiles();
		}
		else{
			files = new File(getPath("")).listFiles();
		}

		if(files == null){
			return "0000";
		}
		int lastInc = 0;
		for(File file : files){
			if(file.getName().startsWith(filePrefix)){
				StringTokenizer tk = new StringTokenizer(file.getName(), ".");
				if(tk.countTokens() > 2){
					tk.nextToken();
					//	use 2nd token
					while(tk.hasMoreTokens()){
						try{
							String sub = tk.nextToken();
							int inc = Integer.parseInt(sub);
							if(inc > lastInc){
								lastInc = inc;
							}
							break;
						}
						catch(NumberFormatException ex){
							//	get next one
						}
					}
				}
			}
		}
		NumberFormat nf = new DecimalFormat("0000");
		return nf.format(lastInc+1);
	}
	
	public void close() throws IOException {
		rotateFile();
		closeWriter();
	}
	
	void rotateFile() throws IOException {
		if(!hasWriter()){
			return;
		}
		closeWriter();
		currentFileSize = 0;
		File file = new File(getPath(getFilename()));
		String outputFilename = getFilename(getNextIncrement());
		file.renameTo(new File(getPath(outputFilename)));
		if(compressOnRotate){
			new FileCompressionThread(getPath(outputFilename)).start();
		}
		resetWriter();
	}

	OutputStream getOutputStream(String filePrefix) throws IOException {
		File file = new File(getPath(getFilename()));
		if(hasWriter()){
			try{
				closeWriter();
			}
			catch(Exception e){}
		}
		if(file.exists()){
			file.renameTo(new File(getPath(getFilename(getNextIncrement()))));
		}
		return new FileOutputStream(new File(getPath(getFilename())));
	}
	
	abstract void resetWriter();
}
