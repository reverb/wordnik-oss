package com.wordnik.system.mongodb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.StringTokenizer;

public class RotatingFileWriter {
	String filePrefix;
	String destinationDirectory;
	String extension;
	boolean compressOnRotate = false;
	
	long totalBytesWritten;
	long maxFileSizeInBytes;
	long currentFileSize;
	Writer writer;
	
	public RotatingFileWriter(String filePrefix, String destinationDirectory, String extension, long maxFileSizeInBytes, boolean compressOnRotate){
		this.filePrefix = filePrefix;
		this.destinationDirectory = destinationDirectory;
		this.extension = extension;
		this.maxFileSizeInBytes = maxFileSizeInBytes;
		this.compressOnRotate = compressOnRotate;
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
		if(writer == null){
			openFile(filePrefix);
		}
		return writer;
	}

	void rotateFile() throws IOException {
		writer.close();
		currentFileSize = 0;
		File file = new File(getPath(getFilename()));
		String outputFilename = getFilename(getNextIncrement());
		file.renameTo(new File(getPath(outputFilename)));
		if(compressOnRotate){
			new FileCompressionThread(getPath(outputFilename)).start();
		}
		writer = null;
	}

	String getFilename(){
		return filePrefix + "." + extension;
	}
	
	String getFilename(String increment){
		return filePrefix + "." + increment + "." + extension;
	}

	Writer openFile(String prefix) throws IOException {
		File file = new File(getPath(getFilename()));
		if(writer != null){
			try{
				writer.close();
			}
			catch(Exception e){}
		}
		if(file.exists()){
			file.renameTo(new File(getPath(getFilename(getNextIncrement()))));
		}
		OutputStream out = new FileOutputStream(new File(getPath(getFilename())));
		writer = new OutputStreamWriter(out, "UTF-8");
		currentFileSize = 0;
		return writer;
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
					String sub = tk.nextToken();
					int inc = Integer.parseInt(sub);
					if(inc > lastInc){
						lastInc = inc;
					}
				}
			}
		}
		NumberFormat nf = new DecimalFormat("0000");
		return nf.format(lastInc+1);
	}

	public long getTotalBytesWritten() {
		return totalBytesWritten;
	}
	
	class FileCompressionThread extends Thread {
		String filename = null;
		public FileCompressionThread(String filename){
			this.filename = filename;
		}

		public void run(){
			try{
				System.out.println("compressing file " + filename);
				ZipUtil.createArchive(getPath(filename + ".zip"), null, getPath(filename));
				new File(getPath(filename)).delete();
				System.out.println("done compressing " + filename);
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
	}
}
