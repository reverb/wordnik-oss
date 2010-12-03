package com.wordnik.system.mongodb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.bson.BSON;

import com.mongodb.BasicDBObject;
import com.wordnik.util.AbstractFileWriter;
import com.wordnik.util.BinaryRotatingFileWriter;
import com.wordnik.util.RotatingFileWriter;

public class OplogFileWriter implements OplogRecordProcessor {
	protected String outputDirectory;
	protected String collectionName = "oplog";
	protected static boolean WRITE_JSON = false;
	protected static boolean COMPRESS_OUTPUT_FILES = false;
	protected static int UNCOMPRESSED_FILE_SIZE_MB = 100;
	protected static Map<String, AbstractFileWriter> WRITERS = new HashMap<String, AbstractFileWriter>();

	@Override
	public void processRecord(BasicDBObject dbo) throws Exception {
		if(WRITE_JSON){
			RotatingFileWriter writer = (RotatingFileWriter)WRITERS.get(collectionName);
			if(writer == null){
				writer = new RotatingFileWriter(collectionName, outputDirectory, "json", UNCOMPRESSED_FILE_SIZE_MB * 1048576L, COMPRESS_OUTPUT_FILES);
				WRITERS.put(collectionName, writer);
			}
			writer.write(dbo.toString());
		}
		else{
			BinaryRotatingFileWriter writer = (BinaryRotatingFileWriter)WRITERS.get(collectionName);
			if(writer == null){
				writer = new BinaryRotatingFileWriter(collectionName, outputDirectory, "bson", UNCOMPRESSED_FILE_SIZE_MB * 1048576L, COMPRESS_OUTPUT_FILES);
				WRITERS.put(collectionName, writer);
			}
			writer.write(BSON.encode(dbo));
		}
	}

	public String getOutputDirectory() {
		return outputDirectory;
	}

	public void setOutputDirectory(String outputDirectory) {
		this.outputDirectory = outputDirectory;
	}

	@Override
	public void close(String string) throws IOException {
		AbstractFileWriter writer;
		if(WRITE_JSON){
			writer = (RotatingFileWriter)WRITERS.get(collectionName);
		}
		else{
			writer = WRITERS.get(collectionName);
		}
		if(writer != null){
			writer.close();
		}
	}
}
