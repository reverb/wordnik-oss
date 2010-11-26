package com.wordnik.system.mongodb;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

import org.bson.BSON;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.wordnik.util.AbstractFileWriter;
import com.wordnik.util.BinaryRotatingFileWriter;
import com.wordnik.util.RotatingFileWriter;

public class BaseMongoUtil {
	protected static String DATABASE_NAME = "test";
	protected static String DATABASE_USER_NAME = null;
	protected static String DATABASE_PASSWORD = null;
	protected static String DATABASE_HOST = "localhost";
	protected static boolean WRITE_JSON = false;
	protected static String OUTPUT_DIRECTORY = null;
	protected static NumberFormat LONG_FORMAT = new DecimalFormat("###,###");
	protected static NumberFormat NUMBER_FORMAT = new DecimalFormat("###.##");
	protected static NumberFormat PERCENT_FORMAT = new DecimalFormat("#.##%");
	
	protected static boolean COMPRESS_OUTPUT_FILES = false;
	protected static int UNCOMPRESSED_FILE_SIZE_MB = 100;
	protected static long WRITES = 0;
	protected static long REPORT_INTERVAL = 10000;

	private static DB db = null;

	protected static Map<String, AbstractFileWriter> WRITERS = new HashMap<String, AbstractFileWriter>();

	protected DB getDb() throws Exception {
		if(db == null){
			synchronized(this){
				Mongo m = new Mongo(DATABASE_HOST);
				if(DATABASE_USER_NAME != null){
					db = m.getDB(DATABASE_NAME);
					db.authenticate(DATABASE_USER_NAME, DATABASE_PASSWORD.toCharArray());
				}
				else{
					db = m.getDB(DATABASE_NAME);
				}
			}
		}
		return db;
	}

	protected void writeConnectivityDetailString(String collectionName) throws IOException {
		writeComment(collectionName, "##########################################");
		writeComment(collectionName, "##\texport created on " + new java.util.Date());
		writeComment(collectionName, "##\thost: " + DATABASE_HOST);
		writeComment(collectionName, "##\tdatabase: " + DATABASE_NAME);
		writeComment(collectionName, "##\tcollection: " + collectionName);
		writeComment(collectionName, "##########################################");
	}

	protected void writeComment(String collectionName, BasicDBObject comment) throws IOException {
/*		BinaryRotatingFileWriter writer = WRITERS.get(collectionName);
		if(writer == null){
			writer = new BinaryRotatingFileWriter(collectionName, OUTPUT_DIRECTORY, "bson", UNCOMPRESSED_FILE_SIZE_MB * 1048576L, COMPRESS_OUTPUT_FILES);
			WRITERS.put(collectionName, writer);
		}
		writer.write("//  "+comment.toString());
*/
	}

	protected void writeComment(String collectionName, String comment) throws IOException {
/*		BinaryRotatingFileWriter writer = WRITERS.get(collectionName);
		if(writer == null){
			writer = new BinaryRotatingFileWriter(collectionName, OUTPUT_DIRECTORY, "bson", UNCOMPRESSED_FILE_SIZE_MB * 1048576L, COMPRESS_OUTPUT_FILES);
			WRITERS.put(collectionName, writer);
		}
		writer.write("//\t"+comment);
*/
	}

	protected void writeIndex(String collectionName, BasicDBObject index) throws IOException {
/*		RotatingFileWriter writer = (RotatingFileWriter)WRITERS.get(collectionName);
		if(writer == null){
			writer = new RotatingFileWriter(collectionName, OUTPUT_DIRECTORY, "json", UNCOMPRESSED_FILE_SIZE_MB * 1048576L, COMPRESS_OUTPUT_FILES);
			WRITERS.put(collectionName, writer);
		}
		writer.write("// index:" + new String(BSON.encode(index)));
*/
	}

	protected void write(String collectionName, BasicDBObject dbo) throws IOException {
		if(WRITE_JSON){
			RotatingFileWriter writer = (RotatingFileWriter)WRITERS.get(collectionName);
			if(writer == null){
				writer = new RotatingFileWriter(collectionName, OUTPUT_DIRECTORY, "json", UNCOMPRESSED_FILE_SIZE_MB * 1048576L, COMPRESS_OUTPUT_FILES);
				WRITERS.put(collectionName, writer);
			}
			writer.write(dbo.toString());
		}
		else{
			BinaryRotatingFileWriter writer = (BinaryRotatingFileWriter)WRITERS.get(collectionName);
			if(writer == null){
				writer = new BinaryRotatingFileWriter(collectionName, OUTPUT_DIRECTORY, "bson", UNCOMPRESSED_FILE_SIZE_MB * 1048576L, COMPRESS_OUTPUT_FILES);
				WRITERS.put(collectionName, writer);
			}
			writer.write(BSON.encode(dbo));
		}
	}

	protected void closeWriter(String collectionName) throws IOException {
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

	public static void usage(){
		System.out.println(" -d : database name");
		System.out.println(" -h : hostname");
		System.out.println(" [-u : username]");
		System.out.println(" [-p : password]");
	}

	protected static int parseArg(int pos, String[] args) {
		int i = pos;
		switch (args[i].charAt(1)) {
		case 'J':
			WRITE_JSON = true;
			i++;
			break;
		case 'd':
			DATABASE_NAME = args[++i];
			break;
		case 'u':
			DATABASE_USER_NAME = args[++i];
			break;
		case 'p':
			DATABASE_PASSWORD = args[++i];
			break;
		case 'h':
			DATABASE_HOST = args[++i];
			break;
		default:
			return 0;
		}
		return i - pos;
	}
}