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

package com.wordnik.system.mongodb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
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

	private static Map<String, DB> DATABASE_CONNECTIONS = new HashMap<String, DB>();

	protected static Map<String, AbstractFileWriter> WRITERS = new HashMap<String, AbstractFileWriter>();

	protected DB getDb() throws Exception {
		return getDb(DATABASE_NAME);
	}

	protected DB getDb(String dbName) throws Exception {
		DB db = DATABASE_CONNECTIONS.get(dbName);
		if(db == null){
			synchronized(this){
				Mongo m = new Mongo(DATABASE_HOST);
				if(DATABASE_USER_NAME != null){
					db = m.getDB(dbName);
					db.authenticate(DATABASE_USER_NAME, DATABASE_PASSWORD.toCharArray());
					DATABASE_CONNECTIONS.put(dbName, db);
				}
				else{
					db = m.getDB(dbName);
					DATABASE_CONNECTIONS.put(dbName, db);
				}
			}
		}
		return db;
	}

	protected void removeSummaryFile(String name) {
		File file = new File(name + ".txt");
		if(file.exists() && !file.delete()){
			throw new RuntimeException("unable to remove summary file");
		}
	}

	protected void writeConnectivityDetailString(String collectionName) throws IOException {
		writeToSummaryFile(collectionName, "##########################################");
		writeToSummaryFile(collectionName, "##\texport created on " + new java.util.Date());
		writeToSummaryFile(collectionName, "##\thost: " + DATABASE_HOST);
		writeToSummaryFile(collectionName, "##\tdatabase: " + DATABASE_NAME);
		writeToSummaryFile(collectionName, "##\tcollection: " + collectionName);
		writeToSummaryFile(collectionName, "##########################################");
	}

	protected void writeObjectToSummaryFile(String collectionName, BasicDBObject comment) throws IOException {
		writeToSummaryFile(collectionName, comment.toString());
	}

	protected void writeToSummaryFile(String collectionName, String comment) throws IOException {
		String filename = collectionName + ".txt";
		Writer writer = new OutputStreamWriter(new FileOutputStream(new File(filename), true));
		writer.write(comment.toString());
		writer.write("\n");
		writer.close();
	}

	protected void writeIndexInfoToSummaryFile(String collectionName, BasicDBObject index) throws IOException {
		BasicDBObject i = (BasicDBObject)index.get("key");

		//	don't write the _id index
		if(!i.containsField("_id")){
			writeToSummaryFile(collectionName, i.toString());
		}
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