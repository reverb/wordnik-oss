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

import java.io.BufferedInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.zip.GZIPInputStream;

import org.bson.BSONDecoder;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BSONTimestamp;

import com.mongodb.BasicDBObject;
import com.wordnik.util.PrintFormat;

public class ReplayUtil extends MongoUtil {
	protected static String INPUT_DIR;
	protected static String COLLECTION_STRING;
	protected static String COLLECTION_MAPPING_STRING;
	protected static String DATABASE_MAPPING_STRING;
	protected static Map<String, String> COLLECTION_MAPPING = new HashMap<String, String>();
	protected static Map<String, String> DATABASE_MAPPING = new HashMap<String, String>();
	protected static Set<String> COLLECTIONS_TO_SKIP = new HashSet<String>();
	protected static Set<String> COLLECTIONS_TO_ADD = new HashSet<String>();
	protected static BSONTimestamp AFTER_TIMESTAMP = null;
	protected static BSONTimestamp BEFORE_TIMESTAMP = null;
	protected static boolean ONLY_COLLECTION_EXCLUSIONS = true;
	protected static Map<String, String> NAMESPACE_COLLECTION_MAP = new HashMap<String, String>();
	
	protected static String DEST_DATABASE_NAME = "test";
	protected static String DEST_DATABASE_USER_NAME = null;
	protected static String DEST_DATABASE_PASSWORD = null;
	protected static String DEST_DATABASE_HOST = "localhost";

	protected static long REPORT_INTERVAL = 10000;

	public static void main(String ... args){
		if(!parseArgs(args)){
			usage();
			return;
		}
		if(INPUT_DIR == null){
			usage();
			return;
		}
		new ReplayUtil().run();
	}
	
	protected static void selectCollections(){
		if(COLLECTION_STRING != null){
			String[] collectionNames = COLLECTION_STRING.split(",");
			for(String collectionName : collectionNames){
				if(collectionName.startsWith("!")){
					//	skip it
					COLLECTIONS_TO_SKIP.add(collectionName.substring(1));
				}
				else{
					ONLY_COLLECTION_EXCLUSIONS = false;
					COLLECTIONS_TO_ADD.add(collectionName);
				}
			}
		}
	}

	protected static void createMappings(String databaseMappingString, String collectionMappingString, Map<String, String> databaseMappings, Map<String, String> collectionMappings){
		if(databaseMappingString != null){
			StringTokenizer tk = new StringTokenizer(databaseMappingString, ",");
			while(tk.hasMoreElements()){
				String[] split = tk.nextToken().split("\\=");
				databaseMappings.put(split[0], split[1]);
			}
		}

		if(collectionMappingString != null){
			StringTokenizer tk = new StringTokenizer(collectionMappingString, ",");
			while(tk.hasMoreElements()){
				String[] split = tk.nextToken().split("\\=");
				collectionMappings.put(split[0], split[1]);
			}
		}	
	}

	protected void run(){
		//	decide what collections to process
		selectCollections();

		OplogReplayWriter util = new OplogReplayWriter();

		//	create any re-mappings
		Map<String, String> collectionMappings = new HashMap<String, String>();
		Map<String, String> databaseMappings = new HashMap<String, String>();
		createMappings(DATABASE_MAPPING_STRING, COLLECTION_MAPPING_STRING, databaseMappings, collectionMappings);

		//	configure the writer
		util.setCollectionMappings(collectionMappings);
		util.setDatabaseMappings(databaseMappings);
		util.setDestinationDatabaseUsername(DEST_DATABASE_USER_NAME);
		util.setDestinationDatabasePassword(DEST_DATABASE_PASSWORD);
		util.setDestinationDatabaseHost(DEST_DATABASE_HOST);

		try{
			File[] files = new File(INPUT_DIR).listFiles();
			if(files != null){
				List<File> filesToProcess = new ArrayList<File>();
				for(File file : files){
					if(file.getName().indexOf(".bson") > 0){
						filesToProcess.add(file);
					}
				}
				long operationsRead = 0;
				long operationsSkipped = 0;
				long lastOutput = System.currentTimeMillis();
				for(File file : filesToProcess){
					System.out.println("replaying file " + file.getName());
					BufferedInputStream inputStream = null;
					try{
						if(file.getName().endsWith(".gz")){
							InputStream is = new GZIPInputStream(new FileInputStream(file));
							inputStream = new BufferedInputStream(is);
						}
						else{
							inputStream = new BufferedInputStream(new FileInputStream(file));
						}
						BSONDecoder decoder = new BSONDecoder();
						while(true){
							if(inputStream.available() == 0){
								break;
							}
							BSONObject obj = decoder.readObject(inputStream);
							if(obj == null){
								break;
							}
							BasicDBObject dbo = new BasicDBObject((BasicBSONObject)obj);

							BSONTimestamp operationTimestamp = (BSONTimestamp)dbo.get("ts");
							String namespace = dbo.getString("ns");
							String collection = util.getUnmappedCollectionFromNamespace(namespace);

							boolean shouldProcess = shouldProcessRecord(collection, operationTimestamp);

							if(collection != null && shouldProcess){
								util.processRecord(dbo);
								operationsRead++;
							}
							else{
								operationsSkipped++;
							}

							long durationSinceLastOutput = System.currentTimeMillis() - lastOutput;
							if(durationSinceLastOutput > REPORT_INTERVAL){
								report(util.getInsertCount(), util.getUpdateCount(), util.getDeleteCount(), operationsRead, operationsSkipped, durationSinceLastOutput);
								lastOutput = System.currentTimeMillis();
							}
						}
					}
					catch(Exception ex){
						ex.printStackTrace();
					}
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	protected boolean shouldProcessRecord(String collection, BSONTimestamp timestamp) {
		boolean shouldProcess = false;

		if(COLLECTIONS_TO_ADD.contains(collection)){
			shouldProcess = true;
		}
		if(COLLECTIONS_TO_SKIP.contains(collection)){
			shouldProcess = false;
		}
		else{
			if(ONLY_COLLECTION_EXCLUSIONS){
				shouldProcess = true;
			}
		}
		if(AFTER_TIMESTAMP != null){
			if(timestamp.getTime() < AFTER_TIMESTAMP.getTime()){
				shouldProcess = false;
			}
		}
		if(BEFORE_TIMESTAMP != null){
			if(timestamp.getTime() >= BEFORE_TIMESTAMP.getTime()){
				shouldProcess = false;
			}
		}
		return shouldProcess;
	}

	public static boolean parseArgs(String...args){
		for (int i = 0; i < args.length; i++) {
			switch (args[i].charAt(1)) {
			case 'i':
				INPUT_DIR = args[++i];
				break;
			case 'c':
				COLLECTION_STRING = args[++i];
				break;
			case 'R':
				DATABASE_MAPPING_STRING = args[++i];
				break;
			case 'r':
				COLLECTION_MAPPING_STRING = args[++i];
				break;
			case 'a':
				try{
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					Date date = sdf.parse(args[++i]);
					AFTER_TIMESTAMP = new BSONTimestamp((int)(date.getTime()/1000), 0);
				}
				catch(Exception e){
					throw new RuntimeException("invalid date supplied");
				}
				break;
			case 'b':
				try{
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					Date date = sdf.parse(args[++i]);
					BEFORE_TIMESTAMP = new BSONTimestamp((int)(date.getTime()/1000), 0);
				}
				catch(Exception e){
					throw new RuntimeException("invalid date supplied");
				}
				break;
			case 'u':
				DEST_DATABASE_USER_NAME = args[++i];
				break;
			case 'p':
				DEST_DATABASE_PASSWORD = args[++i];
				break;
			case 'h':
				DEST_DATABASE_HOST = args[++i];
				break;
			default:
				return false;
			}
		}
		return true;
	}

	void report(long inserts, long updates, long deletes, long totalCount, long skips, long duration){
		double brate = (double)totalCount / ((duration) / 1000.0);
		System.out.println("inserts: " + PrintFormat.LONG_FORMAT.format(inserts) + ", updates: " + PrintFormat.LONG_FORMAT.format(updates) + ", deletes: " + PrintFormat.LONG_FORMAT.format(deletes) + ", skips: " + PrintFormat.LONG_FORMAT.format(skips) + " (" + PrintFormat.LONG_FORMAT.format(brate) + " req/sec)");
	}

	public static void usage(){
		System.out.println("usage: ReplayUtil");
		System.out.println(" -i : input directory");
		System.out.println(" -c : CSV collection string (prefix with ! to exclude)");
		System.out.println(" -r : collection re-targeting (format: {SOURCE}={TARGET}");
		System.out.println(" -R : database re-targeting (format: {SOURCE}={TARGET}");
		System.out.println(" -a : only process entries after this timestamp");
		System.out.println(" -b : only process entries before this timestamp");
		System.out.println(" -h : destination hostname");
		System.out.println(" [-u : username]");
		System.out.println(" [-p : password]");
	}
}