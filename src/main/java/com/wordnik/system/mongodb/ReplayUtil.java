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
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class ReplayUtil extends BaseMongoUtil {
	protected static String INPUT_DIR;
	protected static String COLLECTION_STRING;
	protected static String COLLECTION_RETARGET_STRING;
	protected static String DATABASE_RETARGET_STRING;
	protected static Map<String, String> COLLECTION_MAPPING = new HashMap<String, String>();
	protected static Map<String, String> DATABASE_MAPPING = new HashMap<String, String>();
	protected static Set<String> COLLECTIONS_TO_SKIP = new HashSet<String>();
	protected static Set<String> COLLECTIONS_TO_ADD = new HashSet<String>();
	protected static BSONTimestamp AFTER_TIMESTAMP = null;
	protected static BSONTimestamp BEFORE_TIMESTAMP = null;
	protected static boolean ONLY_COLLECTION_EXCLUSIONS = true;
	protected static Map<String, String> NAMESPACE_COLLECTION_MAP = new HashMap<String, String>();

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

	protected static void createMappings(){
		if(DATABASE_RETARGET_STRING != null){
			StringTokenizer tk = new StringTokenizer(DATABASE_RETARGET_STRING, ",");
			while(tk.hasMoreElements()){
				String[] split = tk.nextToken().split("\\=");
				DATABASE_MAPPING.put(split[0], split[1]);
			}
		}
		
		if(COLLECTION_RETARGET_STRING != null){
			StringTokenizer tk = new StringTokenizer(COLLECTION_RETARGET_STRING, ",");
			while(tk.hasMoreElements()){
				String[] split = tk.nextToken().split("\\=");
				COLLECTION_MAPPING.put(split[0], split[1]);
			}
		}	
	}

	protected void run(){
		//	decide what collections to process
		selectCollections();

		//	get mappings
		createMappings();
		
		try{
			File[] files = new File(INPUT_DIR).listFiles();
			if(files != null){
				List<File> filesToProcess = new ArrayList<File>();
				for(File file : files){
					filesToProcess.add(file);
				}
				long operationsRead = 0;
				long operationsSkipped = 0;
				long insertCount = 0;
				long updateCount = 0;
				long deleteCount = 0;
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
							String operationType = dbo.getString("op");
							BasicDBObject operation = new BasicDBObject((BasicBSONObject)dbo.get("o"));
							String database = getDatabaseMapping(namespace);
							String collection = getCollectionMapping(namespace);
							
							boolean shouldProcess = shouldProcessRecord(collection, operationTimestamp);

							if(database != null && collection != null && shouldProcess){
								operationsRead++;
								DB db = super.getDb(database);
								DBCollection coll = db.getCollection(collection);

								if("i".equals(operationType)){
									insertCount++;
									coll.insert(operation);
								}
								else if("d".equals(operationType)){
									deleteCount++;
									coll.remove(operation);
								}
								else if("u".equals(operationType)){
									updateCount++;
									BasicDBObject o2 = new BasicDBObject((BasicBSONObject)dbo.get("o2"));
									coll.update(o2, operation);
								}
								else{
									System.out.println("operation type : " + operationType);
								}
							}
							else{
								operationsSkipped++;
							}

							long durationSinceLastOutput = System.currentTimeMillis() - lastOutput;
							if(durationSinceLastOutput > REPORT_INTERVAL){
								report(insertCount, updateCount, deleteCount, operationsRead, operationsSkipped, durationSinceLastOutput);
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

	/**
	 * returns a collection name from FQ namespace.  Assumes database name never has "." in it.
	 * 
	 * @param namespace
	 * @return
	 */
	public String getCollectionMapping(String namespace) {
		if(NAMESPACE_COLLECTION_MAP.containsKey(namespace)){
			return NAMESPACE_COLLECTION_MAP.get(namespace);
		}
		String[] parts = namespace.split("\\.");
		if(parts == null || parts.length == 1){
			return null;
		}
		String collection = null;
		if(parts.length == 2){
			collection = parts[1];
			NAMESPACE_COLLECTION_MAP.put(namespace, collection);
		}
		else{
			collection = namespace.substring(parts[0].length()+1);
		}
		
		collection = remapCollection(collection);
		NAMESPACE_COLLECTION_MAP.put(namespace, collection);

		return collection;
	}

	/**
	 * remaps a collection if mapping exists, returns original if not
	 * 
	 * @param collection
	 * @return
	 */
	public String remapCollection(String collection){
		String o = COLLECTION_MAPPING.get(collection);
		return o == null ? collection:o;
	}

	/**
	 * returns a database name from FQ namespace.  Assumes database name never has "." in it.
	 * 
	 * @param namespace
	 * @return
	 */
	public String getDatabaseMapping (String namespace) {
		String[] parts = namespace.split("\\.");
		if(parts == null || parts.length == 1){
			return null;
		}
		String databaseName = parts[0];
		databaseName = remapDatabase(databaseName);
		return databaseName;
	}

	/**
	 * remaps a database name if mapping exists, returns original if not
	 * 
	 * @param databaseName
	 * @return
	 */
	public String remapDatabase(String databaseName){
		String o = DATABASE_MAPPING.get(databaseName);
		return o == null ? databaseName:o;
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
				DATABASE_RETARGET_STRING = args[++i];
				break;
			case 'r':
				COLLECTION_RETARGET_STRING = args[++i];
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
			default:
				int s=parseArg(i, args);
				if(s >0){
					i+=s;
					break;
				}
				return false;
			}
		}
		return true;
	}

	void report(long inserts, long updates, long deletes, long totalCount, long skips, long duration){
		double brate = (double)totalCount / ((duration) / 1000.0);
		System.out.println("inserts: " + LONG_FORMAT.format(inserts) + ", updates: " + LONG_FORMAT.format(updates) + ", deletes: " + LONG_FORMAT.format(deletes) + ", skips: " + LONG_FORMAT.format(skips) + " (" + LONG_FORMAT.format(brate) + " req/sec)");
	}

	public static void usage(){
		System.out.println("usage: ReplayUtil");
		System.out.println(" -i : input directory");
		System.out.println(" -c : CSV collection string (prefix with ! to exclude)");
		System.out.println(" -r : collection re-targeting (format: {SOURCE}={TARGET}");
		System.out.println(" -R : database re-targeting (format: {SOURCE}={TARGET}");
		System.out.println(" -a : only process entries after this timestamp");
		System.out.println(" -b : only process entries before this timestamp");

		BaseMongoUtil.usage();
	}
}