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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.zip.GZIPInputStream;

import org.bson.BSONDecoder;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.wordnik.util.PrintFormat;

public class RestoreUtil extends MongoUtil {
	protected static String INPUT_DIR;
	protected static String COLLECTION_STRING;
	protected static boolean DROP_EXISTING = false;
	
	protected static String DATABASE_HOST = "localhost";
	protected static String DATABASE_NAME = null;
	protected static String DATABASE_USER_NAME = null;
	protected static String DATABASE_PASSWORD = null;

	public static void main(String ... args){
		if(!parseArgs(args)){
			usage();
			return;
		}
		new RestoreUtil().run();
	}

	protected void run(){
		if(INPUT_DIR == null){
			usage();
			return;
		}

		//	get collections
		List<CollectionInfo> collections = getCollections();

		//	restore in single-threaded manner
		for(CollectionInfo info : collections){
			restore(info);
		}
	}
	
	static long REPORT_DURATION = 10000;
	
	protected void restore(CollectionInfo info) {
		try{
			System.out.println("restoring collection " + info.getName());
			File[] files = new File(INPUT_DIR).listFiles();
			if(files != null){
				List<File> filesToProcess = new ArrayList<File>();
				for(File file : files){
					if(file.getName().startsWith(info.getName()) && file.getName().indexOf(".bson") > 0){
						filesToProcess.add(file);
					}
				}
				Collections.sort(filesToProcess, new FilenameComparator());
				if(DROP_EXISTING){
					try{
						System.out.println("Dropping collection " + info.getName());
						DB db = DBConnector.getDb(DATABASE_HOST, DATABASE_USER_NAME, DATABASE_PASSWORD, DATABASE_NAME);
						DBCollection coll = db.getCollection(info.getName());
						coll.drop();
					}
					catch(Exception e){
						e.printStackTrace();
					}
				}

				long count = 0;
				long startTime = System.currentTimeMillis();
				long lastOutput = System.currentTimeMillis();
				for(File file : filesToProcess){
					System.out.println("restoring file " + file.getName() + " to collection " + info.getName());
					InputStream inputStream = null;
					try{
						if(file.getName().endsWith(".gz")){
							inputStream = new GZIPInputStream(new FileInputStream(file));
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
							write(info, new BasicDBObject((BasicBSONObject)obj));
							count++;
							
							long duration = System.currentTimeMillis() - lastOutput;
							if(duration > REPORT_DURATION){
								report(info.getName(), count, System.currentTimeMillis() - startTime);
								lastOutput = System.currentTimeMillis();
							}
						}
					}
					catch(java.io.EOFException e){
						break;
					}
					finally{
						inputStream.close();
					}
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	void report(String collectionName, long count, long duration){
		double brate = (double)count / ((duration) / 1000.0);
		System.out.println(collectionName + ": " + PrintFormat.LONG_FORMAT.format(count) + " records, " + PrintFormat.LONG_FORMAT.format(brate) + " req/sec");
	}

	protected void write(CollectionInfo info, DBObject dbo) throws Exception {
		DB db = DBConnector.getDb(DATABASE_HOST, DATABASE_USER_NAME, DATABASE_PASSWORD, DATABASE_NAME);
		DBCollection coll = db.getCollection(info.getName());
		coll.save(dbo);
	}

	private List<CollectionInfo> getCollections() {
		List<CollectionInfo> collections = new ArrayList<CollectionInfo>();
		try{
			Collection<String> collectionsFromFiles = getCollectionNamesFromFiles(INPUT_DIR);
			List<String> collectionsToAdd = new ArrayList<String>();
			List<String> collectionsToSkip = new ArrayList<String>();

			selectCollections(COLLECTION_STRING, collectionsToAdd, collectionsToSkip);

			boolean exclusionsOnly = collectionsToAdd.contains("*");
			if(exclusionsOnly){
				for(String collectionName : collectionsFromFiles){
					if(!collectionsToSkip.contains(collectionName)){
						collectionsToAdd.add(collectionName);
					}
				}
			}
			else{
				if(collectionsToAdd.size() == 0){
					//	add everything
					collectionsToAdd.addAll(collectionsFromFiles);
				}
			}
			if(exclusionsOnly){
				for(String collectionName : collectionsFromFiles){
					if(!collectionsToSkip.contains(collectionName)){
						collectionsToAdd.add(collectionName);
					}
				}
			}
			else{
				if(collectionsToAdd.size() == 0){
					//	add everything
					collectionsToAdd.addAll(collectionsFromFiles);
				}
			}
			for(String collection : collectionsToAdd){
				if(!"*".equals(collection))
					collections.add(new CollectionInfo(collection, 0));
			}
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
		return collections;
	}

	private Collection<String> getCollectionNamesFromFiles(String inputDir) {
		File[] files = new File(inputDir).listFiles();
		Set<String> collectionNames = new HashSet<String>();
		for(File file : files){
			if(file.getName().contains(".bson")){
				int pos = file.getName().indexOf(".bson");
				String collectionName = file.getName().substring(0, pos);
				if(collectionName.indexOf('.') > 0){
					StringTokenizer tk = new StringTokenizer(collectionName, ".");
					if(tk.countTokens() > 1){
						String base = null;
						try{
							base = tk.nextToken();
							Integer.parseInt(tk.nextToken());
							collectionName = base;
						}
						catch(NumberFormatException e){
							//	continue
						}
					}
				}
				collectionNames.add(collectionName);
			}
		}
		return collectionNames;
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
			case 'D':
				DROP_EXISTING = true;
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
				return false;
			}
		}
		return true;
	}

	public static void usage(){
		System.out.println("usage: RestoreUtil");
		System.out.println(" -i : input directory");
		System.out.println(" -c : CSV collection string (prefix with ! to exclude)");
		System.out.println(" -D : drop existing collections");
		System.out.println(" -h : target database host[:port]");
		System.out.println(" -d : target database name");
		System.out.println(" [-u : target database username]");
		System.out.println(" [-p : target database password]");
	}

	class FilenameComparator implements Comparator<File> {
		@Override
		public int compare(File o1, File o2) {
			return o1.getName().compareTo(o2.getName());
		}
	}
}