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
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class RestoreUtil extends BaseMongoUtil {
	protected static String INPUT_DIR;
	protected static String COLLECTION_STRING;
	protected static boolean DROP_EXISTING = false;

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
					if(file.getName().startsWith(info.getName())){
						filesToProcess.add(file);
					}
				}
				Collections.sort(filesToProcess, new FilenameComparator());
				if(DROP_EXISTING){
					try{
						System.out.println("Dropping collection " + info.getName());
						DBCollection coll = getDb().getCollection(info.getName());
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
							write(info, new BasicDBObject((BasicBSONObject)obj));
							count++;
							
							long duration = System.currentTimeMillis() - lastOutput;
							if(duration > REPORT_DURATION){
								report(info.getName(), count, System.currentTimeMillis() - startTime);
								lastOutput = System.currentTimeMillis();
							}
						}
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
		System.out.println(collectionName + ": " + LONG_FORMAT.format(count) + " records, " + LONG_FORMAT.format(brate) + " req/sec");
	}

	private void write(CollectionInfo info, DBObject dbo) throws Exception {
		DB db = getDb();
		DBCollection coll = db.getCollection(info.getName());
		coll.save(dbo);
	}

	private List<CollectionInfo> getCollections() {
		List<CollectionInfo> collections = new ArrayList<CollectionInfo>();
		try{
			Collection<String> collectionsInDb = getCollectionNamesFromFiles(INPUT_DIR);
			List<String> collectionsToAdd = new ArrayList<String>();
			List<String> collectionsToSkip = new ArrayList<String>();

			boolean exclusionsOnly = true;
			if(COLLECTION_STRING != null){
				String[] collectionNames = COLLECTION_STRING.split(",");
				for(String collectionName : collectionNames){
					if(collectionName.startsWith("!")){
						//	skip it
						collectionsToSkip.add(collectionName);
					}
					else{
						exclusionsOnly = false;
						collectionsToAdd.add(collectionName);
					}
				}
			}
			else{
				exclusionsOnly = false;
			}
			if(exclusionsOnly){
				for(String collectionName : collectionsInDb){
					if(!collectionsToSkip.contains(collectionName)){
						collectionsToAdd.add(collectionName);
					}
				}
			}
			else{
				if(collectionsToAdd.size() == 0){
					//	add everything
					collectionsToAdd.addAll(collectionsInDb);
				}
			}

			for(String collectionName : collectionsToAdd){
				if(!"system.indexes".equals(collectionName) && !"system.users".equals(collectionName)){
					long count = getDb().getCollection(collectionName).count();
					CollectionInfo info = new CollectionInfo(collectionName, count);
					if(count > 0){
						info.setCollectionExists(true);
					}
					collections.add(info);

					String indexName = new StringBuilder().append(DATABASE_NAME).append(".").append(collectionName).toString(); 
					DBCursor cur = getDb().getCollection("system.indexes").find(new BasicDBObject("ns", indexName));
					if(cur != null){
						while(cur.hasNext()){
							info.addIndex((BasicDBObject)cur.next());
						}
					}
				}
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

	public static void usage(){
		System.out.println("usage: BackupUtil");
		System.out.println(" -i : input directory");
		System.out.println(" -c : CSV collection string (prefix with ! to exclude)");
		System.out.println(" -D : drop existing collections");

		BaseMongoUtil.usage();
	}

	class FilenameComparator implements Comparator<File> {
		@Override
		public int compare(File o1, File o2) {
			return o1.getName().compareTo(o2.getName());
		}
	}
}