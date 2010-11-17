package com.wordnik.system.mongodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.wordnik.util.AbstractFileWriter;

public class SnapshotUtil extends BaseMongoUtil {
	protected static int THREAD_COUNT = 1;
	protected static String COLLECTION_STRING;

	public static void main(String ... args){
		if(!parseArgs(args)){
			usage();
			return;
		}
		new SnapshotUtil().run();
	}

	protected void run(){
		//	collections to backup
		List<CollectionInfo> collections = getCollections();

		//	spawn a thread for each job
		List<SnapshotThread> threads = new ArrayList<SnapshotThread>();

		int threadCounter = 0;
		for(CollectionInfo collection : collections){
			if(threads.size() < (threadCounter+1)){
				SnapshotThread thread = new SnapshotThread(threadCounter);
				thread.setName("backup_thread_" + collection.getName());
				threads.add(thread);
			}
			threads.get(threadCounter).add(collection);
			if(++threadCounter >= THREAD_COUNT){
				threadCounter = 0;
			}
		}
		for(SnapshotThread thread : threads){
			thread.start();
		}

		//	monitor & report
		while(true){
			try{
				Thread.sleep(5000);
				boolean isDone = true;
				StringBuilder b = new StringBuilder();
				for(SnapshotThread thread : threads){
					if(thread.isDone == false){
						isDone = false;
						double mbRate = thread.getRate();
						double complete = thread.getPercentComplete();
						b.append(thread.currentCollection.getName()).append(": ").append(PERCENT_FORMAT.format(complete)).append(" (").append(NUMBER_FORMAT.format(mbRate)).append("mb/s)   ");
					}
				}
				System.out.println(b.toString());
				if(isDone){
					break;
				}
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
	}
	
	private List<CollectionInfo> getCollections() {
		List<CollectionInfo> collections = new ArrayList<CollectionInfo>();
		try{
			Collection<String> collectionsInDb = super.getDb().getCollectionNames();
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
				if(!"system.indexes".equals(collectionName)){
					long count = getDb().getCollection(collectionName).count();
					if(count > 0){
						CollectionInfo info = new CollectionInfo(collectionName, count);
						info.setCollectionExists(true);
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
			
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
		return collections;
	}

	public static boolean parseArgs(String...args){
		for (int i = 0; i < args.length; i++) {
			switch (args[i].charAt(1)) {
			case 't':
				THREAD_COUNT = Integer.parseInt(args[++i]);
				break;
			case 'c':
				COLLECTION_STRING = args[++i];
				break;
			case 'o':
				OUTPUT_DIRECTORY = args[++i];
				break;
			case 's':
				UNCOMPRESSED_FILE_SIZE_MB = Integer.parseInt(args[++i]);
				break;
			case 'Z':
				COMPRESS_OUTPUT_FILES = true;
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
		System.out.println("usage: SnapshotUtil");
		System.out.println(" -t : threads");
		System.out.println(" -o : output directory");
		System.out.println(" -c : CSV collection string (prefix with ! to exclude)");
		System.out.println(" [-s : max file size in MB]");
		System.out.println(" [-Z : compress files]");
		System.out.println(" [-J : output in JSON (default is BSON)]");

		BaseMongoUtil.usage();
	}

	class SnapshotThread extends Thread {
		long startTime = 0;
		long lastOutput = 0;
		int threadId;
		long writes = 0;
		boolean isDone = false;
		CollectionInfo currentCollection = null;

		public SnapshotThread(int threadId){
			this.threadId = threadId;
		}

		List<CollectionInfo> collections = new ArrayList<CollectionInfo>();
		public void run() {
			for(CollectionInfo info : collections){
				writes = 0;
				currentCollection = info;
				try{
					writeConnectivityDetailString(currentCollection.getName());
					if(currentCollection.getIndexes().size() > 0){
						writeComment(currentCollection.getName(), "indexes");
					}
					for(BasicDBObject index : currentCollection.getIndexes()){
						writeIndex(currentCollection.getName(), index);
					}
					lastOutput = System.currentTimeMillis();
					startTime = System.currentTimeMillis();
					DB db = getDb();
					DBCollection collection = db.getCollection(currentCollection.getName());
		            DBCursor cursor = null;
		            cursor = collection.find();
		            cursor.sort(new BasicDBObject("_id", 1));

		            while (cursor.hasNext() ){
		                BasicDBObject x = (BasicDBObject) cursor.next();
		        		++writes;
		            	write(currentCollection.getName(), x);
		            }
		            closeWriter(currentCollection.getName());
				}
				catch(Exception e){
					e.printStackTrace();
				}
				finally{
					isDone = true;
				}
			}
		}

		public double getPercentComplete(){
			return (double) writes / (double)currentCollection.getCount();
		}

		public double getRate(){
        	AbstractFileWriter writer = WRITERS.get(currentCollection.getName());
			if(writer != null){
				long bytesWritten = writer.getTotalBytesWritten();
				double brate = (double)bytesWritten / ((System.currentTimeMillis() - startTime) / 1000.0) / ((double)1048576L);
				lastOutput = System.currentTimeMillis();
				return brate;
			}
        	return 0;
		}
		
		public void add(CollectionInfo info){
			this.collections.add(info);
		}
	}
}