package com.wordnik.system.mongodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.wordnik.util.RotatingFileWriter;

public class BackupUtil extends BaseMongoUtil {
	protected static int THREAD_COUNT = 1;
	protected static String COLLECTION_STRING;

	public static void main(String ... args){
		if(!parseArgs(args)){
			usage();
			return;
		}
		new BackupUtil().run();
	}

	protected void run(){
		//	collections to backup
		List<CollectionInfo> collections = getCollections();
		
		//	spawn a thread for each job
		List<BackupThread> threads = new ArrayList<BackupThread>();

		int threadCounter = 0;
		for(CollectionInfo collection : collections){
			if(threads.size() < (threadCounter+1)){
				BackupThread thread = new BackupThread(threadCounter);
				thread.setName("backup_thread_" + collection.name);
				threads.add(thread);
			}
			threads.get(threadCounter).add(collection);
			if(++threadCounter >= THREAD_COUNT){
				threadCounter = 0;
			}
		}
		for(BackupThread thread : threads){
			thread.start();
		}
		
		//	monitor & report
		while(true){
			try{
				Thread.sleep(5000);
				boolean isDone = true;
				StringBuilder b = new StringBuilder();
				for(BackupThread thread : threads){
					if(thread.isDone == false){
						isDone = false;
						double mbRate = thread.getRate();
						double complete = thread.getPercentComplete();
						b.append(thread.currentCollection.name).append(": ").append(PERCENT_FORMAT.format(complete)).append(" (").append(NUMBER_FORMAT.format(mbRate)).append("mb/s)   ");
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
				long count = getDb().getCollection(collectionName).count();
				if(count > 0){
					collections.add(new CollectionInfo(collectionName, count));
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
		System.out.println(" -t : threads");

		BaseMongoUtil.usage();
	}
	
	class CollectionInfo {
		String name;
		long count;
		
		public CollectionInfo(String name, long count){
			this.name = name;
			this.count = count;
		}

		@Override
		public String toString() {
			return "CollectionInfo [count=" + count + ", name=" + name + "]";
		}
	}

	class BackupThread extends Thread {
		long START_TIME = 0;
		long LAST_OUTPUT = 0;
		int threadId;
		long writes = 0;
		boolean isDone = false;
		CollectionInfo currentCollection = null;
		
		public BackupThread(int threadId){
			this.threadId = threadId;
		}

		List<CollectionInfo> collections = new ArrayList<CollectionInfo>();
		public void run() {
			for(CollectionInfo info : collections){
				writes = 0;
				currentCollection = info;
				try{
					LAST_OUTPUT = System.currentTimeMillis();
					START_TIME = System.currentTimeMillis();
					DB db = getDb();
					DBCollection collection = db.getCollection(currentCollection.name);

		            DBCursor cursor = null;
		            cursor = collection.find();
		            cursor.sort(new BasicDBObject("$natural", 1));

		            while (cursor.hasNext() ){
		                BasicDBObject x = (BasicDBObject) cursor.next();
		        		++writes;
		            	write(currentCollection.name, x);
		            }
		            closeWriter(currentCollection.name);
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
			return (double)writes / (double)currentCollection.count;
		}

		public double getRate(){
        	RotatingFileWriter writer = WRITERS.get(currentCollection.name);
			if(writer != null){
				long bytesWritten = writer.getTotalBytesWritten();
				double brate = (double)bytesWritten / ((System.currentTimeMillis() - START_TIME) / 1000.0) / ((double)1048576L);
				LAST_OUTPUT = System.currentTimeMillis();
				return brate;
			}
        	return 0;
		}
		
		public void add(CollectionInfo info){
			this.collections.add(info);
		}
	}
}