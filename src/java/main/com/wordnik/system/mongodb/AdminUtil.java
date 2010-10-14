package com.wordnik.system.mongodb;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;

public class AdminUtil {
	enum Mode {NONE,TAIL,LOAD,COPY,READ};

	static String database = null;
	static String username = null;
	static String password = null;
	static String host = null;
	static String collection = null;
	static boolean zip = false;
	static int fileSizeInMb = 100;
	
	static Mode MODE = Mode.NONE;
	static long WRITES = 0;
	static long REPORT_INTERVAL = 10000;
	static long START_TIME = 0;
	static long LAST_OUTPUT = 0;

	static NumberFormat LONG_FORMAT = new DecimalFormat("###,###");
	static NumberFormat PERCENT_FORMAT = new DecimalFormat("#.##");

	public static void main(String ... args){
		if(!parseArgs(args)){
			usage();
			return;
		}
		if(MODE == Mode.NONE){
			usage();
			return;
		}
		if(MODE == Mode.COPY || MODE == Mode.READ){
			if(host == null){
				host = "localhost";
			}
			if(database == null){
				System.out.println("Give a database name to copy");
				usage();
				return;
			}
			if(collection == null){
				System.out.println("Give a collection name to copy");
				usage();
				return;
			}
			new AdminUtil().copy();	
		}
		if(MODE == Mode.READ){
			new AdminUtil().read();
		}
	}

	protected void read(){
		try{
			START_TIME = System.currentTimeMillis();
			DB db = getDb();
			String collectionToCopy = getCollection();
			DBCollection collection = db.getCollection(collectionToCopy);

            DBCursor cursor = null;
            cursor = collection.find();
            cursor.sort(new BasicDBObject("$natural", 1));

            while (cursor.hasNext() ){
                cursor.next();
        		++WRITES;
                if(System.currentTimeMillis() - LAST_OUTPUT > REPORT_INTERVAL){
                	report();
                	LAST_OUTPUT = System.currentTimeMillis();
                }
            }
            report();
            if(WRITER != null){
            	WRITER.close();
            }
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	protected void copy(){
		try{
			LAST_OUTPUT = System.currentTimeMillis();
			START_TIME = System.currentTimeMillis();
			DB db = getDb();
			String collectionToCopy = getCollection();
			DBCollection collection = db.getCollection(collectionToCopy);

            DBCursor cursor = null;
            cursor = collection.find();
            cursor.sort(new BasicDBObject("$natural", 1));

            while (cursor.hasNext() ){
                BasicDBObject x = (BasicDBObject) cursor.next();
        		++WRITES;

            	write(collectionToCopy, x);
                if(System.currentTimeMillis() - LAST_OUTPUT > REPORT_INTERVAL){
                	report();
                	LAST_OUTPUT = System.currentTimeMillis();
                }
            }
            report();
            if(WRITER != null){
            	WRITER.close();
            }
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private void report(){
		long elapsed = System.currentTimeMillis() - START_TIME;
		double rate = (double)WRITES / (elapsed / 1000.0);

		String msg = null;

		if(WRITER != null){
			long bytesWritten = WRITER.getTotalBytesWritten();
			double brate = (double)bytesWritten / (elapsed / 1000.0) / ((double)1048576L);
			msg = "Written " + LONG_FORMAT.format(WRITES) + " values (" + PERCENT_FORMAT.format(rate) + " req/sec, " + PERCENT_FORMAT.format(brate) + " mb/sec)";
		}
		else{
			msg = "Written " + LONG_FORMAT.format(WRITES) + " values (" + PERCENT_FORMAT.format(rate) + " req/sec)";
		}
		System.out.println(msg);
	}

	private String getCollection(){
		return collection;
	}

	private static DB db = null;
	private DB getDb() throws Exception {
		if(db == null){
			Mongo m = new Mongo(host);
			if(username != null){
				db = m.getDB(database);
				db.authenticate(username, password.toCharArray());
			}
			else{
				db = m.getDB(database);
			}
		}
		return db;
	}

	static RotatingFileWriter WRITER = null;
	void write(String collectionName, BasicDBObject dbo) throws IOException {
		if(WRITER == null){
			WRITER = new RotatingFileWriter(collectionName, null, "json", fileSizeInMb * 1048576L, zip);
		}
		WRITER.write(dbo.toString());
	}

	private static boolean parseArgs(String[] args) {
		for (int i = 0; i < args.length; i++) {
			switch (args[i].charAt(1)) {
			case 'm':
				String modeString = args[++i];
				if("tail".equalsIgnoreCase(modeString)){
					MODE = Mode.TAIL;
				}
				if("load".equalsIgnoreCase(modeString)){
					MODE = Mode.LOAD;
				}
				if("copy".equalsIgnoreCase(modeString)){
					MODE = Mode.COPY;
				}
				if("read".equalsIgnoreCase(modeString)){
					MODE = Mode.READ;
				}
				break;
			case 'c':
				collection = args[++i];
				break;
			case 'd':
				database = args[++i];
				break;
			case 'u':
				username = args[++i];
				break;
			case 'p':
				password = args[++i];
				break;
			case 'h':
				host = args[++i];
				break;
			case 'z':
				zip = true;
				break;
			case 's':
				fileSizeInMb = Integer.parseInt(args[++i]);
				break;
			default:
				return false;
			}
		}
		return true;
	}

	static void usage(){
		System.out.println("usage: AdminUtil");
		System.out.println(" -m : mode=TAIL | LOAD | COPY");
		System.out.println(" -c : colleciton name");
		System.out.println(" -d : database name");
		System.out.println(" -h : hostname");
		System.out.println(" [-u : username]");
		System.out.println(" [-p : password]");
		System.out.println(" -z : zip file on rotate");
		System.out.println(" -s : file size in mb");
	}
}
