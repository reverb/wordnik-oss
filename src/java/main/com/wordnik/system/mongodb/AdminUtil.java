package com.wordnik.system.mongodb;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.wordnik.util.RotatingFileWriter;

public class AdminUtil extends BaseMongoUtil {
	enum Mode {NONE,TAIL,LOAD,COPY,READ};

	static Mode MODE = Mode.NONE;
	static long START_TIME = 0;
	static long LAST_OUTPUT = 0;

	protected static String collection = null;

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
			if(DATABASE_HOST == null){
				DATABASE_HOST = "localhost";
			}
			if(DATABASE_NAME == null){
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
                	report(collectionToCopy);
                	LAST_OUTPUT = System.currentTimeMillis();
                }
            }
            report(collectionToCopy);
            RotatingFileWriter writer = WRITERS.get(collectionToCopy);
            if(writer != null){
            	writer.close();
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
                	report(collectionToCopy);
                	LAST_OUTPUT = System.currentTimeMillis();
                }
            }
            report(collectionToCopy);
            RotatingFileWriter writer = WRITERS.get(collectionToCopy);
            if(writer != null){
            	writer.close();
            }
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private void report(String collectionToCopy){
		long elapsed = System.currentTimeMillis() - START_TIME;
		double rate = (double)WRITES / (elapsed / 1000.0);

		String msg = null;

        RotatingFileWriter writer = WRITERS.get(collectionToCopy);
        if(writer != null){
        	long bytesWritten = writer.getTotalBytesWritten();
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
			default:
				int s = parseArg(i, args);
				if(s==0){
					return false;
				}
				i+=s;
			}
		}
		return true;
	}

	public static void usage(){
		System.out.println("usage: AdminUtil");
		System.out.println(" -m : mode=TAIL | LOAD | COPY");
		System.out.println(" -z : zip file on rotate");
		System.out.println(" -s : file size in mb");
		BaseMongoUtil.usage();
	}
}
