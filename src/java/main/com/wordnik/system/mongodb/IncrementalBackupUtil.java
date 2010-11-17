package com.wordnik.system.mongodb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.bson.types.BSONTimestamp;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class IncrementalBackupUtil extends BaseMongoUtil {
	protected static String INPUT_DIR;
	protected final static String OPLOG_LAST_FILENAME = "last_timestamp.txt";

	public static void main(String ... args){
		if(!parseArgs(args)){
			usage();
			return;
		}
		
		new IncrementalBackupUtil().run();
	}
	
	void run(){
		try{
			DATABASE_NAME="local";
			OplogTailThread thd = new OplogTailThread();
			thd.setName("OplogTailThread");
			thd.start();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static boolean parseArgs(String...args){
		for (int i = 0; i < args.length; i++) {
			switch (args[i].charAt(1)) {
			case 'i':
				INPUT_DIR = args[++i];
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
		System.out.println("usage: IncrementalBackupUtil");
		System.out.println(" -i : input directory");
		System.out.println(" -c : CSV collection string (prefix with ! to exclude)");
		System.out.println(" -o : output directory");

		BaseMongoUtil.usage();
	}
	
	class OplogTailThread extends Thread {
		boolean running = false;
		boolean kill = false;
		long reportInterval = 10000;
		
		public void run(){
			running = true;
			
			BSONTimestamp ts = null;
			try{
				ts = getLastTimestamp();

	            long lastWrite = 0;
	            long startTime = System.currentTimeMillis();
	            long lastOutput = System.currentTimeMillis();
            	while(true){
            		try{
		            	if(kill){
		            		//	step out on exception
		            		break;
		            	}
	    				DB db = getDb();
	    				DBCollection oplog = db.getCollection("oplog.$main");
	
	    	            DBCursor cursor = null;
	    	            if(ts != null){
	    	            	cursor = oplog.find( new BasicDBObject( "ts" , new BasicDBObject( "$gt" , ts ) ) );
	    	            }
	    	            else{
	    	            	cursor = oplog.find();
	    	            }
	    	            cursor.addOption( Bytes.QUERYOPTION_TAILABLE );
	    	            cursor.addOption( Bytes.QUERYOPTION_AWAITDATA );
	    	            long count = 0;
			            while (!kill && cursor.hasNext() ){
			                DBObject x = cursor.next();
			                count++;
			                ts = (BSONTimestamp)x.get("ts");
			                write("oplog", (BasicDBObject)x);
			                if(System.currentTimeMillis() - lastWrite > 1000){
			    				saveLastTimestamp(ts);
			    				lastWrite = System.currentTimeMillis();
			                }
			                long duration = System.currentTimeMillis() - lastOutput;
							if(duration > reportInterval){
								report("oplog", count, System.currentTimeMillis() - startTime);
								lastOutput = System.currentTimeMillis();
							}
			            }
		            }
		            catch(com.mongodb.MongoInternalException ex){
		            	saveLastTimestamp(ts);
		            }
	            }
				Thread.sleep(1000);
			}
			catch(Exception e){
				e.printStackTrace();
			}
			finally{
				saveLastTimestamp(ts);
			}
			running = false;
		}
		
		void report(String collectionName, long count, long duration){
			double brate = (double)count / ((duration) / 1000.0);
			System.out.println(collectionName + ": " + LONG_FORMAT.format(count) + " records, " + LONG_FORMAT.format(brate) + " req/sec");
		}

		void saveLastTimestamp(BSONTimestamp ts){
			Writer writer = null;
			try{
				OutputStream out = new FileOutputStream(new File(OPLOG_LAST_FILENAME));
				writer = new OutputStreamWriter(out, "UTF-8");
				writer.write(Integer.toString(ts.getTime()) + "|" + Integer.toString(ts.getInc()));
			}
			catch(Exception e){
				e.printStackTrace();
			}
			finally{
				if(writer != null){
					try{writer.close();}
					catch(Exception e){}
				}
			}
		}
		
		BSONTimestamp getLastTimestamp() {
			BufferedReader input = null;
			try{
				File file = new File(OPLOG_LAST_FILENAME);
				if(!file.exists()){
//					LOGGER.warn("no oplog last file found, pulling all oplog data");
					return null;
				}
				input = new BufferedReader(new InputStreamReader(new FileInputStream(file),"UTF8"));
				String line = input.readLine();
				String[] parts = line.split("\\|");
				return new BSONTimestamp(Integer.parseInt(parts[0]),Integer.parseInt(parts[1]));
			}
			catch(Exception e){
				e.printStackTrace();
			}
			finally{
				if(input != null){
					try{input.close();}
					catch(Exception e){}
				}
			}
			return null;
		}
	}
}
