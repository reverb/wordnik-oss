package com.wordnik.system.mongodb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;

import org.bson.types.BSONTimestamp;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.wordnik.util.PrintFormat;

public class OplogTailThread extends Thread {
	protected boolean running = false;
	protected boolean killMe = false;
	protected long reportInterval = 10000;
	protected List<String> inclusions;
	protected List<String> exclusions;
	protected OplogRecordProcessor processor;
	protected DBCollection oplog;
	protected static String OPLOG_LAST_FILENAME = "last_timestamp.txt";

	public OplogTailThread(OplogRecordProcessor processor, DBCollection oplog){
		this.oplog = oplog;
		this.processor = processor;
		setName("OplogTailThread");
	}

	public void setBaseDir(String dir){
		if(dir != null){
			OPLOG_LAST_FILENAME = dir + File.separator + OPLOG_LAST_FILENAME;
		}
	}

	public void setInclusions(List<String> inclusions){
		this.inclusions = inclusions;
	}

	public void setExclusions(List<String> exclusions){
		this.exclusions = exclusions;
	}

	public void writeLastTimestamp(BSONTimestamp ts){
		if(ts == null){
			return;
		}
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

	public BSONTimestamp getLastTimestamp() {
		BufferedReader input = null;
		try{
			File file = new File(OPLOG_LAST_FILENAME);
			if(!file.exists()){
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

	public void run(){
		running = true;
		BSONTimestamp lastTimestamp = null;
		try{
			lastTimestamp = getLastTimestamp();

            long lastWrite = 0;
            long startTime = System.currentTimeMillis();
            long lastOutput = System.currentTimeMillis();
        	while(true){
        		try{
	            	if(killMe){
	            		System.out.println("exiting thread");
	            		return;
	            	}
    	            DBCursor cursor = null;
    	            if(lastTimestamp != null){
    	            	cursor = oplog.find( new BasicDBObject( "ts" , new BasicDBObject( "$gt" , lastTimestamp ) ) );
    	            }
    	            else{
    	            	cursor = oplog.find();
    	            }
    	            cursor.addOption( Bytes.QUERYOPTION_TAILABLE );
    	            cursor.addOption( Bytes.QUERYOPTION_AWAITDATA );
    	            long count = 0;
    	            long skips = 0;

    	            while (!killMe && cursor.hasNext() ){
		                DBObject x = cursor.next();
		                lastTimestamp = (BSONTimestamp)x.get("ts");
		                if(shouldWrite(x)){
		                	processor.processRecord((BasicDBObject)x);
		                	count++;
		                }
		                else{
		                	skips++;
		                }
		                if(System.currentTimeMillis() - lastWrite > 1000){
		    				writeLastTimestamp(lastTimestamp);
		    				lastWrite = System.currentTimeMillis();
		                }
		                long duration = System.currentTimeMillis() - lastOutput;
						if(duration > reportInterval){
							report("oplog", count, skips, System.currentTimeMillis() - startTime);
							lastOutput = System.currentTimeMillis();
						}
		            }
	            }
	            catch(com.mongodb.MongoException.CursorNotFound ex){
	            	writeLastTimestamp(lastTimestamp);
	            	System.out.println("Cursor not found, waiting");
	            	Thread.sleep(2000);
	            }
	            catch(com.mongodb.MongoInternalException ex){
	            	System.out.println("Cursor not found, waiting");
	            	writeLastTimestamp(lastTimestamp);
	            	ex.printStackTrace();
	            }
	            catch(com.mongodb.MongoException ex){
	            	writeLastTimestamp(lastTimestamp);
	            	System.out.println("Internal exception, waiting");
	            	Thread.sleep(2000);
	            }
	            catch(Exception ex){
	            	killMe = true;
	            	writeLastTimestamp(lastTimestamp);
	            	ex.printStackTrace();
	            	break;
	            }
            }
			Thread.sleep(1000);
		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
			writeLastTimestamp(lastTimestamp);
			try{
				processor.close("oplog");
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
		running = false;
	}

	boolean shouldWrite(DBObject obj){
        String ns = (String)obj.get("ns");
        if(ns == null || "".equals(ns)){
        	return false;
        }
        if(exclusions.size() == 0 && inclusions.size() == 0){
        	return true;
        }
    	if(exclusions.contains(ns)){
    		return false;
    	}
    	if(inclusions.contains(ns) || inclusions.contains("*")){
    		return true;
    	}
    	//	check database-level inclusion
    	if(ns.indexOf('.') > 0 && inclusions.contains(ns.substring(0, ns.indexOf('.')))){
    		System.out.println("including* " + ns);
    		return true;
    	}

    	return false;
	}

	void report(String collectionName, long count, long skips, long duration){
		double brate = (double)count / ((duration) / 1000.0);
		System.out.println(collectionName + ": " + PrintFormat.LONG_FORMAT.format(count) + " records, " + PrintFormat.LONG_FORMAT.format(brate) + " req/sec, " + PrintFormat.LONG_FORMAT.format(skips) + " skips");
	}
}
