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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

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
	protected static String COLLECTIONS_TO_INCLUDE;
	protected static String COLLECTIONS_TO_EXCLUDE;

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
			
			if(COLLECTIONS_TO_INCLUDE != null){
				List<String> inclusions = new ArrayList<String>();

				StringTokenizer tk = new StringTokenizer(COLLECTIONS_TO_INCLUDE, ",");
				while(tk.hasMoreElements()){
					inclusions.add(tk.nextToken().trim());
				}
				if(inclusions.size() > 0){
					thd.setInclusions(inclusions);
				}
			}

			if(COLLECTIONS_TO_EXCLUDE != null){
				List<String> exclusions = new ArrayList<String>();

				StringTokenizer tk = new StringTokenizer(COLLECTIONS_TO_EXCLUDE, ",");
				while(tk.hasMoreElements()){
					exclusions.add(tk.nextToken().trim());
				}
				if(exclusions.size() > 0){
					thd.setExclusions(exclusions);
				}
			}

			thd.setName("OplogTailThread");
			thd.start();

			new StopFileMonitor(thd).start();
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
			case 'I':
				COLLECTIONS_TO_INCLUDE = args[++i];
				break;
			case 'E':
				COLLECTIONS_TO_EXCLUDE = args[++i];
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
		System.out.println(" -o : output directory");
		System.out.println(" -I : CSV collections to include");
		System.out.println(" -E : CSV collections to exclude");
		System.out.println(" [-Z : compress files]");

		BaseMongoUtil.usage();
	}

	class StopFileMonitor extends Thread {
		OplogTailThread tailThread;
		public StopFileMonitor(OplogTailThread tailThread){
			this.tailThread = tailThread;
		}

		public void run(){
			while(true){
				try{
					Thread.sleep(1000);
					File file = new File("stop.txt");
					if(file.exists() || tailThread.kill){
						System.out.println("found stop file, exiting");
						tailThread.kill = true;
						file.deleteOnExit();
						return;
					}
				}
				catch(Exception e){
					e.printStackTrace();
				}
			}
		}
	}

	class OplogTailThread extends Thread {
		boolean running = false;
		boolean kill = false;
		long reportInterval = 10000;
		List<String> inclusions;
		List<String> exclusions;
		
		public void setInclusions(List<String> inclusions){
			this.inclusions = inclusions;
		}
		
		public void setExclusions(List<String> exclusions){
			this.exclusions = exclusions;
		}

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
		            		System.out.println("exiting thread");
		            		return;
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
	    	            long skips = 0;

	    	            while (!kill && cursor.hasNext() ){
			                DBObject x = cursor.next();
			                ts = (BSONTimestamp)x.get("ts");
			                if(shouldWrite(x)){
			                	write("oplog", (BasicDBObject)x);
			                	count++;
			                }
			                else{
			                	skips++;
			                }
			                if(System.currentTimeMillis() - lastWrite > 1000){
			    				saveLastTimestamp(ts);
			    				lastWrite = System.currentTimeMillis();
			                }
			                long duration = System.currentTimeMillis() - lastOutput;
							if(duration > reportInterval){
								report("oplog", count, skips, System.currentTimeMillis() - startTime);
								lastOutput = System.currentTimeMillis();
							}
			            }
		            }
		            catch(com.mongodb.MongoInternalException ex){
		            	kill = true;
		            	saveLastTimestamp(ts);
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
				saveLastTimestamp(ts);
				try{
					closeWriter("oplog");
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
            if(exclusions == null && inclusions == null){
            	return true;
            }
            if(exclusions != null){
            	if(exclusions.contains(obj.get(ns))){
            		return false;
            	}
            }
            if(inclusions != null){
            	if(inclusions.contains(obj.get(ns))){
            		return true;
            	}
            	else{
            		return false;
            	}
            }
			return true;
		}
		
		void report(String collectionName, long count, long skips, long duration){
			double brate = (double)count / ((duration) / 1000.0);
			System.out.println(collectionName + ": " + LONG_FORMAT.format(count) + " records, " + LONG_FORMAT.format(brate) + " req/sec, " + LONG_FORMAT.format(skips) + " skips");
		}

		void saveLastTimestamp(BSONTimestamp ts){
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
