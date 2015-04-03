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

import com.mongodb.DB;
import com.wordnik.mongo.connection.*;

import java.util.ArrayList;
import java.util.List;

public class IncrementalBackupUtil extends MongoUtil {
	protected static String DATABASE_HOST = null;
	protected static String DATABASE_USER_NAME = null;
	protected static String DATABASE_PASSWORD = null;

	protected static String COLLECTIONS_STRING;
	protected static boolean COMPRESS_OUTPUT_FILES = false;
	protected static int UNCOMPRESSED_FILE_SIZE_MB = 100;

	protected static String OUTPUT_DIRECTORY;

	protected static boolean START_FROM_NOW = false;

	public static void main(String ... args){
		if(!parseArgs(args)){
			usage();
			return;
		}
		new IncrementalBackupUtil().run();
	}

	void run(){
		try{
			//	create a file-based writer and configure it
			OplogFileWriter util = new OplogFileWriter();
			OplogFileWriter.COMPRESS_OUTPUT_FILES = COMPRESS_OUTPUT_FILES;
			OplogFileWriter.UNCOMPRESSED_FILE_SIZE_MB = UNCOMPRESSED_FILE_SIZE_MB;
			util.setOutputDirectory(OUTPUT_DIRECTORY);

			//	create the thread and give it a connection + the util
			OplogTailThread thd = new OplogTailThread(util, MongoDBConnectionManager.getOplog("oplog", DATABASE_HOST, DATABASE_USER_NAME, DATABASE_PASSWORD).get());
      thd.setExitOnStopThread(true);
			List<String> inclusions = new ArrayList<String>();
			List<String> exclusions = new ArrayList<String>();
			selectCollections(COLLECTIONS_STRING, inclusions, exclusions);

			thd.setBaseDir(OUTPUT_DIRECTORY);
			thd.setInclusions(inclusions);
			thd.setExclusions(exclusions);

			if(START_FROM_NOW)
				thd.writeTimestampToStartFromNow();

			thd.start();

			//	start a stop-file monitor
			new StopFileMonitor(thd).start();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	public static boolean parseArgs(String...args){
		for (int i = 0; i < args.length; i++) {
			switch (args[i].charAt(1)) {
			case 'o':
				OUTPUT_DIRECTORY = args[++i];
				validateDirectory(OUTPUT_DIRECTORY);
				break;
			case 's':
				UNCOMPRESSED_FILE_SIZE_MB = Integer.parseInt(args[++i]);
				break;
			case 'Z':
				COMPRESS_OUTPUT_FILES = true;
				break;
			case 'c':
				COLLECTIONS_STRING = args[++i];
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
			case 'n':
				START_FROM_NOW = true;
				break;
			default:
				System.out.println("unknown argument " + args[i]);
				return false;
			}
		}
		return true;
	}

	public static void usage(){
		System.out.println("usage: IncrementalBackupUtil");
		System.out.println(" -c : CSV of collections to process, scoped to the db (database.collection), ! will exclude");
		System.out.println(" -h : source database host[:port]");
		System.out.println(" -o : output directory");
		System.out.println(" [-u : source database username]");
		System.out.println(" [-p : source database password]");
		System.out.println(" [-s : max file size in MB, default 100]");
		System.out.println(" [-Z : gzip files]");
		System.out.println(" [-n : start from now]");
	}
}
