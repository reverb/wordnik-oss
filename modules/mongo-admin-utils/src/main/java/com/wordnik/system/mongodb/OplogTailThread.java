// Copyright (C) 2012  Wordnik, Inc.
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
import java.util.List;
import java.util.ArrayList;

import org.bson.types.BSONTimestamp;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.wordnik.util.PrintFormat;

public class OplogTailThread extends Thread {
  protected boolean enableOutput = true;
  protected boolean running = false;
  protected boolean killMe = false;
  protected long reportInterval = 10000;
  protected List<String> inclusions;
  protected List<String> exclusions;
  protected List<OplogRecordProcessor> processors = new ArrayList<OplogRecordProcessor>();
  protected DBCollection oplog;
  protected String OPLOG_LAST_FILENAME = "last_timestamp.txt";
  protected boolean exitOnStopThread = false;

  public OplogTailThread(OplogRecordProcessor processor, DBCollection oplog){
    this.oplog = oplog;
    this.processors.add(processor);
    setName("oplog");
  }

  public OplogTailThread(OplogRecordProcessor processor, DBCollection oplog, String threadName){
    this.oplog = oplog;
    this.processors.add(processor);
    setName(threadName);
  }

  public void setOutputEnabled(boolean enabled) {
    this.enableOutput = enabled;
  }

  public void setStopFilename(String filename) {
    this.OPLOG_LAST_FILENAME = filename;
  }

  public void addOplogProcessor(OplogRecordProcessor processor) {
    this.processors.add(processor);
  }

  public void setBaseDir(String dir){
    if(dir != null){
      OPLOG_LAST_FILENAME = dir + File.separator + OPLOG_LAST_FILENAME;
    }
  }

  public void setBaseDir(String dir, String fileName){
    if(dir != null && fileName != null){
      OPLOG_LAST_FILENAME = dir + File.separator + fileName;
    }
  }

  public void setExitOnStopThread(Boolean isExit){
    exitOnStopThread = isExit;
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

  public void writeTimestampToStartFromNow(){
    Writer writer = null;
    try{
      OutputStream out = new FileOutputStream(new File(OPLOG_LAST_FILENAME));
      writer = new OutputStreamWriter(out, "UTF-8");
      writer.write(Integer.toString((int) ((System.currentTimeMillis() / 1000L)-1)) + "|" + Integer.toString(1)); // -1 is for reliabilty just in case
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
          if(killMe) {
            System.out.println("exiting thread");
            return;
          }
          DBCursor cursor = null;
          if(lastTimestamp != null){
            cursor = oplog.find( new BasicDBObject( "ts" , new BasicDBObject( "$gt" , lastTimestamp ) ) );
            cursor.addOption( Bytes.QUERYOPTION_OPLOGREPLAY );
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
            if(!killMe) {
              lastTimestamp = (BSONTimestamp)x.get("ts");
              if(shouldWrite(x)){
                for(OplogRecordProcessor processor : processors)
                  processor.processRecord((BasicDBObject)x);
                count++;
              }
              else {
                skips++;
              }
              if(System.currentTimeMillis() - lastWrite > 1000){
                writeLastTimestamp(lastTimestamp);
                lastWrite = System.currentTimeMillis();
              }
              long duration = System.currentTimeMillis() - lastOutput;
              if(duration > reportInterval){
                report(this.getName(), count, skips, System.currentTimeMillis() - startTime);
                lastOutput = System.currentTimeMillis();
              }
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
        for(OplogRecordProcessor processor : processors) {
          processor.close("oplog");
        }
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
    //  check database-level inclusion
    if(ns.indexOf('.') > 0 && inclusions.contains(ns.substring(0, ns.indexOf('.')))){
      return true;
    }

    return false;
  }

  void report(String collectionName, long count, long skips, long duration){
    double brate = (double)count / ((duration) / 1000.0);
    if(enableOutput)
      System.out.println(collectionName + ": " + PrintFormat.LONG_FORMAT.format(count) + " records, " + PrintFormat.LONG_FORMAT.format(brate) + " req/sec, " + PrintFormat.LONG_FORMAT.format(skips) + " skips");
  }
}
