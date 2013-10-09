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

import java.io.File;

public class StopFileMonitor extends Thread {
	OplogTailThread tailThread;
	public StopFileMonitor(OplogTailThread tailThread){
		this.tailThread = tailThread;
	}

	public void run(){
		while(true){
			try{
				Thread.sleep(1000);
				File file = new File("stop.txt");
				if(file.exists() || tailThread.killMe){
					System.out.println("found stop file, exiting, exit on stop setting is " + tailThread.exitOnStopThread);
					tailThread.killMe = true;
          tailThread.interrupt();
          if(tailThread.exitOnStopThread){
              System.out.println("Exiting the JVM because of exit on stop thread");
              file.delete();
              System.exit(-1);
          }else{
              file.deleteOnExit();
          }

					return;
				}
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
	}
}
