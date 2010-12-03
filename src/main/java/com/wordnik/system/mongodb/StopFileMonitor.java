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
					System.out.println("found stop file, exiting");
					tailThread.killMe = true;
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
