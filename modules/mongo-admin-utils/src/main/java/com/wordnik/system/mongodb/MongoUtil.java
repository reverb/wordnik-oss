package com.wordnik.system.mongodb;

import java.io.File;
import java.util.List;

public abstract class MongoUtil {
	protected void selectCollections(String collectionString, List<String> collectionsToAdd, List<String> collectionsToSkip){
		if(collectionString != null){
			boolean hasIncludes = false;
			String[] collectionNames = collectionString.split(",");
			for(String collectionName : collectionNames){
				if(collectionName.startsWith("!")){
					//	skip it
					collectionsToSkip.add(collectionName.substring(1).trim());
				}
				else{
					collectionsToAdd.add(collectionName.trim());
					hasIncludes = true;
				}
			}
			if(!hasIncludes){
				collectionsToAdd.add("*");
			}
		}
		else{
			collectionsToAdd.add("*");
		}
	}

	protected static void validateDirectory(String directory){
		File dir = new File(directory);
		if(!dir.exists()){
			if(!dir.mkdir()){
				throw new RuntimeException("can't create output dir");
			}
		}
	}
}